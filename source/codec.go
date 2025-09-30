package source

import (
	"runtime"
	"sync/atomic"

	"github.com/ringboundio/chasm/core"
	"github.com/ringboundio/kit"
)

type (
	SymbolCodec interface {
		Proto
		BatchDecoder[core.SymbolEvent]
	}

	BookCodec interface {
		Proto
		UnitDecoder[core.BookEvent]
	}

	CandleCodec interface {
		Proto
		SinkDecoder[core.CandleEvent]
	}

	TradeCodec interface {
		Proto
		BatchDecoder[core.TradeEvent]
	}

	Proto interface {
		Endpoint() string
		Subscribe(key core.Key) []byte
		Unsubscribe(key core.Key) []byte
	}

	ProtoMsgFunc func(key core.Key) []byte

	UnitDecoder[T core.Event] interface {
		Decode(key core.Key, frame []byte) (T, bool, error)
	}

	LossyDecodeFunc[T core.Event, S WorkingState] func(state *S, frame []byte) (T, bool, error)

	BatchDecoder[T core.Event] interface {
		Decode(frame []byte, destination []T) (int, bool, error)
	}

	BatchDecodeFunc[T core.Event] func(frame []byte, destination []T) (int, bool, error)

	SinkDecoder[T core.Event] interface {
		Sink(key core.Key, frame []byte) (T, bool, error)
		Try(key core.Key) (T, bool, error)
	}

	SinkFunc[T core.Event, S WorkingState]    func(state *S, key core.Key, frame []byte) (T, bool, error)
	SinkTryFunc[T core.Event, S WorkingState] func(state *S, key core.Key) (T, bool, error)
)

type symbolCodec struct {
	endpoint
	sub    ProtoMsgFunc
	unsub  ProtoMsgFunc
	decode BatchDecodeFunc[core.SymbolEvent]
}

func NewSymbolCodec(
	endpoint endpoint,
	sub, unsub ProtoMsgFunc,
	decode BatchDecodeFunc[core.SymbolEvent],
) SymbolCodec {
	return &symbolCodec{
		endpoint: endpoint,
		sub:      sub,
		unsub:    unsub,
		decode:   decode,
	}
}

func (c *symbolCodec) Subscribe(key core.Key) []byte   { return c.sub(key) }
func (c *symbolCodec) Unsubscribe(key core.Key) []byte { return c.unsub(key) }

func (c *symbolCodec) Decode(frame []byte, destination []core.SymbolEvent) (int, bool, error) {
	return c.decode(frame, destination)
}

const (
	cuckooCap     int    = 1024 * 4
	cuckooMask    uint64 = uint64(cuckooCap) - 1
	cuckooMaxKick int    = 32
)

type bookCodec struct {
	index [cuckooCap]core.Key
	store [cuckooCap]OrderBook
	endpoint
	sub    ProtoMsgFunc
	unsub  ProtoMsgFunc
	decode LossyDecodeFunc[core.BookEvent, OrderBook]
	_      [24]byte
}

func NewBookCodec(
	endpoint endpoint,
	sub, unsub ProtoMsgFunc,
	decode LossyDecodeFunc[core.BookEvent, OrderBook],
) BookCodec {
	return &bookCodec{
		endpoint: endpoint,
		sub:      sub,
		unsub:    unsub,
		decode:   decode,
	}
}

func (c *bookCodec) Subscribe(key core.Key) []byte {
	var (
		hash = mix(key)
		i    = int(hash & cuckooMask)
		ii   = int(uint64(i) ^ (hash|1)&cuckooMask)
	)

	if c.index[i] == 0 {
		c.index[i] = key
		c.store[i].meta = key
		return c.sub(key)
	}

	if c.index[ii] == 0 {
		c.index[ii] = key
		c.store[ii].meta = key
		return c.sub(key)
	}

	var (
		Bid   kit.LeakyHeap[Bid]
		Ask   kit.LeakyHeap[Ask]
		k     = key
		count int
	)

	for count < cuckooMaxKick {
		meta := k
		k, c.index[i] = c.index[i], k

		s := &c.store[i]
		s.Lock()
		Bid, s.Bid = s.Bid, Bid
		Ask, s.Ask = s.Ask, Ask
		s.meta = meta
		s.Unlock()

		if k == 0 {
			return c.sub(key)
		}

		var (
			hash = mix(k)
			p1   = int(hash & cuckooMask)
			p2   = int(uint64(p1) ^ (hash|1)&cuckooMask)
		)

		if i == p1 {
			i = p2
		} else {
			i = p1
		}

		count++
	}

	// TODO: debate on stash
	panic(key)
}

// TODO: debate on clearing stale state
func (c *bookCodec) Unsubscribe(key core.Key) []byte {
	var (
		hash = mix(key)
		i    = int(hash & cuckooMask)
		ii   = int(uint64(i) ^ (hash|1)&cuckooMask)
	)

	for {
		if c.index[i] == key {
			s := &c.store[i]
			s.Lock()
			if s.meta == key {
				s.meta = 0
				s.Unlock()
				c.index[i] = 0
				return c.unsub(key)
			}
			s.Unlock()
		}

		if c.index[ii] == key {
			s := &c.store[ii]
			s.Lock()
			if s.meta == key {
				s.meta = 0
				s.Unlock()
				c.index[ii] = 0
				return c.unsub(key)
			}
			s.Unlock()
		}
	}
}

func (c *bookCodec) Decode(key core.Key, frame []byte) (core.BookEvent, bool, error) {
	return c.decode(c.getStore(key), frame)
}

func (c *bookCodec) getStore(key core.Key) *OrderBook {
	var (
		hash = mix(key)
		i    = int(hash & cuckooMask)
		ii   = int(uint64(i) ^ (hash|1)&cuckooMask)
	)

	for {
		if c.index[i] == key {
			s := &c.store[i]
			s.Lock()
			if s.meta == key {
				return s
			}
			s.Unlock()
		}

		if c.index[ii] == key {
			s := &c.store[ii]
			s.Lock()
			if s.meta == key {
				return s
			}
			s.Unlock()
		}
	}
}

type candleCodec struct {
	index [cuckooCap]core.Key
	store [cuckooCap]CandleStore
	endpoint
	sub   ProtoMsgFunc
	unsub ProtoMsgFunc
	sink  SinkFunc[core.CandleEvent, CandleStore]
	try   SinkTryFunc[core.CandleEvent, CandleStore]
	_     [16]byte
}

func NewCandleCodec(
	endpoint endpoint,
	sub, unsub ProtoMsgFunc,
	sink SinkFunc[core.CandleEvent, CandleStore],
	try SinkTryFunc[core.CandleEvent, CandleStore],
) CandleCodec {
	return &candleCodec{
		endpoint: endpoint,
		sub:      sub,
		unsub:    unsub,
		sink:     sink,
		try:      try,
	}
}

func (c *candleCodec) Subscribe(key core.Key) []byte {
	var (
		interval = int64(key & core.IntMask)
		base     = key &^ core.IntMask
		hash     = mix(base)
		i        = int(hash & cuckooMask)
		ii       = int(uint64(i) ^ (hash|1)&cuckooMask)
	)

	if c.index[i] == base {
		s := &c.store[i]
		s.Lock()
		for index, got := range s.Interval {
			if got == 0 {
				s.Interval[index] = interval
				s.meta++
				s.Unlock()
				return c.sub(key)
			}
		}

		s.Unlock()
		panic(key)
	}

	if c.index[ii] == base {
		s := &c.store[ii]
		s.Lock()
		for index, got := range s.Interval {
			if got == 0 {
				s.Interval[index] = interval
				s.meta++
				s.Unlock()
				return c.sub(key)
			}
		}

		s.Unlock()
		panic(key)
	}

	if c.index[i] == 0 {
		c.index[i] = base
		c.store[i].Interval[0] = interval
		c.store[i].meta = base | 1
		return c.sub(key)
	}

	if c.index[ii] == 0 {
		c.index[ii] = base
		c.store[ii].Interval[0] = interval
		c.store[ii].meta = base | 1
		return c.sub(key)
	}

	var (
		Interval = [10]int64{0: interval}
		New      [10]bool
		Candle   [10]core.CandleEvent
		meta     = base | 1
		k        = base
		count    int
	)

	for count < cuckooMaxKick {
		k, c.index[i] = c.index[i], k

		s := &c.store[i]
		s.Lock()
		Interval, s.Interval = s.Interval, Interval
		New, s.New = s.New, New
		Candle, s.Candle = s.Candle, Candle
		meta, s.meta = s.meta, meta
		s.Unlock()

		if k == 0 {
			return c.sub(key)
		}

		var (
			hash = mix(k)
			p1   = int(hash & cuckooMask)
			p2   = int(uint64(p1) ^ (hash|1)&cuckooMask)
		)

		if i == p1 {
			i = p2
		} else {
			i = p1
		}

		count++
	}

	// TODO: debate on stash
	panic(key)
}

// TODO: debate on clearing stale state
func (c *candleCodec) Unsubscribe(key core.Key) []byte {
	var (
		base = key &^ core.IntMask
		hash = mix(base)
		i    = int(hash & cuckooMask)
		ii   = int(uint64(i) ^ (hash|1)&cuckooMask)
		want = int64(key & core.IntMask)
	)

	for {
		if c.index[i] == base {
			s := &c.store[i]
			s.Lock()
			if s.meta&^core.IntMask == base {
				for index, got := range s.Interval {
					if got == want {
						s.Interval[index] = 0
						if (s.meta & core.IntMask) == 1 {
							c.index[i] = 0
							s.meta = 0
							s.Unlock()
							return c.unsub(key)
						}
						s.meta--
						s.Unlock()
						return c.unsub(key)
					}
				}
				s.Unlock()
				panic(key)
			}
			s.Unlock()
		}

		if c.index[ii] == base {
			s := &c.store[ii]
			s.Lock()
			if s.meta&^core.IntMask == base {
				for index, got := range s.Interval {
					if got == want {
						s.Interval[index] = 0
						if (s.meta & core.IntMask) == 1 {
							c.index[ii] = 0
							s.meta = 0
							s.Unlock()
							return c.unsub(key)
						}
						s.meta--
						s.Unlock()
						return c.unsub(key)
					}
				}
				s.Unlock()
				panic(key)
			}
			s.Unlock()
		}
	}
}

func (c *candleCodec) Sink(key core.Key, frame []byte) (core.CandleEvent, bool, error) {
	return c.sink(c.getStore(key), key, frame)
}

func (c *candleCodec) Try(key core.Key) (core.CandleEvent, bool, error) {
	return c.try(c.getStore(key), key)
}

func (c *candleCodec) getStore(key core.Key) *CandleStore {
	key &^= core.IntMask

	var (
		hash = mix(key)
		i    = int(hash & cuckooMask)
		ii   = int(uint64(i) ^ (hash|1)&cuckooMask)
	)

	for {
		if c.index[i] == key {
			s := &c.store[i]
			s.Lock()
			if s.meta&^core.IntMask == key {
				return s
			}
			s.Unlock()
		}

		if c.index[ii] == key {
			s := &c.store[ii]
			s.Lock()
			if s.meta&^core.IntMask == key {
				return s
			}
			s.Unlock()
		}
	}
}

func mix(key core.Key) uint64 {
	key ^= key >> 33
	key *= 0xff51afd7ed558ccd
	key ^= key >> 29
	return key
}

type tradeCodec struct {
	endpoint
	sub    ProtoMsgFunc
	unsub  ProtoMsgFunc
	decode BatchDecodeFunc[core.TradeEvent]
}

func NewTradeCodec(
	endpoint endpoint,
	sub, unsub ProtoMsgFunc,
	decode BatchDecodeFunc[core.TradeEvent],
) TradeCodec {
	return &tradeCodec{
		endpoint: endpoint,
		sub:      sub,
		unsub:    unsub,
		decode:   decode,
	}
}

func (c *tradeCodec) Subscribe(key core.Key) []byte   { return c.sub(key) }
func (c *tradeCodec) Unsubscribe(key core.Key) []byte { return c.unsub(key) }

func (c *tradeCodec) Decode(frame []byte, destination []core.TradeEvent) (int, bool, error) {
	return c.decode(frame, destination)
}

type endpoint string

func (this endpoint) Endpoint() string { return string(this) }

type WorkingState interface {
	OrderBook | CandleStore
}

type OrderBook struct {
	Bid kit.LeakyHeap[Bid]
	Ask kit.LeakyHeap[Ask]

	spinLock
	_    [52]byte
	meta uint64
}

type Bid uint64

func (this Bid) Before(that Bid) bool { return this > that }
func (this Bid) After(that Bid) bool  { return this < that }
func (this Bid) Equal(that Bid) bool  { return this == that }
func (this Bid) Compare(that Bid) int { return kit.BoolToInt(this < that) - kit.BoolToInt(this > that) }

type Ask uint64

func (this Ask) Before(that Ask) bool { return this < that }
func (this Ask) After(that Ask) bool  { return this > that }
func (this Ask) Equal(that Ask) bool  { return this == that }
func (this Ask) Compare(that Ask) int { return kit.BoolToInt(this > that) - kit.BoolToInt(this < that) }

type CandleStore struct {
	Interval [10]int64
	New      [10]bool
	Candle   [10]core.CandleEvent
	_        [16]byte

	spinLock
	_    [52]byte
	meta uint64
}

type spinLock struct {
	state atomic.Bool
}

func (s *spinLock) Lock() {
	backoff := 1
	for !s.TryLock() {
		for i := 0; i < backoff; i++ {
			runtime.Gosched()
		}
		if backoff < 64 {
			backoff <<= 1
		}
	}
}

func (s *spinLock) TryLock() bool { return s.state.CompareAndSwap(false, true) }
func (s *spinLock) Unlock()       { s.state.Store(false) }
