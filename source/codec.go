package source

import (
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
		Decode(frame []byte) (T, bool, error)
	}

	LossyDecodeFunc[T core.Event, B codecBase] func(base *B, frame []byte) (T, bool, error)

	BatchDecoder[T core.Event] interface {
		Decode(frame []byte, destination []T) (int, bool, error)
	}

	BatchDecodeFunc[T core.Event] func(frame []byte, destination []T) (int, bool, error)

	SinkDecoder[T core.Event] interface {
		Sink(key core.Key, frame []byte) (T, bool, error)
		Try(key core.Key) (T, bool, error)
	}

	SinkFunc[T core.Event, B codecBase]    func(base *B, key core.Key, frame []byte) (T, bool, error)
	SinkTryFunc[T core.Event, B codecBase] func(base *B, key core.Key) (T, bool, error)
)

type symbolCodec struct {
	symbolBase
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
		symbolBase: symbolBase{endpoint: endpoint},
		sub:        sub,
		unsub:      unsub,
		decode:     decode,
	}
}

func (c *symbolCodec) Subscribe(key core.Key) []byte   { return c.sub(key) }
func (c *symbolCodec) Unsubscribe(key core.Key) []byte { return c.unsub(key) }

func (c *symbolCodec) Decode(frame []byte, destination []core.SymbolEvent) (int, bool, error) {
	return c.decode(frame, destination)
}

type bookCodec struct {
	BookBase
	sub    ProtoMsgFunc
	unsub  ProtoMsgFunc
	decode LossyDecodeFunc[core.BookEvent, BookBase]
}

func NewBookCodec(
	endpoint endpoint,
	sub, unsub ProtoMsgFunc,
	decode LossyDecodeFunc[core.BookEvent, BookBase],
) BookCodec {
	return &bookCodec{
		BookBase: BookBase{endpoint: endpoint},
		sub:      sub,
		unsub:    unsub,
		decode:   decode,
	}
}

func (c *bookCodec) Subscribe(key core.Key) []byte   { return c.sub(key) }
func (c *bookCodec) Unsubscribe(key core.Key) []byte { return c.unsub(key) }

func (c *bookCodec) Decode(frame []byte) (core.BookEvent, bool, error) {
	return c.decode(&c.BookBase, frame)
}

type candleCodec struct {
	CandleBase
	sub   ProtoMsgFunc
	unsub ProtoMsgFunc
	sink  SinkFunc[core.CandleEvent, CandleBase]
	try   SinkTryFunc[core.CandleEvent, CandleBase]
}

func NewCandleCodec(
	endpoint endpoint,
	sub, unsub ProtoMsgFunc,
	sink SinkFunc[core.CandleEvent, CandleBase],
	try SinkTryFunc[core.CandleEvent, CandleBase],
) CandleCodec {
	return &candleCodec{
		CandleBase: CandleBase{endpoint: endpoint},
		sub:        sub,
		unsub:      unsub,
		sink:       sink,
		try:        try,
	}
}

func (c *candleCodec) Subscribe(key core.Key) []byte   { return c.sub(key) }
func (c *candleCodec) Unsubscribe(key core.Key) []byte { return c.unsub(key) }

func (c *candleCodec) Sink(key core.Key, frame []byte) (core.CandleEvent, bool, error) {
	return c.sink(&c.CandleBase, key, frame)
}

func (c *candleCodec) Try(key core.Key) (core.CandleEvent, bool, error) {
	return c.try(&c.CandleBase, key)
}

type tradeCodec struct {
	tradeBase
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
		tradeBase: tradeBase{endpoint: endpoint},
		sub:       sub,
		unsub:     unsub,
		decode:    decode,
	}
}

func (c *tradeCodec) Subscribe(key core.Key) []byte   { return c.sub(key) }
func (c *tradeCodec) Unsubscribe(key core.Key) []byte { return c.unsub(key) }

func (c *tradeCodec) Decode(frame []byte, destination []core.TradeEvent) (int, bool, error) {
	return c.decode(frame, destination)
}

type codecBase interface {
	symbolBase | BookBase | CandleBase | tradeBase
}

type symbolBase struct {
	endpoint
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

type BookBase struct {
	Bid kit.LeakyHeap[Bid]
	Ask kit.LeakyHeap[Ask]
	endpoint
}

type CandleStore struct {
	Interval [10]int64
	New      [10]bool
	Candle   [10]core.CandleEvent
	_        [16]byte
}

type CandleBase struct {
	Symbol [1024]CandleStore
	endpoint
}

type tradeBase struct {
	endpoint
}

type endpoint string

func (this endpoint) Endpoint() string { return string(this) }
