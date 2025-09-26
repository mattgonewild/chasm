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
	bookBase
	sub    ProtoMsgFunc
	unsub  ProtoMsgFunc
	decode LossyDecodeFunc[core.BookEvent, bookBase]
}

func NewBookCodec(
	endpoint endpoint,
	sub, unsub ProtoMsgFunc,
	decode LossyDecodeFunc[core.BookEvent, bookBase],
) BookCodec {
	return &bookCodec{
		bookBase: bookBase{endpoint: endpoint},
		sub:      sub,
		unsub:    unsub,
		decode:   decode,
	}
}

func (c *bookCodec) Subscribe(key core.Key) []byte   { return c.sub(key) }
func (c *bookCodec) Unsubscribe(key core.Key) []byte { return c.unsub(key) }

func (c *bookCodec) Decode(frame []byte) (core.BookEvent, bool, error) {
	return c.decode(&c.bookBase, frame)
}

type candleCodec struct {
	candleBase
	sub   ProtoMsgFunc
	unsub ProtoMsgFunc
	sink  SinkFunc[core.CandleEvent, candleBase]
	try   SinkTryFunc[core.CandleEvent, candleBase]
}

func NewCandleCodec(
	endpoint endpoint,
	sub, unsub ProtoMsgFunc,
	sink SinkFunc[core.CandleEvent, candleBase],
	try SinkTryFunc[core.CandleEvent, candleBase],
) CandleCodec {
	return &candleCodec{
		candleBase: candleBase{endpoint: endpoint},
		sub:        sub,
		unsub:      unsub,
		sink:       sink,
		try:        try,
	}
}

func (c *candleCodec) Subscribe(key core.Key) []byte   { return c.sub(key) }
func (c *candleCodec) Unsubscribe(key core.Key) []byte { return c.unsub(key) }

func (c *candleCodec) Sink(key core.Key, frame []byte) (core.CandleEvent, bool, error) {
	return c.sink(&c.candleBase, key, frame)
}

func (c *candleCodec) Try(key core.Key) (core.CandleEvent, bool, error) {
	return c.try(&c.candleBase, key)
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
	symbolBase | bookBase | candleBase | tradeBase
}

type symbolBase struct {
	endpoint
}

type bid uint64

func (this bid) Before(that bid) bool { return this > that }
func (this bid) After(that bid) bool  { return this < that }
func (this bid) Equal(that bid) bool  { return this == that }
func (this bid) Compare(that bid) int { return kit.BoolToInt(this < that) - kit.BoolToInt(this > that) }

type ask uint64

func (this ask) Before(that ask) bool { return this < that }
func (this ask) After(that ask) bool  { return this > that }
func (this ask) Equal(that ask) bool  { return this == that }
func (this ask) Compare(that ask) int { return kit.BoolToInt(this > that) - kit.BoolToInt(this < that) }

type bookBase struct {
	bid kit.LeakyHeap[bid]
	ask kit.LeakyHeap[ask]
	endpoint
}

type candleStore struct {
	interval [10]int64
	new      [10]bool
	candle   [10]core.CandleEvent
	_        [16]byte
}

type candleBase struct {
	symbol [1024]candleStore
	endpoint
}

type tradeBase struct {
	endpoint
}

type endpoint string

func (this endpoint) Endpoint() string { return string(this) }
