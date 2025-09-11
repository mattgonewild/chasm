package sink

import "github.com/mattgonewild/chasm/core"

type Sink interface {
	Symbol(key core.Key) (Writer[core.SymbolEvent], error)
	Book(key core.Key) (Writer[core.BookEvent], error)
	Candle(key core.Key) (Writer[core.CandleEvent], error)
	Trade(key core.Key) (Writer[core.TradeEvent], error)
}

type Writer[T core.Event] interface {
	Write(event T) error
	Close() error
}
