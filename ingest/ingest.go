package ingest

import (
	"github.com/mattgonewild/chasm/core"
)

type Provider interface {
	Symbol(key core.Key) (Stream[core.SymbolEvent], error)
	Book(key core.Key) (Stream[core.BookEvent], error)
	Candle(key core.Key) (Stream[core.CandleEvent], error)
	Trade(key core.Key) (Stream[core.TradeEvent], error)
}

type Stream[T Event] interface {
	Read() T
	Next() (bool, error)
	Close() error
}

type Event interface {
	core.SymbolEvent | core.BookEvent | core.CandleEvent | core.TradeEvent
}
