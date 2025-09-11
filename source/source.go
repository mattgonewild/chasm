package source

import "github.com/mattgonewild/chasm/core"

type Source interface {
	Symbol(key core.Key) (Reader[core.SymbolEvent], error)
	Book(key core.Key) (Reader[core.BookEvent], error)
	Candle(key core.Key) (Reader[core.CandleEvent], error)
	Trade(key core.Key) (Reader[core.TradeEvent], error)
}

type Reader[T core.Event] interface {
	Read() T
	Next() (bool, error)
	Close() error
}
