package core

import "math"

type Event interface {
	SymbolEvent | BookEvent | CandleEvent | TradeEvent
	UnixTimestamped
}

type SymbolEvent struct {
	Symbol uint64

	// Consider using IsOnline() helper. Negative value indicates online event.
	EventUnixTime int64
}

func (this SymbolEvent) IsOnline() bool  { return this.EventUnixTime < 0 }
func (this SymbolEvent) UnixNano() int64 { return this.EventUnixTime & math.MaxInt64 }

type BookEvent struct {
	Bid, Ask      uint32
	EventUnixTime int64
}

func (this BookEvent) UnixNano() int64 { return this.EventUnixTime }

type CandleEvent struct {
	Open, High, Low, Close uint32
	EventUnixTime          int64
}

func (this CandleEvent) UnixNano() int64 { return this.EventUnixTime }

type TradeEvent int64

func (this TradeEvent) IsSell() bool    { return this < 0 }
func (this TradeEvent) UnixNano() int64 { return int64(this) & math.MaxInt64 }

type ScheduleEvent struct {
	Symbol                 uint64
	Balance, Basis         uint32
	MinOrder, MinIncrement uint32
	EventUnixTime          int64
}

func (this ScheduleEvent) UnixNano() int64 { return this.EventUnixTime }
