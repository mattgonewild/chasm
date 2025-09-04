package chasm

import "github.com/mattgonewild/common"

type (
	Registry[T any] interface {
		Register(key uint, value T) error
		Unregister(key uint) error
		Get(key uint) (T, error)
	}

	BookEventLogRegistry     = Registry[common.Log[BookEvent]]
	CandleEventLogRegistry   = Registry[common.Log[CandleEvent]]
	TradeEventLogRegistry    = Registry[common.Log[TradeEvent]]
	SymbolEventLogRegistry   = Registry[common.Log[SymbolEvent]]
	ScheduleEventLogRegistry = Registry[common.Log[ScheduleEvent]]

	BrokerageDataEventLogRegistry interface {
		Book() BookEventLogRegistry
		Candle() CandleEventLogRegistry
		Trade() TradeEventLogRegistry
		Symbol() SymbolEventLogRegistry
		Schedule() ScheduleEventLogRegistry
	}
)
