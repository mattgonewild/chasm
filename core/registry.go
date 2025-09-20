package core

import "github.com/mattgonewild/kit"

type (
	SymbolLogRegistry   = Registry[EventLog[SymbolEvent]]
	BookLogRegistry     = Registry[EventLog[BookEvent]]
	CandleLogRegistry   = Registry[EventLog[CandleEvent]]
	TradeLogRegistry    = Registry[EventLog[TradeEvent]]
	ScheduleLogRegistry = Registry[EventLog[ScheduleEvent]]

	BrokerageDataLogRegistry interface {
		Symbol() SymbolLogRegistry
		Book() BookLogRegistry
		Candle() CandleLogRegistry
		Trade() TradeLogRegistry
		Schedule() ScheduleLogRegistry
	}
)

type brokerageDataLogRegistry struct {
	symbol   kit.CoarseRegistry[Key, EventLog[SymbolEvent]]
	_        [32]byte
	book     kit.CoarseRegistry[Key, EventLog[BookEvent]]
	_        [32]byte
	candle   kit.CoarseRegistry[Key, EventLog[CandleEvent]]
	_        [32]byte
	trade    kit.CoarseRegistry[Key, EventLog[TradeEvent]]
	_        [32]byte
	schedule kit.CoarseRegistry[Key, EventLog[ScheduleEvent]]
	_        [32]byte
}

func (this *brokerageDataLogRegistry) Symbol() SymbolLogRegistry     { return &this.symbol }
func (this *brokerageDataLogRegistry) Book() BookLogRegistry         { return &this.book }
func (this *brokerageDataLogRegistry) Candle() CandleLogRegistry     { return &this.candle }
func (this *brokerageDataLogRegistry) Trade() TradeLogRegistry       { return &this.trade }
func (this *brokerageDataLogRegistry) Schedule() ScheduleLogRegistry { return &this.schedule }
