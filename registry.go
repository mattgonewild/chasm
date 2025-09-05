package chasm

import (
	"github.com/mattgonewild/common"
	"github.com/mattgonewild/kit"
)

type (
	Registry[T any] = common.Registry[Key, T]

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

type brokerageDataEventLogRegistry struct {
	book     kit.CoarseRegistry[Key, common.Log[BookEvent]]
	_        [32]byte
	candle   kit.CoarseRegistry[Key, common.Log[CandleEvent]]
	_        [32]byte
	trade    kit.CoarseRegistry[Key, common.Log[TradeEvent]]
	_        [32]byte
	symbol   kit.CoarseRegistry[Key, common.Log[SymbolEvent]]
	_        [32]byte
	schedule kit.CoarseRegistry[Key, common.Log[ScheduleEvent]]
	_        [32]byte
}

func (this *brokerageDataEventLogRegistry) Book() BookEventLogRegistry         { return &this.book }
func (this *brokerageDataEventLogRegistry) Candle() CandleEventLogRegistry     { return &this.candle }
func (this *brokerageDataEventLogRegistry) Trade() TradeEventLogRegistry       { return &this.trade }
func (this *brokerageDataEventLogRegistry) Symbol() SymbolEventLogRegistry     { return &this.symbol }
func (this *brokerageDataEventLogRegistry) Schedule() ScheduleEventLogRegistry { return &this.schedule }
