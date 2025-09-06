package chasm

import (
	"github.com/mattgonewild/common"
	"github.com/mattgonewild/kit"
)

type (
	Registry[T any] = common.Registry[Key, T]

	SymbolLogRegistry   = Registry[common.Log[SymbolEvent]]
	BookLogRegistry     = Registry[common.Log[BookEvent]]
	CandleLogRegistry   = Registry[common.Log[CandleEvent]]
	TradeLogRegistry    = Registry[common.Log[TradeEvent]]
	ScheduleLogRegistry = Registry[common.Log[ScheduleEvent]]

	BrokerageDataLogRegistry interface {
		Symbol() SymbolLogRegistry
		Book() BookLogRegistry
		Candle() CandleLogRegistry
		Trade() TradeLogRegistry
		Schedule() ScheduleLogRegistry
	}
)

type brokerageDataLogRegistry struct {
	symbol   kit.CoarseRegistry[Key, common.Log[SymbolEvent]]
	_        [32]byte
	book     kit.CoarseRegistry[Key, common.Log[BookEvent]]
	_        [32]byte
	candle   kit.CoarseRegistry[Key, common.Log[CandleEvent]]
	_        [32]byte
	trade    kit.CoarseRegistry[Key, common.Log[TradeEvent]]
	_        [32]byte
	schedule kit.CoarseRegistry[Key, common.Log[ScheduleEvent]]
	_        [32]byte
}

func (this *brokerageDataLogRegistry) Symbol() SymbolLogRegistry     { return &this.symbol }
func (this *brokerageDataLogRegistry) Book() BookLogRegistry         { return &this.book }
func (this *brokerageDataLogRegistry) Candle() CandleLogRegistry     { return &this.candle }
func (this *brokerageDataLogRegistry) Trade() TradeLogRegistry       { return &this.trade }
func (this *brokerageDataLogRegistry) Schedule() ScheduleLogRegistry { return &this.schedule }
