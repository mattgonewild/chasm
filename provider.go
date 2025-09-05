package chasm

import (
	"time"

	"github.com/mattgonewild/common"
)

type (
	Provider[T common.UnixTimestamped] interface {
		Get() T
	}

	BrokerageDataProvider = Provider[BrokerageData]

	BrokerageData interface {
		Before(endUnixTime int64) BrokerageDataReader
		At(floorUnixTime int64) BrokerageDataReader
		After(startUnixTime int64) BrokerageDataReader
		common.UnixTimestamped
	}

	BrokerageDataReader interface {
		Book(key Key) (common.Cursor[BookEvent], error)
		Candle(key Key) (common.Cursor[CandleEvent], error)
		Trade(key Key) (common.Cursor[TradeEvent], error)
		Symbol(key Key) (common.Cursor[SymbolEvent], error)
		Schedule(key Key) (common.Cursor[ScheduleEvent], error)
	}

	NewCursorFunc[T common.UnixTimestamped] func(log common.Log[T], boundUnixTime int64) (common.Cursor[T], error)
)

type brokerageDataProvider struct {
	registry BrokerageDataEventLogRegistry
}

func NewBrokerageDataProvider(registry BrokerageDataEventLogRegistry) BrokerageDataProvider {
	return &brokerageDataProvider{
		registry: registry,
	}
}

func (this *brokerageDataProvider) Get() BrokerageData { return NewBrokerageData(this.registry) }

type brokerageData struct {
	registry BrokerageDataEventLogRegistry
}

func NewBrokerageData(registry BrokerageDataEventLogRegistry) BrokerageData {
	return &brokerageData{
		registry: registry,
	}
}

func (this *brokerageData) Before(endUnixTime int64) BrokerageDataReader {
	return NewBrokerageDataReader(
		this.registry,
		endUnixTime,

		NewCursorBeforeFunc[BookEvent](),
		NewCursorBeforeFunc[CandleEvent](),
		NewCursorBeforeFunc[TradeEvent](),
		NewCursorBeforeFunc[SymbolEvent](),
		NewCursorBeforeFunc[ScheduleEvent](),
	)
}

func (this *brokerageData) At(floorUnixTime int64) BrokerageDataReader {
	return NewBrokerageDataReader(
		this.registry,
		floorUnixTime,

		NewCursorAtFunc[BookEvent](),
		NewCursorAtFunc[CandleEvent](),
		NewCursorAtFunc[TradeEvent](),
		NewCursorAtFunc[SymbolEvent](),
		NewCursorAtFunc[ScheduleEvent](),
	)
}

func (this *brokerageData) After(startUnixTime int64) BrokerageDataReader {
	return NewBrokerageDataReader(
		this.registry,
		startUnixTime,

		NewCursorAfterFunc[BookEvent](),
		NewCursorAfterFunc[CandleEvent](),
		NewCursorAfterFunc[TradeEvent](),
		NewCursorAfterFunc[SymbolEvent](),
		NewCursorAfterFunc[ScheduleEvent](),
	)
}

func (this *brokerageData) UnixNano() int64 { return time.Now().UnixNano() }

type brokerageDataReader struct {
	registry      BrokerageDataEventLogRegistry
	boundUnixTime int64

	newBookCursor     NewCursorFunc[BookEvent]
	newCandleCursor   NewCursorFunc[CandleEvent]
	newTradeCursor    NewCursorFunc[TradeEvent]
	newSymbolCursor   NewCursorFunc[SymbolEvent]
	newScheduleCursor NewCursorFunc[ScheduleEvent]
}

func NewBrokerageDataReader(
	registry BrokerageDataEventLogRegistry,
	boundUnixTime int64,

	newBookCursor NewCursorFunc[BookEvent],
	newCandleCursor NewCursorFunc[CandleEvent],
	newTradeCursor NewCursorFunc[TradeEvent],
	newSymbolCursor NewCursorFunc[SymbolEvent],
	newScheduleCursor NewCursorFunc[ScheduleEvent],
) BrokerageDataReader {
	return &brokerageDataReader{
		registry:      registry,
		boundUnixTime: boundUnixTime,

		newBookCursor:     newBookCursor,
		newCandleCursor:   newCandleCursor,
		newTradeCursor:    newTradeCursor,
		newSymbolCursor:   newSymbolCursor,
		newScheduleCursor: newScheduleCursor,
	}
}

func (this *brokerageDataReader) Book(key Key) (common.Cursor[BookEvent], error) {
	log, err := this.registry.Book().Get(key)
	if err != nil {
		return nil, err
	}

	return this.newBookCursor(log, this.boundUnixTime)
}

func (this *brokerageDataReader) Candle(key Key) (common.Cursor[CandleEvent], error) {
	log, err := this.registry.Candle().Get(key)
	if err != nil {
		return nil, err
	}

	return this.newCandleCursor(log, this.boundUnixTime)
}

func (this *brokerageDataReader) Trade(key Key) (common.Cursor[TradeEvent], error) {
	log, err := this.registry.Trade().Get(key)
	if err != nil {
		return nil, err
	}

	return this.newTradeCursor(log, this.boundUnixTime)
}

func (this *brokerageDataReader) Symbol(key Key) (common.Cursor[SymbolEvent], error) {
	log, err := this.registry.Symbol().Get(key)
	if err != nil {
		return nil, err
	}

	return this.newSymbolCursor(log, this.boundUnixTime)
}

func (this *brokerageDataReader) Schedule(key Key) (common.Cursor[ScheduleEvent], error) {
	log, err := this.registry.Schedule().Get(key)
	if err != nil {
		return nil, err
	}

	return this.newScheduleCursor(log, this.boundUnixTime)
}

func NewCursorBeforeFunc[T common.UnixTimestamped]() NewCursorFunc[T] {
	return func(log common.Log[T], endUnixTime int64) (common.Cursor[T], error) {
		return log.NewCursorBefore(endUnixTime)
	}
}

func NewCursorAtFunc[T common.UnixTimestamped]() NewCursorFunc[T] {
	return func(log common.Log[T], floorUnixTime int64) (common.Cursor[T], error) {
		return log.NewCursorAt(floorUnixTime)
	}
}

func NewCursorAfterFunc[T common.UnixTimestamped]() NewCursorFunc[T] {
	return func(log common.Log[T], startUnixTime int64) (common.Cursor[T], error) {
		return log.NewCursorAfter(startUnixTime)
	}
}
