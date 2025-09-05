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

	newCursorFunc[T common.UnixTimestamped] func(log common.Log[T], boundUnixTime int64) (common.Cursor[T], error)
)

type brokerageDataProvider struct {
	registry BrokerageDataEventLogRegistry
}

func newBrokerageDataProvider(registry BrokerageDataEventLogRegistry) BrokerageDataProvider {
	return &brokerageDataProvider{
		registry: registry,
	}
}

func (this *brokerageDataProvider) Get() BrokerageData { return newBrokerageData(this.registry) }

type brokerageData struct {
	registry BrokerageDataEventLogRegistry
}

func newBrokerageData(registry BrokerageDataEventLogRegistry) BrokerageData {
	return &brokerageData{
		registry: registry,
	}
}

func (this *brokerageData) Before(endUnixTime int64) BrokerageDataReader {
	return newBrokerageDataReader(
		this.registry,
		endUnixTime,

		newCursorBeforeFunc[BookEvent](),
		newCursorBeforeFunc[CandleEvent](),
		newCursorBeforeFunc[TradeEvent](),
		newCursorBeforeFunc[SymbolEvent](),
		newCursorBeforeFunc[ScheduleEvent](),
	)
}

func (this *brokerageData) At(floorUnixTime int64) BrokerageDataReader {
	return newBrokerageDataReader(
		this.registry,
		floorUnixTime,

		newCursorAtFunc[BookEvent](),
		newCursorAtFunc[CandleEvent](),
		newCursorAtFunc[TradeEvent](),
		newCursorAtFunc[SymbolEvent](),
		newCursorAtFunc[ScheduleEvent](),
	)
}

func (this *brokerageData) After(startUnixTime int64) BrokerageDataReader {
	return newBrokerageDataReader(
		this.registry,
		startUnixTime,

		newCursorAfterFunc[BookEvent](),
		newCursorAfterFunc[CandleEvent](),
		newCursorAfterFunc[TradeEvent](),
		newCursorAfterFunc[SymbolEvent](),
		newCursorAfterFunc[ScheduleEvent](),
	)
}

func (this *brokerageData) UnixNano() int64 { return time.Now().UnixNano() }

type brokerageDataReader struct {
	registry      BrokerageDataEventLogRegistry
	boundUnixTime int64

	newBookCursor     newCursorFunc[BookEvent]
	newCandleCursor   newCursorFunc[CandleEvent]
	newTradeCursor    newCursorFunc[TradeEvent]
	newSymbolCursor   newCursorFunc[SymbolEvent]
	newScheduleCursor newCursorFunc[ScheduleEvent]
}

func newBrokerageDataReader(
	registry BrokerageDataEventLogRegistry,
	boundUnixTime int64,

	newBookCursor newCursorFunc[BookEvent],
	newCandleCursor newCursorFunc[CandleEvent],
	newTradeCursor newCursorFunc[TradeEvent],
	newSymbolCursor newCursorFunc[SymbolEvent],
	newScheduleCursor newCursorFunc[ScheduleEvent],
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

func newCursorBeforeFunc[T common.UnixTimestamped]() newCursorFunc[T] {
	return func(log common.Log[T], endUnixTime int64) (common.Cursor[T], error) {
		return log.NewCursorBefore(endUnixTime)
	}
}

func newCursorAtFunc[T common.UnixTimestamped]() newCursorFunc[T] {
	return func(log common.Log[T], floorUnixTime int64) (common.Cursor[T], error) {
		return log.NewCursorAt(floorUnixTime)
	}
}

func newCursorAfterFunc[T common.UnixTimestamped]() newCursorFunc[T] {
	return func(log common.Log[T], startUnixTime int64) (common.Cursor[T], error) {
		return log.NewCursorAfter(startUnixTime)
	}
}
