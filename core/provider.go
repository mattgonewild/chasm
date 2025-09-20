package core

import "time"

type (
	Provider[T UnixTimestamped] interface {
		Get() T
	}

	BrokerageDataProvider = Provider[BrokerageData]

	BrokerageData interface {
		Before(endUnixTime int64) BrokerageDataReader
		At(floorUnixTime int64) BrokerageDataReader
		After(startUnixTime int64) BrokerageDataReader
		UnixTimestamped
	}

	BrokerageDataReader interface {
		Symbol(key Key) (Cursor[SymbolEvent], error)
		Book(key Key) (Cursor[BookEvent], error)
		Candle(key Key) (Cursor[CandleEvent], error)
		Trade(key Key) (Cursor[TradeEvent], error)
		Schedule(key Key) (Cursor[ScheduleEvent], error)
	}

	newCursorFunc[T UnixTimestamped] func(log EventLog[T], boundUnixTime int64) (Cursor[T], error)
)

type brokerageDataProvider struct {
	registry BrokerageDataLogRegistry
}

func newBrokerageDataProvider(registry BrokerageDataLogRegistry) BrokerageDataProvider {
	return &brokerageDataProvider{
		registry: registry,
	}
}

func (this *brokerageDataProvider) Get() BrokerageData { return newBrokerageData(this.registry) }

type brokerageData struct {
	registry BrokerageDataLogRegistry
}

func newBrokerageData(registry BrokerageDataLogRegistry) BrokerageData {
	return &brokerageData{
		registry: registry,
	}
}

func (this *brokerageData) Before(endUnixTime int64) BrokerageDataReader {
	return newBrokerageDataReader(
		this.registry,
		endUnixTime,

		newCursorBeforeFunc[SymbolEvent](),
		newCursorBeforeFunc[BookEvent](),
		newCursorBeforeFunc[CandleEvent](),
		newCursorBeforeFunc[TradeEvent](),
		newCursorBeforeFunc[ScheduleEvent](),
	)
}

func (this *brokerageData) At(floorUnixTime int64) BrokerageDataReader {
	return newBrokerageDataReader(
		this.registry,
		floorUnixTime,

		newCursorAtFunc[SymbolEvent](),
		newCursorAtFunc[BookEvent](),
		newCursorAtFunc[CandleEvent](),
		newCursorAtFunc[TradeEvent](),
		newCursorAtFunc[ScheduleEvent](),
	)
}

func (this *brokerageData) After(startUnixTime int64) BrokerageDataReader {
	return newBrokerageDataReader(
		this.registry,
		startUnixTime,

		newCursorAfterFunc[SymbolEvent](),
		newCursorAfterFunc[BookEvent](),
		newCursorAfterFunc[CandleEvent](),
		newCursorAfterFunc[TradeEvent](),
		newCursorAfterFunc[ScheduleEvent](),
	)
}

func (this *brokerageData) UnixNano() int64 { return time.Now().UnixNano() }

type brokerageDataReader struct {
	registry      BrokerageDataLogRegistry
	boundUnixTime int64

	newSymbolCursor   newCursorFunc[SymbolEvent]
	newBookCursor     newCursorFunc[BookEvent]
	newCandleCursor   newCursorFunc[CandleEvent]
	newTradeCursor    newCursorFunc[TradeEvent]
	newScheduleCursor newCursorFunc[ScheduleEvent]
}

func newBrokerageDataReader(
	registry BrokerageDataLogRegistry,
	boundUnixTime int64,

	newSymbolCursor newCursorFunc[SymbolEvent],
	newBookCursor newCursorFunc[BookEvent],
	newCandleCursor newCursorFunc[CandleEvent],
	newTradeCursor newCursorFunc[TradeEvent],
	newScheduleCursor newCursorFunc[ScheduleEvent],
) BrokerageDataReader {
	return &brokerageDataReader{
		registry:      registry,
		boundUnixTime: boundUnixTime,

		newSymbolCursor:   newSymbolCursor,
		newBookCursor:     newBookCursor,
		newCandleCursor:   newCandleCursor,
		newTradeCursor:    newTradeCursor,
		newScheduleCursor: newScheduleCursor,
	}
}

func (this *brokerageDataReader) Symbol(key Key) (Cursor[SymbolEvent], error) {
	log, err := this.registry.Symbol().Get(key)
	if err != nil {
		return nil, err
	}

	return this.newSymbolCursor(log, this.boundUnixTime)
}

func (this *brokerageDataReader) Book(key Key) (Cursor[BookEvent], error) {
	log, err := this.registry.Book().Get(key)
	if err != nil {
		return nil, err
	}

	return this.newBookCursor(log, this.boundUnixTime)
}

func (this *brokerageDataReader) Candle(key Key) (Cursor[CandleEvent], error) {
	log, err := this.registry.Candle().Get(key)
	if err != nil {
		return nil, err
	}

	return this.newCandleCursor(log, this.boundUnixTime)
}

func (this *brokerageDataReader) Trade(key Key) (Cursor[TradeEvent], error) {
	log, err := this.registry.Trade().Get(key)
	if err != nil {
		return nil, err
	}

	return this.newTradeCursor(log, this.boundUnixTime)
}

func (this *brokerageDataReader) Schedule(key Key) (Cursor[ScheduleEvent], error) {
	log, err := this.registry.Schedule().Get(key)
	if err != nil {
		return nil, err
	}

	return this.newScheduleCursor(log, this.boundUnixTime)
}

func newCursorBeforeFunc[T UnixTimestamped]() newCursorFunc[T] {
	return func(log EventLog[T], endUnixTime int64) (Cursor[T], error) {
		return log.NewCursorBefore(endUnixTime)
	}
}

func newCursorAtFunc[T UnixTimestamped]() newCursorFunc[T] {
	return func(log EventLog[T], floorUnixTime int64) (Cursor[T], error) {
		return log.NewCursorAt(floorUnixTime)
	}
}

func newCursorAfterFunc[T UnixTimestamped]() newCursorFunc[T] {
	return func(log EventLog[T], startUnixTime int64) (Cursor[T], error) {
		return log.NewCursorAfter(startUnixTime)
	}
}
