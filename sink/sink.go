package sink

import (
	"math/bits"
	"net"
	"sync"

	"github.com/mattgonewild/chasm/core"
)

type Sink interface {
	Symbol(key core.Key) Writer[core.SymbolEvent]
	Book(key core.Key) Writer[core.BookEvent]
	Candle(key core.Key) Writer[core.CandleEvent]
	Trade(key core.Key) Writer[core.TradeEvent]
}

type Writer[T core.Event] interface {
	Write(event T) error
	Flush() error
}

type sink struct {
	sender sharedSender
}

func NewSink(location string) (Sink, error) {
	conn, err := net.Dial("tcp4", location)
	if err != nil {
		return nil, err
	}

	sink := new(sink)
	sink.sender.conn = conn
	return sink, nil
}

func (this *sink) Symbol(key core.Key) Writer[core.SymbolEvent] {
	return &qdbSymbolWriter{key: key, sender: &this.sender}
}

func (this *sink) Book(key core.Key) Writer[core.BookEvent] {
	return &qdbBookWriter{key: key, sender: &this.sender}
}

func (this *sink) Candle(key core.Key) Writer[core.CandleEvent] {
	return &qdbCandleWriter{key: key, sender: &this.sender}
}

func (this *sink) Trade(key core.Key) Writer[core.TradeEvent] {
	return &qdbTradeWriter{key: key, sender: &this.sender}
}

const (
	maxKeyLen  int = 10
	maxUintLen int = 10
	maxTimeLen int = 20

	measurement int = 2
	keyFi       int = 2 + maxKeyLen + 1
	boolFi      int = 3
	intFi       int = 2 + maxUintLen + 1
	timestamp   int = 1 + maxTimeLen + 1

	symbolLineMax int = measurement +
		keyFi + 1 +
		boolFi + 1 +
		intFi + 1 +
		intFi +
		timestamp

	bookLineMax int = measurement +
		keyFi + 1 +
		intFi + 1 +
		intFi +
		timestamp

	candleLineMax int = measurement +
		keyFi + 1 +
		intFi + 1 +
		intFi + 1 +
		intFi + 1 +
		intFi +
		timestamp

	tradeLineMax int = measurement +
		keyFi + 1 +
		boolFi +
		timestamp

	Symbol       byte = 's'
	Book         byte = 'b'
	Candle       byte = 'c'
	Trade        byte = 't'
	Online       byte = 'o'
	MinOrder     byte = 'm'
	MinIncrement byte = 'i'
	Bid          byte = 'b'
	Ask          byte = 'a'
	Open         byte = 'o'
	High         byte = 'h'
	Low          byte = 'l'
	Close        byte = 'c'
	Buy          byte = 'b'
)

type qdbSymbolWriter struct {
	key    core.Key
	sender *sharedSender
}

func (this *qdbSymbolWriter) Write(event core.SymbolEvent) error {
	const (
		table        byte = Symbol
		symbol       byte = Symbol
		online       byte = Online
		minOrder     byte = MinOrder
		minIncrement byte = MinIncrement
	)

	this.sender.Lock()
	if (this.sender.length + symbolLineMax) > senderBufCap {
		if err := this.sender.flush(); err != nil {
			this.sender.Unlock()
			return err
		}
	}

	this.sender.putTable(table)
	this.sender.putInt(symbol, uint(this.key))
	this.sender.putByte(',')
	this.sender.putBool(online, event.Online)
	this.sender.putByte(',')
	this.sender.putInt(minOrder, uint(event.MinOrder))
	this.sender.putByte(',')
	this.sender.putInt(minIncrement, uint(event.MinIncrement))
	this.sender.putAt(event.EventUnixTime)

	this.sender.Unlock()
	return nil
}

func (this *qdbSymbolWriter) Flush() error {
	this.sender.Lock()
	err := this.sender.flush()
	this.sender.Unlock()
	return err
}

type qdbBookWriter struct {
	key    core.Key
	sender *sharedSender
}

func (this *qdbBookWriter) Write(event core.BookEvent) error {
	const (
		table  byte = Book
		symbol byte = Symbol
		bid    byte = Bid
		ask    byte = Ask
	)

	this.sender.Lock()
	if (this.sender.length + bookLineMax) > senderBufCap {
		if err := this.sender.flush(); err != nil {
			this.sender.Unlock()
			return err
		}
	}

	this.sender.putTable(table)
	this.sender.putInt(symbol, uint(this.key))
	this.sender.putByte(',')
	this.sender.putInt(bid, uint(event.Bid))
	this.sender.putByte(',')
	this.sender.putInt(ask, uint(event.Ask))
	this.sender.putAt(event.EventUnixTime)

	this.sender.Unlock()
	return nil
}

func (this *qdbBookWriter) Flush() error {
	this.sender.Lock()
	err := this.sender.flush()
	this.sender.Unlock()
	return err
}

type qdbCandleWriter struct {
	key    core.Key
	sender *sharedSender
}

func (this *qdbCandleWriter) Write(event core.CandleEvent) error {
	const (
		table  byte = Candle
		symbol byte = Symbol
		open   byte = Open
		high   byte = High
		low    byte = Low
		close  byte = Close
	)

	this.sender.Lock()
	if (this.sender.length + candleLineMax) > senderBufCap {
		if err := this.sender.flush(); err != nil {
			this.sender.Unlock()
			return err
		}
	}

	this.sender.putTable(table)
	this.sender.putInt(symbol, uint(this.key))
	this.sender.putByte(',')
	this.sender.putInt(open, uint(event.Open))
	this.sender.putByte(',')
	this.sender.putInt(high, uint(event.High))
	this.sender.putByte(',')
	this.sender.putInt(low, uint(event.Low))
	this.sender.putByte(',')
	this.sender.putInt(close, uint(event.Close))
	this.sender.putAt(event.EventUnixTime)

	this.sender.Unlock()
	return nil
}

func (this *qdbCandleWriter) Flush() error {
	this.sender.Lock()
	err := this.sender.flush()
	this.sender.Unlock()
	return err
}

type qdbTradeWriter struct {
	key    core.Key
	sender *sharedSender
}

func (this *qdbTradeWriter) Write(event core.TradeEvent) error {
	const (
		table  byte = Trade
		symbol byte = Symbol
		buy    byte = Buy
	)

	this.sender.Lock()
	if (this.sender.length + tradeLineMax) > senderBufCap {
		if err := this.sender.flush(); err != nil {
			this.sender.Unlock()
			return err
		}
	}

	this.sender.putTable(table)
	this.sender.putInt(symbol, uint(this.key))
	this.sender.putByte(',')
	this.sender.putBool(buy, !event.IsSell())
	this.sender.putAt(event.UnixNano())

	this.sender.Unlock()
	return nil
}

func (this *qdbTradeWriter) Flush() error {
	this.sender.Lock()
	err := this.sender.flush()
	this.sender.Unlock()
	return err
}

const senderBufCap int = 32768

type sharedSender struct {
	sync.Mutex
	conn   net.Conn
	length int
	buf    [senderBufCap]byte
}

func (this *sharedSender) flush() error {
	length := this.length
	if length == 0 {
		return nil
	}
	this.length = 0

	_, err := this.conn.Write(this.buf[:length])
	return err
}

func (this *sharedSender) putTable(name byte) {
	this.putByte(name)
	this.putByte(' ')
}

func (this *sharedSender) putInt(column byte, value uint) {
	this.putByte(column)
	this.putByte('=')
	this.putDigit(value)
	this.putByte('i')
}

func (this *sharedSender) putBool(column byte, true bool) {
	this.putByte(column)
	this.putByte('=')
	if true {
		this.putByte('t')
	} else {
		this.putByte('f')
	}
}

func (this *sharedSender) putAt(unixTime int64) {
	this.putByte(' ')
	this.putDigit(uint(unixTime))
	this.putByte('\n')
}

func (this *sharedSender) putByte(value byte) { this.buf[this.length] = value; this.length++ }

func (this *sharedSender) putDigit(value uint) {
	n := digitCount(value)
	this.length = this.length + n
	cursor := this.length

	for value >= 100 {
		quot := value / 100
		rem := value - (quot * 100)
		value = quot

		pair := twoDigitMap[rem]
		cursor -= 2
		this.buf[cursor] = byte(pair >> 8)
		this.buf[cursor+1] = byte(pair)
	}

	if value < 10 {
		cursor--
		this.buf[cursor] = byte('0' + value)
	} else {
		pair := twoDigitMap[value]
		cursor -= 2
		this.buf[cursor] = byte(pair >> 8)
		this.buf[cursor+1] = byte(pair)
	}
}

var (
	powerOfTen  [20]uint    = newPowerOfTen()
	twoDigitMap [100]uint16 = newTwoDigitMap()
)

func newPowerOfTen() [20]uint {
	var table [20]uint

	table[0] = 1
	for index := 1; index < 20; index++ {
		table[index] = table[index-1] * 10
	}

	return table
}

func newTwoDigitMap() [100]uint16 {
	return [100]uint16{
		'0'<<8 | '0', '0'<<8 | '1', '0'<<8 | '2', '0'<<8 | '3', '0'<<8 | '4', '0'<<8 | '5', '0'<<8 | '6', '0'<<8 | '7', '0'<<8 | '8', '0'<<8 | '9',
		'1'<<8 | '0', '1'<<8 | '1', '1'<<8 | '2', '1'<<8 | '3', '1'<<8 | '4', '1'<<8 | '5', '1'<<8 | '6', '1'<<8 | '7', '1'<<8 | '8', '1'<<8 | '9',
		'2'<<8 | '0', '2'<<8 | '1', '2'<<8 | '2', '2'<<8 | '3', '2'<<8 | '4', '2'<<8 | '5', '2'<<8 | '6', '2'<<8 | '7', '2'<<8 | '8', '2'<<8 | '9',
		'3'<<8 | '0', '3'<<8 | '1', '3'<<8 | '2', '3'<<8 | '3', '3'<<8 | '4', '3'<<8 | '5', '3'<<8 | '6', '3'<<8 | '7', '3'<<8 | '8', '3'<<8 | '9',
		'4'<<8 | '0', '4'<<8 | '1', '4'<<8 | '2', '4'<<8 | '3', '4'<<8 | '4', '4'<<8 | '5', '4'<<8 | '6', '4'<<8 | '7', '4'<<8 | '8', '4'<<8 | '9',
		'5'<<8 | '0', '5'<<8 | '1', '5'<<8 | '2', '5'<<8 | '3', '5'<<8 | '4', '5'<<8 | '5', '5'<<8 | '6', '5'<<8 | '7', '5'<<8 | '8', '5'<<8 | '9',
		'6'<<8 | '0', '6'<<8 | '1', '6'<<8 | '2', '6'<<8 | '3', '6'<<8 | '4', '6'<<8 | '5', '6'<<8 | '6', '6'<<8 | '7', '6'<<8 | '8', '6'<<8 | '9',
		'7'<<8 | '0', '7'<<8 | '1', '7'<<8 | '2', '7'<<8 | '3', '7'<<8 | '4', '7'<<8 | '5', '7'<<8 | '6', '7'<<8 | '7', '7'<<8 | '8', '7'<<8 | '9',
		'8'<<8 | '0', '8'<<8 | '1', '8'<<8 | '2', '8'<<8 | '3', '8'<<8 | '4', '8'<<8 | '5', '8'<<8 | '6', '8'<<8 | '7', '8'<<8 | '8', '8'<<8 | '9',
		'9'<<8 | '0', '9'<<8 | '1', '9'<<8 | '2', '9'<<8 | '3', '9'<<8 | '4', '9'<<8 | '5', '9'<<8 | '6', '9'<<8 | '7', '9'<<8 | '8', '9'<<8 | '9',
	}
}

func digitCount(value uint) int {
	if value == 0 {
		return 1
	}

	index := int(
		(uint(bits.Len(value)) * 1233) >> 12,
	)

	if value < powerOfTen[index] {
		return index
	}

	return index + 1
}
