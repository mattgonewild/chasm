package source

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"net/url"

	"github.com/mattgonewild/chasm/core"
)

type (
	Source interface {
		Symbol(key core.Key) (Reader[core.SymbolEvent], error)
		Book(key core.Key) (Reader[core.BookEvent], error)
		Candle(key core.Key) (Reader[core.CandleEvent], error)
		Trade(key core.Key) (Reader[core.TradeEvent], error)
	}

	Reader[T core.Event] interface {
		Read() T
		Next() (bool, error)
		Close() error
	}

	Proto[T core.Event] interface {
		Endpoint() string
		Subscribe(key core.Key) []byte
		Unsubscribe(key core.Key) []byte
		Decode(frame []byte) (T, error)
	}
)

type source struct {
	symbol Proto[core.SymbolEvent]
	book   Proto[core.BookEvent]
	candle Proto[core.CandleEvent]
	trade  Proto[core.TradeEvent]
	origin url.URL
}

func NewSource(
	symbol Proto[core.SymbolEvent],
	book Proto[core.BookEvent],
	candle Proto[core.CandleEvent],
	trade Proto[core.TradeEvent],
	origin url.URL,
) Source {
	return &source{
		symbol: symbol,
		book:   book,
		candle: candle,
		trade:  trade,
		origin: origin,
	}
}

func (this *source) Symbol(key core.Key) (Reader[core.SymbolEvent], error) {
	return newBufWebSockReader(&this.origin, this.symbol, key)
}
func (this *source) Book(key core.Key) (Reader[core.BookEvent], error) {
	return newBufWebSockReader(&this.origin, this.book, key)
}
func (this *source) Candle(key core.Key) (Reader[core.CandleEvent], error) {
	return newBufWebSockReader(&this.origin, this.candle, key)
}
func (this *source) Trade(key core.Key) (Reader[core.TradeEvent], error) {
	return newBufWebSockReader(&this.origin, this.trade, key)
}

type bufWebSockReader[T core.Event] struct {
	conn    *tls.Conn
	proto   Proto[T]
	decoded T
	buf     [8192]byte
	mask    uint32
	key     core.Key
}

func newBufWebSockReader[T core.Event](origin *url.URL, proto Proto[T], key core.Key) (Reader[T], error) {
	conn, err := tls.Dial("tcp4", origin.Host, nil)
	if err != nil {
		return nil, err
	}

	var (
		buf [512]byte
		n   int

		as = func(s string) { n += copy(buf[n:], s) }
		ab = func(b []byte) { n += copy(buf[n:], b) }
	)

	const lineEnd = "\r\n"

	as("GET ")
	as(origin.RequestURI())
	as(" HTTP/1.1")
	as(lineEnd)

	as("Host: ")
	as(origin.Host)
	as(lineEnd)

	as("Upgrade: websocket")
	as(lineEnd)

	as("Connection: Upgrade")
	as(lineEnd)

	var (
		raw [16]byte
		b64 [24]byte
	)

	rand.Read(raw[:])
	base64.StdEncoding.Encode(b64[:], raw[:])

	as("Sec-WebSocket-Key: ")
	ab(b64[:])
	as(lineEnd)

	as("Origin: ")
	as(origin.String())
	as(lineEnd)

	as("Sec-WebSocket-Version: 13")
	as(lineEnd)

	as(lineEnd)

	_, err = conn.Write(buf[:n])
	if err != nil {
		conn.Close()
		return nil, err
	}

	// TODO: read 101

	return &bufWebSockReader[T]{
		conn:  conn,
		proto: proto,
		mask:  binary.LittleEndian.Uint32(raw[:4]),
		key:   key,
	}, nil
}

func (this *bufWebSockReader[T]) Read() T { return this.decoded }

func (this *bufWebSockReader[T]) Next() (bool, error)

func (this *bufWebSockReader[T]) sendPong() error {
	var buf [6]byte
	buf[0] = 0x80 | 10
	buf[1] = 0x80 | 0

	var mask = this.mask
	mask ^= mask << 13
	mask ^= mask >> 17
	mask ^= mask << 5
	if mask == 0 {
		mask = 1
	}
	this.mask = mask

	buf[2] = byte(mask)
	buf[3] = byte(mask >> 8)
	buf[4] = byte(mask >> 16)
	buf[5] = byte(mask >> 24)

	_, err := this.conn.Write(buf[:])
	return err
}

func (this *bufWebSockReader[T]) Close() (err error)

func (this *bufWebSockReader[T]) writeFrame(text []byte) error {
	var buf [8 + 4096]byte
	buf[0] = 0x80 | 1
	buf[1] = 0x80 | 126

	var length = len(text)
	buf[2] = byte(length >> 8)
	buf[3] = byte(length)

	var mask = this.mask
	mask ^= mask << 13
	mask ^= mask >> 17
	mask ^= mask << 5
	if mask == 0 {
		mask = 1
	}
	this.mask = mask

	buf[4] = byte(mask)
	buf[5] = byte(mask >> 8)
	buf[6] = byte(mask >> 16)
	buf[7] = byte(mask >> 24)

	for index := range length {
		buf[8+index] = text[index] ^ buf[4+(index&3)]
	}

	_, err := this.conn.Write(buf[:8+length])
	return err
}

func (this *bufWebSockReader[T]) sendClose() error {
	var buf [8]byte
	buf[0] = 0x80 | 8
	buf[1] = 0x80 | 2

	var mask = this.mask
	mask ^= mask << 13
	mask ^= mask >> 17
	mask ^= mask << 5
	if mask == 0 {
		mask = 1
	}
	this.mask = mask

	buf[2] = byte(mask)
	buf[3] = byte(mask >> 8)
	buf[4] = byte(mask >> 16)
	buf[5] = byte(mask >> 24)

	const closeNormal uint16 = 1000
	high, low := closeNormal>>8, closeNormal
	buf[6] = byte(high) ^ buf[2]
	buf[7] = byte(low) ^ buf[3]

	_, err := this.conn.Write(buf[:])
	return err
}
