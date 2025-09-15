package source

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"strings"

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
		Next() error
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
	origin string
}

func NewSource(
	symbol Proto[core.SymbolEvent],
	book Proto[core.BookEvent],
	candle Proto[core.CandleEvent],
	trade Proto[core.TradeEvent],
	origin string,
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
	return newBufWebSockReader(this.origin, this.symbol, key)
}
func (this *source) Book(key core.Key) (Reader[core.BookEvent], error) {
	return newBufWebSockReader(this.origin, this.book, key)
}
func (this *source) Candle(key core.Key) (Reader[core.CandleEvent], error) {
	return newBufWebSockReader(this.origin, this.candle, key)
}
func (this *source) Trade(key core.Key) (Reader[core.TradeEvent], error) {
	return newBufWebSockReader(this.origin, this.trade, key)
}

type bufWebSockReader[T core.Event] struct {
	conn    *tls.Conn
	proto   Proto[T]
	decoded T
	buf     [8192]byte
	mask    uint32
	key     core.Key
}

var (
	ErrBadUpgrade = errors.New("matt::chasm::source: bad upgrade")
	ErrBadRead    = errors.New("matt::chasm::source: bad read")
)

func newBufWebSockReader[T core.Event](origin string, proto Proto[T], key core.Key) (Reader[T], error) {
	conn, err := tls.Dial("tcp4", origin, nil)
	if err != nil {
		return nil, err
	}

	var (
		buf [512]byte
		n   int

		as = func(s string) { n += copy(buf[n:], s) }
		ab = func(b []byte) { n += copy(buf[n:], b) }
	)

	const lineEnd string = "\r\n"

	as("GET ")
	as(proto.Endpoint())
	as(" HTTP/1.1")
	as(lineEnd)

	as("Host: ")
	as(origin)
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

	as("Sec-WebSocket-Version: 13")
	as(lineEnd)

	as(lineEnd)

	_, err = conn.Write(buf[:n])
	if err != nil {
		conn.Close()
		return nil, err
	}

	var (
		ruf  [512]byte
		r    int
		step int = 1
		end  int = len(ruf) - step
	)

	for r < end {
		n, err := conn.Read(ruf[r : r+step])
		if err != nil {
			return nil, err
		}

		r += n
		if r >= 4 && ruf[r-4] == '\r' && ruf[r-3] == '\n' && ruf[r-2] == '\r' && ruf[r-1] == '\n' {
			break
		}
	}

	if !strings.Contains(string(ruf[:r]), " 101 ") {
		conn.Close()
		return nil, ErrBadUpgrade
	}

	return &bufWebSockReader[T]{
		conn:  conn,
		proto: proto,
		mask:  binary.LittleEndian.Uint32(raw[:4]),
		key:   key,
	}, nil
}

func (this *bufWebSockReader[T]) Read() T { return this.decoded }

func (this *bufWebSockReader[T]) Next() error {
	length := this.bufferTextFrame()
	if length < 0 {
		return ErrBadRead
	}

	decoded, err := this.proto.Decode(this.buf[:length])
	if err != nil {
		return err
	}

	this.decoded = decoded
	return nil
}

func (this *bufWebSockReader[T]) bufferTextFrame() int {
	const (
		fmask byte = 0x80
		omask byte = 0x0F
		hmask byte = 0x7F
		text  byte = 0x1
		cont  byte = 0x0
		ping  byte = 0x9
		pong  byte = 0xA
	)
	var length int
start:
	var (
		hdr [2]byte
		off int
	)

	for off < 2 {
		n, err := this.conn.Read(hdr[off:])
		if err != nil {
			return -1
		}

		off += n
	}

	var (
		final = (hdr[0] & fmask) != 0
		op    = (hdr[0] & omask)
		hint  = int(hdr[1] & hmask)
	)

	switch op {
	case text:
	case cont:
	case ping:
		var buf [6 + 125]byte
		buf[0] = 0x80 | 10
		buf[1] = 0x80 | byte(hint)

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

		var (
			data [125]byte
			off  int
		)

		for off < hint {
			n, err := this.conn.Read(data[off:hint])
			if err != nil {
				return -1
			}

			off += n
		}

		for index := range hint {
			buf[6+index] = data[index] ^ buf[2+(index&3)]
		}

		if _, err := this.conn.Write(buf[:6+hint]); err != nil {
			return -1
		}

		goto start
	case pong:
		var buf [125]byte
		for hint > 0 {
			n, err := this.conn.Read(buf[:hint])
			if err != nil {
				return -1
			}

			hint -= n
		}

		goto start
	default:
		return -1
	}

	switch hint {
	case 126:
		var (
			ext [2]byte
			off int
		)

		for off < 2 {
			n, err := this.conn.Read(ext[off:])
			if err != nil {
				return -1
			}

			off += n
		}

		var plen = int(binary.BigEndian.Uint16(ext[:]))
		if (length + plen) > len(this.buf) {
			return -1
		}

		dst := this.buf[length : length+plen]
		off = 0
		for off < plen {
			n, err := this.conn.Read(dst[off:])
			if err != nil {
				return -1
			}

			off += n
		}

		length += plen
		if !final {
			goto start
		}

		return length
	case 127:
		return -1
	default:
		if (length + hint) > len(this.buf) {
			return -1
		}

		var (
			dst = this.buf[length : length+hint]
			off int
		)

		for off < hint {
			n, err := this.conn.Read(dst[off:])
			if err != nil {
				return -1
			}

			off += n
		}

		length += hint
		if !final {
			goto start
		}

		return length
	}
}

func (this *bufWebSockReader[T]) Close() error {
	this.writeTextFrame(this.proto.Unsubscribe(this.key))
	this.writeCloseFrame()
	return this.conn.Close()
}

func (this *bufWebSockReader[T]) writeTextFrame(data []byte) {
	var buf [8 + 4096]byte
	buf[0] = 0x80 | 1
	buf[1] = 0x80 | 126

	var length = len(data)
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
		buf[8+index] = data[index] ^ buf[4+(index&3)]
	}

	this.conn.Write(buf[:8+length])
}

func (this *bufWebSockReader[T]) writeCloseFrame() {
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

	this.conn.Write(buf[:])
}
