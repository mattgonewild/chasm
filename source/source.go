package source

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/mattgonewild/chasm/core"
)

type (
	Source interface {
		Symbol(key core.Key) Reader[core.SymbolEvent]
		Book(key core.Key) Reader[core.BookEvent]
		Candle(key core.Key) Reader[core.CandleEvent]
		Trade(key core.Key) Reader[core.TradeEvent]
		Get() KeyGetter
	}

	Reader[T core.Event] interface {
		Open() error
		Close() error
		Read() T
		Next() error
	}

	Proto[T core.Event] interface {
		Endpoint() string
		Subscribe(key core.Key) []byte
		Unsubscribe(key core.Key) []byte
		Decode(frame []byte) (T, error)
	}

	KeyGetter interface {
		Online() []core.Key
		Offline() []core.Key
		All() []core.Key
	}
)

type source struct {
	symbol Proto[core.SymbolEvent]
	book   Proto[core.BookEvent]
	candle Proto[core.CandleEvent]
	trade  Proto[core.TradeEvent]
	origin string
	getter KeyGetter
}

func NewSource(
	symbol Proto[core.SymbolEvent],
	book Proto[core.BookEvent],
	candle Proto[core.CandleEvent],
	trade Proto[core.TradeEvent],
	origin string,
	getter KeyGetter,
) Source {
	return &source{
		symbol: symbol,
		book:   book,
		candle: candle,
		trade:  trade,
		origin: origin,
		getter: getter,
	}
}

func (this *source) Symbol(key core.Key) Reader[core.SymbolEvent] {
	return newBufWebSockReader(this.origin, this.symbol, key)
}

func (this *source) Book(key core.Key) Reader[core.BookEvent] {
	return newBufWebSockReader(this.origin, this.book, key)
}

func (this *source) Candle(key core.Key) Reader[core.CandleEvent] {
	return newBufWebSockReader(this.origin, this.candle, key)
}

func (this *source) Trade(key core.Key) Reader[core.TradeEvent] {
	return newBufWebSockReader(this.origin, this.trade, key)
}

func (this *source) Get() KeyGetter { return this.getter }

type bufWebSockReader[T core.Event] struct {
	conn    *tls.Conn
	proto   Proto[T]
	decoded T
	buf     [8192]byte
	mask    uint32
	key     core.Key
	origin  string
}

var (
	ErrBadUpgrade = errors.New("matt::chasm::source: bad upgrade")
	ErrBadRead    = errors.New("matt::chasm::source: bad read")
)

func newBufWebSockReader[T core.Event](origin string, proto Proto[T], key core.Key) Reader[T] {
	return &bufWebSockReader[T]{
		proto:  proto,
		key:    key,
		origin: origin,
	}
}

func (this *bufWebSockReader[T]) Open() error {
	conn, err := tls.Dial("tcp4", this.origin, nil)
	if err != nil {
		return err
	}

	var (
		buf [512]byte
		n   int

		as = func(s string) { n += copy(buf[n:], s) }
		ab = func(b []byte) { n += copy(buf[n:], b) }
	)

	const lineEnd string = "\r\n"

	as("GET ")
	as(this.proto.Endpoint())
	as(" HTTP/1.1")
	as(lineEnd)

	as("Host: ")
	as(this.origin)
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
		return err
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
			return err
		}

		r += n
		if r >= 4 && ruf[r-4] == '\r' && ruf[r-3] == '\n' && ruf[r-2] == '\r' && ruf[r-1] == '\n' {
			break
		}
	}

	if !strings.Contains(string(ruf[:r]), " 101 ") {
		conn.Close()
		return ErrBadUpgrade
	}

	this.conn = conn
	this.mask = binary.LittleEndian.Uint32(raw[:4])
	this.writeTextFrame(this.proto.Subscribe(this.key))
	return nil
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

// TODO: we should be returning an error
type DecodeKeyFunc func(raw []byte) []core.Key

type httpGetter struct {
	online  DecodeKeyFunc
	offline DecodeKeyFunc
	all     DecodeKeyFunc
	req     http.Request
	client  http.Client
}

func NewKeyGetter(online, offline, all DecodeKeyFunc, req http.Request) KeyGetter {
	return &httpGetter{
		online:  online,
		offline: offline,
		all:     all,
		req:     req,
		client: http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

func (this *httpGetter) Online() []core.Key  { return this.handle(this.online) }
func (this *httpGetter) Offline() []core.Key { return this.handle(this.offline) }
func (this *httpGetter) All() []core.Key     { return this.handle(this.all) }

func (this *httpGetter) handle(decode DecodeKeyFunc) []core.Key {
	resp, err := this.client.Do(&this.req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil
	}

	return decode(body)
}
