package source

import (
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/ringboundio/chasm/core"
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
		Read() (T, bool)
		Next() error
	}

	Proto interface {
		Endpoint() string
		Subscribe(key core.Key) []byte
		Unsubscribe(key core.Key) []byte
	}

	SymbolProto interface {
		Proto
		MultiDecoder[core.SymbolEvent]
	}

	BookProto interface {
		Proto
		SingleDecoder[core.BookEvent]
	}

	CandleProto interface {
		Proto
		SinkDecoder[core.CandleEvent]
	}

	TradeProto interface {
		Proto
		MultiDecoder[core.TradeEvent]
	}

	KeyGetter interface {
		Online() []core.Key
		Offline() []core.Key
		All() []core.Key
	}
)

type source struct {
	origin string
	symbol SymbolProto
	book   BookProto
	trade  TradeProto
	candle CandleProto
	demux  map[core.Key]*candleDemux
	getter KeyGetter
}

func NewSource(origin string,
	symbol SymbolProto, book BookProto, trade TradeProto, candle CandleProto, getter KeyGetter) Source {
	return &source{
		origin: origin,
		symbol: symbol,
		book:   book,
		trade:  trade,
		candle: candle,
		demux:  make(map[core.Key]*candleDemux, 1024),
		getter: getter,
	}
}

func (s *source) Symbol(key core.Key) Reader[core.SymbolEvent] {
	return newSymbolReader(s.origin, s.symbol, key)
}

func (s *source) Book(key core.Key) Reader[core.BookEvent] {
	return newBookReader(s.origin, s.book, key)
}

func (s *source) Candle(key core.Key) Reader[core.CandleEvent] { // TODO: demuxers are never dereferenced
	symbol := core.NewKey(core.DecodeSymbol(key))

	if demux, ok := s.demux[symbol]; ok {
		return newCandleReader(demux, key)
	}

	demux := newCandleDemux(s.origin, s.candle)
	s.demux[symbol] = demux
	return newCandleReader(demux, key)
}

func (s *source) Trade(key core.Key) Reader[core.TradeEvent] {
	return newTradeReader(s.origin, s.trade, key)
}

func (s *source) Get() KeyGetter { return s.getter }

type symbolReader struct {
	proto      SymbolProto
	key        core.Key
	start, end int16
	ok         bool
	buf        [1024]core.SymbolEvent
	_          [32]byte
	socket     tcp4WebSocket
	origin     string
}

func newSymbolReader(origin string, proto SymbolProto, key core.Key) *symbolReader {
	return &symbolReader{
		proto:  proto,
		key:    key,
		origin: origin,
	}
}

func (r *symbolReader) Open() error {
	return open(&r.socket, r.origin, r.proto.Endpoint(), r.proto.Subscribe(r.key))
}

func (r *symbolReader) Close() error                   { return close(&r.socket, r.proto.Unsubscribe(r.key)) }
func (r *symbolReader) Read() (core.SymbolEvent, bool) { return r.buf[r.start], r.ok }

func (r *symbolReader) Next() error {
	if r.end > 1 {
		r.end--
		r.start++
		return nil
	}

	length := r.socket.bufferTextFrame()
	if length < 0 {
		return ErrBadRead
	}

	length, ok, err := r.proto.Decode(r.socket.buf[:length], r.buf[:])
	if err != nil {
		return err
	}
	r.end = int16(length)

	r.ok = ok
	r.start = 0
	return nil
}

type bookReader struct {
	proto   BookProto
	decoded core.BookEvent
	ok      bool
	key     core.Key
	_       [8]byte
	socket  tcp4WebSocket
	origin  string
}

func newBookReader(origin string, proto BookProto, key core.Key) *bookReader {
	return &bookReader{
		proto:  proto,
		key:    key,
		origin: origin,
	}
}

func (r *bookReader) Open() error {
	return open(&r.socket, r.origin, r.proto.Endpoint(), r.proto.Subscribe(r.key))
}

func (r *bookReader) Close() error                 { return close(&r.socket, r.proto.Unsubscribe(r.key)) }
func (r *bookReader) Read() (core.BookEvent, bool) { return r.decoded, r.ok }

func (r *bookReader) Next() error {
	length := r.socket.bufferTextFrame()
	if length < 0 {
		return ErrBadRead
	}

	decoded, ok, err := r.proto.Decode(r.socket.buf[:length])
	if err != nil {
		return err
	}

	r.decoded = decoded
	r.ok = ok
	return nil
}

type candleDemux struct {
	socket tcp4WebSocket

	read, total int8
	bad         bool

	origin string
	proto  CandleProto
	mu     sync.Mutex
	cond   *sync.Cond
}

func newCandleDemux(origin string, proto CandleProto) *candleDemux {
	demux := &candleDemux{
		origin: origin,
		proto:  proto,
	}

	demux.cond = sync.NewCond(&demux.mu)
	return demux
}

func (d *candleDemux) open(key core.Key) error {
	d.mu.Lock()
	if d.total == 0 {
		if err := open(&d.socket, d.origin, d.proto.Endpoint(), d.proto.Subscribe(key)); err != nil {
			d.proto.Unsubscribe(key)
			d.mu.Unlock()
			return err
		}

		d.total++
		d.mu.Unlock()
		return nil
	}

	d.total++
	d.read++

	d.proto.Subscribe(key)
	d.mu.Unlock()
	return nil
}

func (d *candleDemux) close(key core.Key) error {
	d.mu.Lock()
	if d.total == 1 {
		d.total--
		err := close(&d.socket, d.proto.Unsubscribe(key))
		d.mu.Unlock()
		return err
	}

	d.total--
	d.proto.Unsubscribe(key)
	d.mu.Unlock()
	return nil
}

func (d *candleDemux) next(reader *candleReader) (err error) {
	d.mu.Lock()
	if d.read == d.total {
		d.read = 0
		d.mu.Unlock()

		length := d.socket.bufferTextFrame()
		if length < 0 {
			d.mu.Lock()
			d.read++
			d.bad = true
			d.mu.Unlock()
			d.cond.Broadcast()
			return ErrBadRead
		}
		reader.decoded, reader.ok, err = d.proto.Sink(reader.key, d.socket.buf[:length])

		d.mu.Lock()
		d.read++
		d.mu.Unlock()
		d.cond.Broadcast()
		return err
	}

	d.cond.Wait()
	if d.bad {
		d.read++
		d.mu.Unlock()
		return ErrBadRead
	}
	reader.decoded, reader.ok, err = d.proto.Rebuild(reader.key)

	d.read++
	d.mu.Unlock()
	return err
}

type candleReader struct {
	decoded core.CandleEvent
	ok      bool
	demux   *candleDemux
	key     core.Key
}

func newCandleReader(demux *candleDemux, key core.Key) *candleReader {
	return &candleReader{
		demux: demux,
		key:   key,
	}
}

func (r *candleReader) Open() error                    { return r.demux.open(r.key) }
func (r *candleReader) Close() error                   { return r.demux.close(r.key) }
func (r *candleReader) Read() (core.CandleEvent, bool) { return r.decoded, r.ok }
func (r *candleReader) Next() error                    { return r.demux.next(r) }

type tradeReader struct {
	proto      TradeProto
	key        core.Key
	start, end int16
	ok         bool
	buf        [128]core.TradeEvent
	_          [32]byte
	socket     tcp4WebSocket
	origin     string
}

func newTradeReader(origin string, proto TradeProto, key core.Key) *tradeReader {
	return &tradeReader{
		proto:  proto,
		key:    key,
		origin: origin,
	}
}

func (r *tradeReader) Open() error {
	return open(&r.socket, r.origin, r.proto.Endpoint(), r.proto.Subscribe(r.key))
}

func (r *tradeReader) Close() error                  { return close(&r.socket, r.proto.Unsubscribe(r.key)) }
func (r *tradeReader) Read() (core.TradeEvent, bool) { return r.buf[r.start], r.ok }

func (r *tradeReader) Next() error {
	if r.end > 1 {
		r.end--
		r.start++
		return nil
	}

	length := r.socket.bufferTextFrame()
	if length < 0 {
		return ErrBadRead
	}

	length, ok, err := r.proto.Decode(r.socket.buf[:length], r.buf[:])
	if err != nil {
		return err
	}
	r.end = int16(length)

	r.ok = ok
	r.start = 0
	return nil
}

func open(socket *tcp4WebSocket, origin, endpoint string, message []byte) error {
	if err := socket.dial(origin, endpoint); err != nil {
		return err
	}

	socket.writeTextFrame(message)
	return nil
}

func close(socket *tcp4WebSocket, message []byte) error {
	socket.writeTextFrame(message)
	return socket.close()
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

func (g *httpGetter) Online() []core.Key  { return g.handle(g.online) }
func (g *httpGetter) Offline() []core.Key { return g.handle(g.offline) }
func (g *httpGetter) All() []core.Key     { return g.handle(g.all) }

func (g *httpGetter) handle(decode DecodeKeyFunc) []core.Key {
	resp, err := g.client.Do(&g.req)
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
