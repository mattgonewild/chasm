package daemon

import (
	"encoding/binary"
	"sync"

	"github.com/google/uuid"
	"github.com/mattgonewild/chasm/core"
	"github.com/mattgonewild/chasm/daemon/proto"
	"github.com/mattgonewild/chasm/sink"
	"github.com/mattgonewild/chasm/source"
	"github.com/mattgonewild/kit"
	protobuf "google.golang.org/protobuf/proto"
)

const (
	shepName    string   = "shepherd"
	shepVersion string   = "TODO"
	shepIntCap  uint8    = 10
	shepKey     core.Key = 0
)

type ShepherdConfig struct {
	// Ignore is a set of Keys to ignore.
	// Must not be nil.
	Ignore map[core.Key]bool

	// Controls what candle producers get spawned. Duplicates are not checked.
	// Must be within [0, 2047] and ascending.
	Interval [shepIntCap]int16

	// Where to read data from and where to write it to.
	// Must not be nil.
	Source source.Source
	Sink   sink.Sink
}

type shepherdFactory struct {
	ignore   map[core.Key]bool
	id       uuid.UUID
	interval [shepIntCap]int16
	iLen     uint8
	source   source.Source
	sink     sink.Sink
	config   []byte
}

func NewShepherdFactory(cfg ShepherdConfig) (core.SymbolFactory, error) {
	if cfg.Ignore == nil {
		return nil, errNil
	}

	if cfg.Source == nil {
		return nil, errNil
	}

	if cfg.Sink == nil {
		return nil, errNil
	}

	var (
		buf [shepIntCap]int16
		n   int
	)

	for _, value := range cfg.Interval {
		if value == 0 {
			continue
		}

		if okInterval(int64(value)) {
			buf[n] = value
			n++
			continue
		}

		return nil, errInvalid
	}

	var (
		buf32 = make([]int32, n)
		proto = &proto.ShepherdConfig{Ignore: cfg.Ignore, Interval: buf32}
	)

	for index := range buf32 {
		buf32[index] = int32(buf[index])
	}

	config, err := protobuf.Marshal(proto)
	if err != nil {
		return nil, err
	}

	return &shepherdFactory{
		ignore:   cfg.Ignore,
		id:       uuid.New(),
		interval: buf,
		iLen:     uint8(n),
		source:   cfg.Source,
		sink:     cfg.Sink,
		config:   config,
	}, nil
}

func (this *shepherdFactory) New() core.SymbolDaemon {
	return &shepherd{
		since:    kit.UnixNano(),
		ignore:   this.ignore,
		ctrl:     make(chan pack),
		id:       this.id,
		interval: this.interval,
		code:     Await,
		iLen:     this.iLen,
		source:   this.source,
		sink:     this.sink,
		in: shepInCfg{
			ignore:   this.ignore,
			interval: this.interval,
			iLen:     this.iLen,
			config:   this.config,
		},
		report: newStateReport(shepName, shepVersion, Await),
	}
}

func (this *shepherdFactory) ID() uuid.UUID { return this.id }

type shepInCfg struct {
	ignore   map[core.Key]bool
	interval [shepIntCap]int16
	_        [2]byte
	iLen     uint8
	config   []byte
}

type shepherd struct {
	since    int64
	ignore   map[core.Key]bool
	ctrl     chan pack
	id       uuid.UUID
	interval [shepIntCap]int16
	_        [2]byte
	code     stateCode
	iLen     uint8

	source   source.Source
	sink     sink.Sink
	registry core.SymbolLogRegistry
	linker   core.Linker

	in     shepInCfg
	mu     sync.Mutex
	report report
}

func (s *shepherd) SetConfig(config []byte) error {
	cfg := new(proto.ShepherdConfig)
	if err := protobuf.Unmarshal(config, cfg); err != nil {
		return err
	}

	if cfg.Ignore == nil {
		return errNil
	}

	if cfg.Interval == nil {
		return errNil
	}

	if len(cfg.Interval) > int(shepIntCap) {
		return errInvalid
	}

	var (
		buf [shepIntCap]int16
		n   int
	)

	for _, value := range cfg.Interval {
		if value == 0 {
			continue
		}

		if okInterval(int64(value)) {
			buf[n] = int16(value)
			n++
			continue
		}

		return errInvalid
	}

	s.in = shepInCfg{
		ignore:   cfg.Ignore,
		interval: buf,
		iLen:     uint8(n),
		config:   config,
	}

	return send(s.ctrl, update)
}

func (s *shepherd) Run() error { return s.boot() }

func (s *shepherd) boot() error {
	s.mu.Lock()
	if s.code == Init || s.code == Off {
		s.state(Booting)
		s.mu.Unlock()

		go s.kernel()
		return nil
	}

	s.mu.Unlock()
	return errInvalid
}

func (s *shepherd) kernel() {
	var (
		writer      = s.getSinkWriter()
		log, ourLog = s.getEventLog()
	)

	defer s.freeEventLog(&ourLog)
start:
	reader := s.getSourceReader()
	if err := reader.Open(); err != nil {
		s.fault(errOpen, err)
		return
	}

active:
	s.state(On)
	for {
		select {
		case pack := <-s.ctrl:
			switch pack.cmd {
			case shutdown:
				s.awkOkTransitionTo(pack, reader, Off)
				return
			case pause:
				s.awkOk(pack)
				goto sleep
			case resume:
				s.awkOk(pack)
				continue
			case restart:
				s.awkOkTransitionTo(pack, reader, Booting)
				goto start
			case update:
				s.awkOkTransitionTo(pack, nil, Updating)
				s.syncInConfig()
				goto active
			default:
				s.awkErr(pack, errUnknown)
				continue
			}
		default:
			if err := reader.Next(); err != nil {
				s.fault(errNext, err)
				return
			}

			event, ok := reader.Read()
			if !ok {
				continue
			}

			if event.Online {
				s.handleOnline(event.Symbol)
			} else {
				s.handleOffline(event.Symbol)
			}

			if !ourLog {
				if log, ourLog = s.claimEventLog(); !ourLog {
					goto sink
				}
			}
			log.Append(event)

		sink:
			if err := writer.Write(event); err != nil {
				reader.Close()
				s.fault(errWrite, err)
				return
			}

			continue
		}
	}

sleep:
	s.state(Suspended)
	pack := <-s.ctrl
	switch pack.cmd {
	case shutdown:
		s.awkOkTransitionTo(pack, reader, Off)
		return
	case pause:
		s.awkOk(pack)
		goto sleep
	case resume:
		s.awkOk(pack)
		goto active
	case restart:
		s.awkOkTransitionTo(pack, reader, Booting)
		goto start
	case update:
		s.awkOkTransitionTo(pack, nil, Updating)
		s.syncInConfig()
		goto sleep
	default:
		s.awkErr(pack, errUnknown)
		goto sleep
	}
}

func (s *shepherd) state(code stateCode) {
	s.since = kit.UnixNano()
	s.code = code
	s.report = newStateReport(shepName, shepVersion, code)
}

func (s *shepherd) fault(code errorCode, upstream error) {
	s.since = kit.UnixNano()
	s.code = Fault
	s.report = newFaultReport(shepName, shepVersion, code, upstream)
}

func (s *shepherd) getSourceReader() source.Reader[core.SymbolEvent] { return s.source.Symbol(shepKey) }
func (s *shepherd) getSinkWriter() sink.Writer[core.SymbolEvent]     { return s.sink.Symbol(shepKey) }

func (s *shepherd) getEventLog() (core.EventLog[core.SymbolEvent], bool) {
	_, err := s.registry.Get(shepKey)
	if err != nil {
		log := kit.NewLog[core.SymbolEvent](newLogStepWindow, newLogRetention, newLogCapPerLinkedNode)
		s.registry.Register(shepKey, log)
		return log, true
	}

	return s.claimEventLog()
}

func (s *shepherd) claimEventLog() (core.EventLog[core.SymbolEvent], bool) {
	return s.registry.Claim(shepKey)
}

func (s *shepherd) freeEventLog(ourLog *bool) {
	if *ourLog {
		s.registry.Release(shepKey)
	}
}

func (s *shepherd) awkOk(cmd pack)             { kit.Close(cmd.awk) }
func (s *shepherd) awkErr(cmd pack, err error) { cmd.awk <- err; kit.Close(cmd.awk) }

func (s *shepherd) awkOkTransitionTo(cmd pack, reader source.Reader[core.SymbolEvent], code stateCode) {
	kit.Close(cmd.awk)

	if reader != nil {
		reader.Close()
	}

	s.state(code)
}

func (s *shepherd) syncInConfig() {
	ignored, visible := s.diffIgnore()
	newInt, delInt, newIntLen, delIntLen := s.diffInterval()

	for _, key := range ignored {
		s.kill(s.bookKey(key))
		s.kill(s.tradeKey(key))
		for _, value := range s.interval[:s.iLen] {
			s.kill(s.candleKey(key, value))
		}
	}

	if delIntLen > 0 {
		all := s.source.Get().All()
		for _, key := range all {
			if s.ignore[key] || s.in.ignore[key] {
				continue
			}

			for _, value := range delInt[:delIntLen] {
				s.kill(s.candleKey(key, value))
			}
		}
	}

	s.ignore = s.in.ignore
	s.interval = s.in.interval
	s.iLen = s.in.iLen

	spawned := make(map[core.Key]bool, len(visible))
	for _, key := range visible {
		spawned[key] = true
		s.addBook(s.bookKey(key))
		s.addTrade(s.tradeKey(key))
		for _, value := range s.interval[:s.iLen] {
			s.addCandle(s.candleKey(key, value))
		}
	}

	if newIntLen > 0 {
		online := s.source.Get().Online()
		for _, key := range online {
			if s.ignore[key] || spawned[key] {
				continue
			}

			for _, value := range newInt[:newIntLen] {
				s.addCandle(s.candleKey(key, value))
			}
		}
	}
}

func (s *shepherd) diffIgnore() (in, out []core.Key) {
	var (
		added      = make([]core.Key, 0, 64)
		subtracted = make([]core.Key, 0, 64)
	)

	for key := range s.in.ignore {
		if !s.ignore[key] {
			added = append(added, key)
		}
	}

	for key := range s.ignore {
		if !s.in.ignore[key] {
			subtracted = append(subtracted, key)
		}
	}

	return added, subtracted
}

func (s *shepherd) diffInterval() (in, out [shepIntCap]int16, inLen, outLen uint8) {
	var (
		added, subtracted [shepIntCap]int16
		addLen, subLen    uint8

		old, new           = s.interval, s.in.interval
		oldLen, newLen     = s.iLen, s.in.iLen
		oldIndex, newIndex uint8
	)

	for oldIndex < oldLen && newIndex < newLen {
		old, new := old[oldIndex], new[newIndex]
		switch {
		case old == new:
			oldIndex++
			newIndex++
		case old < new:
			subtracted[subLen] = old
			subLen++
			oldIndex++
		case old > new:
			added[addLen] = new
			addLen++
			newIndex++
		}
	}

	for oldIndex < oldLen {
		subtracted[subLen] = old[oldIndex]
		subLen++
		oldIndex++
	}

	for newIndex < newLen {
		added[addLen] = new[newIndex]
		addLen++
		newIndex++
	}

	return added, subtracted, addLen, subLen
}

func (s *shepherd) handleOnline(key core.Key)  { s.forEach(key, s.start) }
func (s *shepherd) handleOffline(key core.Key) { s.forEach(key, s.pause) }

func (s *shepherd) forEach(key core.Key, yield func(core.Key) error) {
	if s.ignore[key] {
		return
	}

	yield(s.bookKey(key))
	yield(s.tradeKey(key))

	var (
		interval = s.interval
		iLen     = s.iLen
	)

	for _, value := range interval[:iLen] {
		yield(s.candleKey(key, value))
	}
}

func (s *shepherd) bookKey(key core.Key) core.Key { return core.NewDomainKey(core.Book, key) }

func (s *shepherd) candleKey(key core.Key, interval int16) core.Key {
	return core.NewCandleKey(key, int64(interval))
}

func (s *shepherd) tradeKey(key core.Key) core.Key { return core.NewDomainKey(core.Trade, key) }

func (s *shepherd) start(key core.Key) error {
	if s.resume(key) == nil {
		return nil
	}

	switch core.Domain((key >> core.SymIntBit) & core.DomainMask) {
	case core.Book:
		return s.addBook(key)
	case core.Candle:
		return s.addCandle(key)
	case core.Trade:
		return s.addTrade(key)
	default:
		return errInvalid
	}
}

func (s *shepherd) pause(key core.Key) error     { return s.linker.PauseID(s.sid(key)) }
func (s *shepherd) resume(key core.Key) error    { return s.linker.ResumeID(s.sid(key)) }
func (s *shepherd) kill(key core.Key) error      { return s.linker.Kill(s.sid(key)) }
func (s *shepherd) addBook(key core.Key) error   { return s.linker.AddBook(s.newBookFactory(key)) }
func (s *shepherd) addCandle(key core.Key) error { return s.linker.AddCandle(s.newCandleFactory(key)) }
func (s *shepherd) addTrade(key core.Key) error  { return s.linker.AddTrade(s.newTradeFactory(key)) }

func (s *shepherd) newBookFactory(key core.Key) core.BookFactory {
	return newHerdUnitFactory(s.source.Book(key), s.sink.Book(key), core.DecodeSymbol(key), s.sid(key))
}

func (s *shepherd) newCandleFactory(key core.Key) core.CandleFactory {
	return newHerdUnitFactory(s.source.Candle(key), s.sink.Candle(key), core.DecodeSymbol(key), s.sid(key))
}

func (s *shepherd) newTradeFactory(key core.Key) core.TradeFactory {
	return newHerdUnitFactory(s.source.Trade(key), s.sink.Trade(key), core.DecodeSymbol(key), s.sid(key))
}

func (s *shepherd) sid(key core.Key) uuid.UUID {
	msb := binary.BigEndian.Uint64(s.id[:8])
	msb &= ^uint64((1 << 16) - 1)
	msb |= uint64(0x8) << 12
	msb |= uint64((key >> core.SymIntBit) & core.DomainMask)

	lsb := uint64(key)
	lsb &^= uint64(3) << 62
	lsb |= uint64(2) << 62

	id := uuid.Nil
	binary.BigEndian.PutUint64(id[:8], msb)
	binary.BigEndian.PutUint64(id[8:], lsb)
	return id
}

func (s *shepherd) Shutdown() error      { return send(s.ctrl, shutdown) }
func (s *shepherd) Pause() error         { return send(s.ctrl, pause) }
func (s *shepherd) Resume() error        { return send(s.ctrl, resume) }
func (s *shepherd) Restart() error       { return send(s.ctrl, restart) }
func (s *shepherd) Tag() string          { return "control" }
func (s *shepherd) Config() []byte       { return s.in.config }
func (s *shepherd) Status() (int, int64) { return int(s.code), s.since }
func (s *shepherd) Report() []byte       { return s.report[:] }
func (s *shepherd) ID() uuid.UUID        { return s.id }

func (s *shepherd) Initialize(registry core.SymbolLogRegistry) error {
	s.registry = registry
	s.state(Init)
	return nil
}

func (s *shepherd) WithLinker(linker core.Linker) core.SymbolDaemon {
	s.linker = linker
	return s
}

func okInterval(minute int64) bool { return core.OkInterval(minute) }
