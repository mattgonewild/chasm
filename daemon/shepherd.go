package daemon

import (
	"encoding/binary"
	"math"
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
	shepName    string = "TODO"
	shepVersion string = "TODO"
	shepIntCap  uint8  = 10
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

func (s *shepherd) Run() error { return s.tryBoot() }

func (s *shepherd) tryBoot() error {
	s.mu.Lock()
	if s.code == Init || s.code == Off {
		s.reportState(Booting)
		s.mu.Unlock()

		go s.microKernel()
		return nil
	}

	s.mu.Unlock()
	return errInvalid
}

func (s *shepherd) microKernel() { // TODO: ...
	writer := s.sink.Symbol(core.Key(math.MaxUint8))
start:
	reader := s.source.Symbol(core.Key(math.MaxUint8))
	if err := reader.Open(); err != nil {
		s.reportFault(errOpen, err)
		return
	}

active:
	s.reportState(On)
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
		default: // TODO: ...
			if err := reader.Next(); err != nil {
				s.reportFault(errNext, err)
				return
			}

			var (
				event = reader.Read()
				key   = core.EncodeSymbolKey(event.Symbol)
			)

			if event.Online {
				s.handleOnline(key)
			} else {
				s.handleOffline(key)
			}

			if err := writer.Write(event); err != nil {
				reader.Close()
				s.reportFault(errWrite, err)
				return
			}

			continue
		}
	}

sleep:
	s.reportState(Suspended)
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

func (s *shepherd) reportState(code stateCode) {
	s.since = kit.UnixNano()
	s.code = code
	s.report = newStateReport(shepName, shepVersion, code)
}

func (s *shepherd) reportFault(code errorCode, upstream error) {
	s.since = kit.UnixNano()
	s.code = Fault
	s.report = newFaultReport(shepName, shepVersion, code, upstream)
}

func (s *shepherd) awkOk(cmd pack)             { kit.Close(cmd.awk) }
func (s *shepherd) awkErr(cmd pack, err error) { cmd.awk <- err; kit.Close(cmd.awk) }

func (s *shepherd) awkOkTransitionTo(cmd pack, reader source.Reader[core.SymbolEvent], code stateCode) {
	kit.Close(cmd.awk)

	if reader != nil {
		reader.Close()
	}

	s.reportState(code)
}

func (s *shepherd) syncInConfig() {
	ignored, visible := s.diffIgnore()
	newInt, delInt, newIntLen, delIntLen := s.diffInterval()

	for _, key := range ignored {
		s.killBook(key)
		s.killTrade(key)
		for _, value := range s.interval[:s.iLen] {
			s.killCandle(key, value)
		}
	}

	supported := s.source.Supported()
	if delIntLen > 0 {
		for _, key := range supported {
			if s.ignore[key] || s.in.ignore[key] {
				continue
			}

			for _, value := range delInt[:delIntLen] {
				s.killCandle(key, value)
			}
		}
	}

	s.ignore = s.in.ignore
	s.interval = s.in.interval
	s.iLen = s.in.iLen

	spawned := make(map[core.Key]bool, len(visible))
	for _, key := range visible {
		spawned[key] = true
		s.spawnBook(key)
		s.spawnTrade(key)
		for _, value := range s.interval[:s.iLen] {
			s.spawnCandle(key, value)
		}
	}

	if newIntLen > 0 {
		for _, key := range supported {
			if s.ignore[key] || spawned[key] {
				continue
			}

			for _, value := range newInt[:newIntLen] {
				s.spawnCandle(key, value)
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

func (s *shepherd) killBook(key core.Key)                  { s.kill(s.spawnID(core.Book, key)) }
func (s *shepherd) killTrade(key core.Key)                 { s.kill(s.spawnID(core.Trade, key)) }
func (s *shepherd) killCandle(key core.Key, minute int16)  { s.kill(s.candleSpawnID(key, minute)) }
func (s *shepherd) spawnBook(key core.Key)                 { s.addBook(s.spawnID(core.Book, key)) }
func (s *shepherd) spawnTrade(key core.Key)                { s.addTrade(s.spawnID(core.Trade, key)) }
func (s *shepherd) spawnCandle(key core.Key, minute int16) { s.addCandle(s.candleSpawnID(key, minute)) }
func (s *shepherd) handleOnline(key core.Key)              { s.forEach(key, s.start) }
func (s *shepherd) handleOffline(key core.Key)             { s.forEach(key, s.pause) }

func (s *shepherd) forEach(key core.Key, yield func(uuid.UUID) error) {
	if s.ignore[key] {
		return
	}

	yield(s.spawnID(core.Book, key))
	yield(s.spawnID(core.Trade, key))

	var (
		interval = s.interval
		iLen     = s.iLen
	)

	for _, value := range interval[:iLen] {
		yield(s.candleSpawnID(key, value))
	}
}

func (s *shepherd) candleSpawnID(key core.Key, minute int16) uuid.UUID {
	key = core.PromoteSymbolKey(key, int64(minute))
	return s.spawnID(core.Candle, key)
}

func (s *shepherd) spawnID(domain core.Domain, key core.Key) uuid.UUID {
	msb := binary.LittleEndian.Uint64(s.id[:8])
	msb &= ^uint64(0xFFFF)
	msb |= uint64(0x8) << 12
	msb |= uint64(domain)

	lsb := uint64(key)
	lsb |= uint64(2) << 62

	id := uuid.Nil
	binary.BigEndian.PutUint64(id[:8], msb)
	binary.BigEndian.PutUint64(id[8:], lsb)
	return id
}

func (s *shepherd) start(id uuid.UUID) error {
	if s.resume(id) == nil {
		return nil
	}

	switch core.Domain(binary.BigEndian.Uint64(id[:8]) & 0x0FFF) {
	case core.Book:
		return s.addBook(id)
	case core.Candle:
		return s.addCandle(id)
	case core.Trade:
		return s.addTrade(id)
	default:
		return errInvalid
	}
}

func (s *shepherd) pause(id uuid.UUID) error     { return s.linker.PauseID(id) }
func (s *shepherd) resume(id uuid.UUID) error    { return s.linker.ResumeID(id) }
func (s *shepherd) kill(id uuid.UUID) error      { return s.linker.Kill(id) }
func (s *shepherd) addBook(id uuid.UUID) error   { return s.linker.AddBook(s.newBookFactory(id)) }
func (s *shepherd) addCandle(id uuid.UUID) error { return s.linker.AddCandle(s.newCandleFactory(id)) }
func (s *shepherd) addTrade(id uuid.UUID) error  { return s.linker.AddTrade(s.newTradeFactory(id)) }

func (s *shepherd) newBookFactory(id uuid.UUID) core.BookFactory

func (s *shepherd) newCandleFactory(id uuid.UUID) core.CandleFactory {
	// key := core.Key(binary.BigEndian.Uint64(id[8:]) &^ (3 << 62))
	// _, interval := core.DecodeCandleKey(key)
	return nil
}

func (s *shepherd) newTradeFactory(id uuid.UUID) core.TradeFactory

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
	s.reportState(Init)
	return nil
}

func (s *shepherd) WithLinker(linker core.Linker) core.SymbolDaemon {
	s.linker = linker
	return s
}

func okInterval(minute int64) bool { return core.OkIntervalMinute(minute) }
