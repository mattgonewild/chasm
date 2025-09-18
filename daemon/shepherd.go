package daemon

import (
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
	shepIntCap  uint8  = 13
)

type ShepherdConfig struct {
	// Ignore is a set of Keys to ignore.
	// Must not be nil.
	Ignore map[core.Key]bool

	// Controls what candle producers get spawned. Duplicates are not checked.
	// Must be within [0, 2047].
	Interval [shepIntCap]int16

	// The maximum number of Daemons to permit.
	// Must be within [1, 65535].
	Limit uint16

	// Where to read data from and where to write it to.
	// Must not be nil.
	Source source.Source
	Sink   sink.Sink
}

type shepherdFactory struct {
	ignore   map[core.Key]bool
	id       uuid.UUID
	interval [shepIntCap]int16
	limit    uint16
	iLen     uint8
	source   source.Source
	sink     sink.Sink
	config   []byte
}

func NewShepherdFactory(cfg ShepherdConfig) (core.SymbolFactory, error) {
	if cfg.Source == nil {
		return nil, errNil
	}

	if cfg.Sink == nil {
		return nil, errNil
	}

	if cfg.Ignore == nil {
		return nil, errNil
	}

	if cfg.Limit < 1 {
		return nil, errInvalid
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
		proto = &proto.ShepherdConfig{Limit: uint32(cfg.Limit), Ignore: cfg.Ignore, Interval: buf32}
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
		limit:    cfg.Limit,
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
		id:       this.id,
		interval: this.interval,
		limit:    this.limit,
		code:     Await,
		iLen:     this.iLen,
		source:   this.source,
		sink:     this.sink,
		config:   this.config,
		ctrl:     make(chan pack),
		report:   newStateReport(shepName, shepVersion, Await),
	}
}

func (this *shepherdFactory) ID() uuid.UUID { return this.id }

type shepherd struct {
	since    int64
	ignore   map[core.Key]bool
	id       uuid.UUID
	interval [shepIntCap]int16
	limit    uint16
	code     stateCode
	iLen     uint8

	source   source.Source
	sink     sink.Sink
	registry core.SymbolLogRegistry
	linker   core.Linker

	config   []byte
	unlinker core.Unlinker
	mu       sync.Mutex
	ctrl     chan pack
	_        [8]byte
	report   report
}

func (this *shepherd) SetConfig(config []byte) error {
	cfg := new(proto.ShepherdConfig)
	if err := protobuf.Unmarshal(config, cfg); err != nil {
		return err
	}

	if cfg.Ignore == nil {
		return errNil
	}

	if cfg.Limit < 1 || cfg.Limit > math.MaxUint16 {
		return errInvalid
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

	this.mu.Lock()
	this.ignore = cfg.Ignore
	this.interval = buf
	this.limit = uint16(cfg.Limit)
	this.iLen = uint8(n)
	this.config = config
	this.mu.Unlock()
	return nil
}

func (this *shepherd) Run() error { return this.tryBoot() }

func (this *shepherd) tryBoot() error {
	this.mu.Lock()
	if this.code == Init || this.code == Off {
		this.reportState(Booting)
		this.mu.Unlock()

		go this.microKernel()
		return nil
	}

	this.mu.Unlock()
	return errInvalid
}

func (this *shepherd) microKernel() {
start:
	reader := this.source.Symbol(core.Key(math.MaxUint8))
	if err := reader.Open(); err != nil {
		this.reportFault(errOpen, err)
		return
	}

active:
	this.reportState(On)
	for {
		select {
		case pack := <-this.ctrl:
			switch pack.cmd {
			case shutdown:
				this.awkOkTransitionTo(pack, reader, Off)
				return
			case pause:
				this.awkOk(pack)
				goto sleep
			case resume:
				this.awkErr(pack, errInvalid)
				continue
			case restart:
				this.awkOkTransitionTo(pack, reader, Booting)
				goto start
			default:
				this.awkErr(pack, errUnknown)
				continue
			}
		default:
			// do work
			continue
		}
	}

sleep:
	this.reportState(Suspended)
	pack := <-this.ctrl
	switch pack.cmd {
	case shutdown:
		this.awkOkTransitionTo(pack, reader, Off)
		return
	case pause:
		this.awkErr(pack, errInvalid)
		goto sleep
	case resume:
		this.awkOk(pack)
		goto active
	case restart:
		this.awkOkTransitionTo(pack, reader, Booting)
		goto start
	default:
		this.awkErr(pack, errUnknown)
		goto sleep
	}
}

func (this *shepherd) awkOk(cmd pack)             { kit.Close(cmd.awk) }
func (this *shepherd) awkErr(cmd pack, err error) { cmd.awk <- err; kit.Close(cmd.awk) }

func (this *shepherd) awkOkTransitionTo(cmd pack, reader source.Reader[core.SymbolEvent], code stateCode) {
	kit.Close(cmd.awk)
	reader.Close()
	this.reportState(code)
}

func (this *shepherd) reportState(code stateCode) {
	this.since = kit.UnixNano()
	this.code = code
	this.report = newStateReport(shepName, shepVersion, code)
}

func (this *shepherd) reportFault(code errorCode, upstream error) {
	this.since = kit.UnixNano()
	this.code = Fault
	this.report = newFaultReport(shepName, shepVersion, code, upstream)
}

func (this *shepherd) Shutdown() error      { return send(this.ctrl, shutdown) }
func (this *shepherd) Pause() error         { return send(this.ctrl, pause) }
func (this *shepherd) Resume() error        { return send(this.ctrl, resume) }
func (this *shepherd) Restart() error       { return send(this.ctrl, restart) }
func (this *shepherd) Tag() string          { return "control" }
func (this *shepherd) Config() []byte       { return this.config }
func (this *shepherd) Status() (int, int64) { return int(this.code), this.since }
func (this *shepherd) Report() []byte       { return this.report[:] }
func (this *shepherd) ID() uuid.UUID        { return this.id }

func (this *shepherd) Initialize(registry core.SymbolLogRegistry) error {
	this.registry = registry
	this.reportState(Init)
	return nil
}

func (this *shepherd) WithLinker(linker core.Linker) core.SymbolDaemon {
	this.linker = linker
	return this
}

func (this *shepherd) WithUnlinker(unlinker core.Unlinker) core.SymbolDaemon {
	this.unlinker = unlinker
	return this
}

func okInterval(minute int64) bool { return core.OkIntervalMinute(minute) }
