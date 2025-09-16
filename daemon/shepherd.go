package daemon

import (
	"errors"
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
	Await int8 = iota
	Init
	On
	Off
	Suspended
	Fault

	shepIntMax uint8 = 13
)

type ShepherdConfig struct {
	Ignore   map[core.Key]bool
	Interval [shepIntMax]int16
	Limit    uint16
	Source   source.Source
	Sink     sink.Sink
}

type shepherdFactory struct {
	ignore   map[core.Key]bool
	id       uuid.UUID
	interval [shepIntMax]int16
	limit    uint16
	iLen     uint8
	source   source.Source
	sink     sink.Sink
}

var (
	ErrNilSource   = errors.New("matt::chasm::daemon: nil source")
	ErrNilSink     = errors.New("matt::chasm::daemon: nil sink")
	ErrNilIgnore   = errors.New("matt::chasm::daemon: nil ignore")
	ErrNilInterval = errors.New("matt::chasm::daemon: nil interval")
	ErrBadLimit    = errors.New("matt::chasm::daemon: bad limit")
	ErrBadInterval = errors.New("matt::chasm::daemon: bad interval")
)

func NewShepherdFactory(cfg ShepherdConfig) (core.SymbolFactory, error) {
	if cfg.Source == nil {
		return nil, ErrNilSource
	}

	if cfg.Sink == nil {
		return nil, ErrNilSink
	}

	if cfg.Ignore == nil {
		return nil, ErrNilIgnore
	}

	if cfg.Limit < 1 {
		return nil, ErrBadLimit
	}

	var (
		buf [shepIntMax]int16
		n   int
	)

	for _, value := range cfg.Interval {
		if value == 0 {
			continue
		}

		if core.OkInterval(int64(value)) {
			buf[n] = value
			n++
			continue
		}

		return nil, ErrBadInterval
	}

	return &shepherdFactory{
		ignore:   cfg.Ignore,
		id:       uuid.New(),
		interval: buf,
		limit:    cfg.Limit,
		iLen:     uint8(n),
		source:   cfg.Source,
		sink:     cfg.Sink,
	}, nil
}

func (this *shepherdFactory) New() core.SymbolDaemon

func (this *shepherdFactory) ID() uuid.UUID { return this.id }

type shepherd struct {
	since    int64
	ignore   map[core.Key]bool
	id       uuid.UUID
	interval [shepIntMax]int16
	limit    uint16
	code     int8
	iLen     uint8

	source   source.Source
	sink     sink.Sink
	registry core.SymbolLogRegistry
	linker   core.Linker

	config   []byte
	unlinker core.Unlinker
	mu       sync.Mutex
	_        [16]byte
	report   [64]byte
}

func (this *shepherd) SetConfig(config []byte) error {
	cfg := new(proto.ShepherdConfig)
	if err := protobuf.Unmarshal(config, cfg); err != nil {
		return err
	}

	if cfg.Ignore == nil {
		return ErrNilIgnore
	}

	if cfg.Limit < 1 {
		return ErrBadLimit
	}

	if cfg.Interval == nil {
		return ErrNilInterval
	}

	if len(cfg.Interval) > int(shepIntMax) {
		return ErrBadInterval
	}

	var (
		buf [shepIntMax]int16
		n   int
	)

	for _, value := range cfg.Interval {
		if value == 0 {
			continue
		}

		if core.OkInterval(int64(value)) {
			buf[n] = int16(value)
			n++
			continue
		}

		return ErrBadInterval
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

func (this *shepherd) Run() error {
	return nil
}

func (this *shepherd) Shutdown() error {
	return nil
}

func (this *shepherd) Pause() error {
	return nil
}

func (this *shepherd) Resume() error {
	return nil
}

func (this *shepherd) Restart() error {
	return nil
}

func (this *shepherd) Tag() string          { return "control" }
func (this *shepherd) Config() []byte       { return this.config }
func (this *shepherd) Status() (int, int64) { return int(this.code), this.since }
func (this *shepherd) Report() []byte       { return this.report[:] }
func (this *shepherd) ID() uuid.UUID        { return this.id }

func (this *shepherd) Initialize(registry core.SymbolLogRegistry) error {
	this.since = kit.UnixNano()
	this.code = Init
	this.registry = registry
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
