package core

import (
	"errors"

	"github.com/google/uuid"
	"github.com/mattgonewild/kit"
)

type (
	Manager interface {
		Linker
		Revive(id uuid.UUID) error
		Get(id uuid.UUID) (Daemon, error)
		DaemonInfo(hidden bool, selector Selector) []DaemonInfo
		Pause(hidden bool, selector Selector) error
		Resume(hidden bool, selector Selector) error
		Restart(hidden bool, selector Selector) error
		SetConfig(hidden bool, selector Selector, config []byte) error
		Hide(selector Selector) error
		Show(selector Selector) error
		Unlinker
		Shutdown() error
	}

	Linker interface {
		AddSymbol(SymbolFactory) error
		AddBook(BookFactory) error
		AddCandle(CandleFactory) error
		AddTrade(TradeFactory) error
		AddSchedule(ScheduleFactory) error
		AddData(DataFactory) error
	}

	Unlinker interface {
		Kill(id uuid.UUID) error
	}
)

type Config struct {
	// Lineage tracks factories seen and entries are not deleted.
	// This is an initial bucket hint only; maps grow as needed, never shrink.
	// Per entry ≈ 32 B.
	Lineage int

	// Factories are kept around forever so daemons can be revived.
	// Fields are initial bucket hints only; maps grow as needed, never shrink.
	// Per entry ≈ 32 B.
	Factory FactoryConfig

	// Fields are initial bucket hints only; registries grow as needed, never shrink.
	// Per entry ≈ 24 B.
	Registry RegistryConfig

	// Must not be nil.
	OnAdd  func(DaemonInfo)
	OnKill func(DaemonInfo)
}

type FactoryConfig struct {
	Symbol   int
	Book     int
	Candle   int
	Trade    int
	Schedule int
	Data     int
}

type RegistryConfig struct {
	Symbol   int
	Book     int
	Candle   int
	Trade    int
	Schedule int
}

var (
	ErrInvalid = errors.New("matt::chasm::core: invalid")
	ErrLocked  = errors.New("matt::chasm::core: locked")
	ErrNoOp    = errors.New("matt::chasm::core: no-op")
)

func NewManager7(cfg Config) Manager  { return newManager7(cfg) }
func NewManager8(cfg Config) Manager  { return newManager8(cfg) }
func NewManager9(cfg Config) Manager  { return newManager9(cfg) }
func NewManager10(cfg Config) Manager { return newManager10(cfg) }
func NewManager11(cfg Config) Manager { return newManager11(cfg) }
func NewManager12(cfg Config) Manager { return newManager12(cfg) }
func NewManager13(cfg Config) Manager { return newManager13(cfg) }
func NewManager14(cfg Config) Manager { return newManager14(cfg) }
func NewManager15(cfg Config) Manager { return newManager15(cfg) }
func NewManager16(cfg Config) Manager { return newManager16(cfg) }
func NewManager17(cfg Config) Manager { return newManager17(cfg) }
func NewManager18(cfg Config) Manager { return newManager18(cfg) }
func NewManager19(cfg Config) Manager { return newManager19(cfg) }
func NewManager20(cfg Config) Manager { return newManager20(cfg) }
func NewManager21(cfg Config) Manager { return newManager21(cfg) }
func NewManager22(cfg Config) Manager { return newManager22(cfg) }
func NewManager23(cfg Config) Manager { return newManager23(cfg) }
func NewManager24(cfg Config) Manager { return newManager24(cfg) }
func NewManager25(cfg Config) Manager { return newManager25(cfg) }
func NewManager26(cfg Config) Manager { return newManager26(cfg) }
func NewManager27(cfg Config) Manager { return newManager27(cfg) }
func NewManager28(cfg Config) Manager { return newManager28(cfg) }
func NewManager29(cfg Config) Manager { return newManager29(cfg) }
func NewManager30(cfg Config) Manager { return newManager30(cfg) }
func NewManager31(cfg Config) Manager { return newManager31(cfg) }
func NewManager32(cfg Config) Manager { return newManager32(cfg) }
func NewManager33(cfg Config) Manager { return newManager33(cfg) }
func NewManager34(cfg Config) Manager { return newManager34(cfg) }

type domain uint8

const (
	symbol domain = iota
	book
	candle
	trade
	schedule
	data
	domainCount
)

type Lineage struct {
	alive  bool
	domain domain
	ptr    *container
}

func newLineage(domain domain, ptr *container) Lineage {
	return Lineage{alive: true, domain: domain, ptr: ptr}
}

func reviveLineage(lineage Lineage, ptr *container) Lineage {
	lineage.ptr = ptr
	lineage.alive = true
	return lineage
}

func endLineage(lineage Lineage) Lineage {
	lineage.alive = false
	lineage.ptr = nil
	return lineage
}

func (this Lineage) Alive() bool               { return this.alive }
func (this Lineage) HideContainer(hidden bool) { this.ptr.hidden = hidden }

type container struct {
	Daemon
	id     uint64
	hidden bool
}

func newContainer(daemon Daemon, id uint64) container {
	return container{Daemon: daemon, id: id}
}

func (this container) Before(that container) bool { return this.id < that.id }
func (this container) Equal(that container) bool  { return this.id == that.id }
func (this container) After(that container) bool  { return this.id > that.id }

func (this container) Compare(that container) int {
	return kit.BoolToInt(this.id > that.id) - kit.BoolToInt(this.id < that.id)
}

type newDaemonFunc func(id uuid.UUID) (Daemon, error)
