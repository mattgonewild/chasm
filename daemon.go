package chasm

import (
	"errors"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/mattgonewild/common"
	"github.com/mattgonewild/kit"
)

type (
	Configurable = common.Configurable[[]byte]
	Identifiable = common.Identifiable[uuid.UUID]

	Daemon interface {
		Configurable
		Run() error
		Shutdown() error
		Pause() error
		Resume() error
		Restart() error
		DaemonInfo
	}

	DaemonInfo interface {
		Name() string
		Config() []byte
		Status() (int, int64)
		Report() []byte
		Error() error
		FactoryInfo() FactoryInfo
	}

	Factory[T Daemon] interface {
		New() T
		FactoryInfo
	}

	FactoryInfo interface {
		Identifiable
	}

	SymbolFactory   = Factory[Control[common.Log[SymbolEvent]]]
	BookFactory     = Factory[Producer[common.Log[BookEvent]]]
	CandleFactory   = Factory[Producer[common.Log[CandleEvent]]]
	TradeFactory    = Factory[Producer[common.Log[TradeEvent]]]
	ScheduleFactory = Factory[Producer[common.Log[ScheduleEvent]]]
	PluginFactory   = Factory[Plugin[BrokerageData]]

	Control[T any] interface {
		Producer[T]
		WithLinker(linker Linker) Control[T]
		WithUnlinker(unlinker Unlinker) Control[T]
	}

	Linker interface {
		AddSymbolProducer(SymbolFactory) error
		AddBookProducer(BookFactory) error
		AddCandleProducer(CandleFactory) error
		AddTradeProducer(TradeFactory) error
		AddScheduleProducer(ScheduleFactory) error
		AddPlugin(PluginFactory) error
	}

	Unlinker interface {
		Kill(id uuid.UUID) error
	}

	Producer[T any] interface {
		Daemon
		Initialize(registry Registry[T]) error
	}

	Plugin[T common.UnixTimestamped] interface {
		Daemon
		Initialize(provider Provider[T]) error
	}

	Manager interface {
		Shutdown() error
		Pause(hidden bool, filter func(DaemonInfo) bool) error
		Resume(hidden bool, filter func(DaemonInfo) bool) error
		Restart(hidden bool, filter func(DaemonInfo) bool) error
		DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo
		Hide(id uuid.UUID) error
		Show(id uuid.UUID) error
		Get(id uuid.UUID) (Daemon, error)
		Revive(id uuid.UUID) error
		Linker
		Unlinker
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

	// The base-2 exponent (log2) of the backing chasm array. It must be within [7, 34].
	// This directly limits how many daemons can be loaded at any given time so choose wisely.
	// The containers that go in are ≈ 32 B. Daemons can be killed and removed as needed.
	Chasm int

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
	Plugin   int
}

type RegistryConfig struct {
	Symbol   int
	Book     int
	Candle   int
	Trade    int
	Schedule int
}

func NewManager(cfg Config) Manager {
	switch cfg.Chasm {
	case 7:
		return newDaemonManager7(cfg)
	case 8:
		return newDaemonManager8(cfg)
	case 9:
		return newDaemonManager9(cfg)
	case 10:
		return newDaemonManager10(cfg)
	case 11:
		return newDaemonManager11(cfg)
	case 12:
		return newDaemonManager12(cfg)
	case 13:
		return newDaemonManager13(cfg)
	case 14:
		return newDaemonManager14(cfg)
	case 15:
		return newDaemonManager15(cfg)
	case 16:
		return newDaemonManager16(cfg)
	case 17:
		return newDaemonManager17(cfg)
	case 18:
		return newDaemonManager18(cfg)
	case 19:
		return newDaemonManager19(cfg)
	case 20:
		return newDaemonManager20(cfg)
	case 21:
		return newDaemonManager21(cfg)
	case 22:
		return newDaemonManager22(cfg)
	case 23:
		return newDaemonManager23(cfg)
	case 24:
		return newDaemonManager24(cfg)
	case 25:
		return newDaemonManager25(cfg)
	case 26:
		return newDaemonManager26(cfg)
	case 27:
		return newDaemonManager27(cfg)
	case 28:
		return newDaemonManager28(cfg)
	case 29:
		return newDaemonManager29(cfg)
	case 30:
		return newDaemonManager30(cfg)
	case 31:
		return newDaemonManager31(cfg)
	case 32:
		return newDaemonManager32(cfg)
	case 33:
		return newDaemonManager33(cfg)
	case 34:
		return newDaemonManager34(cfg)
	default:
		panic(ErrInvalid)
	}
}

type domain uint8

const (
	symbol domain = iota
	book
	candle
	trade
	schedule
	plugin
	domainCount
)

type lineage struct {
	alive  bool
	domain domain
	ptr    *container
}

func newLineage(domain domain, ptr *container) lineage {
	return lineage{alive: true, domain: domain, ptr: ptr}
}

func reviveLineage(lineage lineage, ptr *container) lineage {
	lineage.ptr = ptr
	lineage.alive = true
	return lineage
}

func endLineage(lineage lineage) lineage {
	lineage.alive = false
	lineage.ptr = nil
	return lineage
}

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

var (
	ErrInvalid = errors.New("matt:chasm::daemon: invalid")
	ErrLocked  = errors.New("matt:chasm::daemon: locked")
)

type daemonManager7 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet7[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager7(cfg Config) *daemonManager7 {
	manager := new(daemonManager7)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager7) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager7) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager7) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager7) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager7) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager7) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager7) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager7) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager7) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager7) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager7) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager7) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager7) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager7) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager7) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager7) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager7) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager7) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager7) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager7) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager7) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager7) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager8 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet8[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager8(cfg Config) *daemonManager8 {
	manager := new(daemonManager8)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager8) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager8) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager8) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager8) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager8) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager8) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager8) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager8) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager8) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager8) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager8) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager8) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager8) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager8) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager8) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager8) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager8) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager8) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager8) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager8) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager8) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager8) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager9 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet9[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager9(cfg Config) *daemonManager9 {
	manager := new(daemonManager9)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager9) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager9) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager9) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager9) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager9) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager9) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager9) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager9) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager9) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager9) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager9) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager9) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager9) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager9) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager9) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager9) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager9) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager9) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager9) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager9) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager9) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager9) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager10 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet10[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager10(cfg Config) *daemonManager10 {
	manager := new(daemonManager10)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager10) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager10) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager10) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager10) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager10) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager10) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager10) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager10) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager10) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager10) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager10) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager10) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager10) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager10) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager10) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager10) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager10) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager10) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager10) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager10) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager10) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager10) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager11 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet11[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager11(cfg Config) *daemonManager11 {
	manager := new(daemonManager11)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager11) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager11) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager11) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager11) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager11) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager11) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager11) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager11) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager11) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager11) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager11) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager11) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager11) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager11) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager11) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager11) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager11) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager11) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager11) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager11) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager11) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager11) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager12 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet12[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager12(cfg Config) *daemonManager12 {
	manager := new(daemonManager12)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager12) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager12) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager12) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager12) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager12) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager12) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager12) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager12) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager12) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager12) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager12) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager12) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager12) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager12) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager12) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager12) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager12) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager12) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager12) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager12) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager12) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager12) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager13 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet13[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager13(cfg Config) *daemonManager13 {
	manager := new(daemonManager13)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager13) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager13) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager13) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager13) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager13) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager13) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager13) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager13) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager13) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager13) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager13) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager13) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager13) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager13) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager13) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager13) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager13) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager13) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager13) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager13) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager13) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager13) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager14 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet14[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager14(cfg Config) *daemonManager14 {
	manager := new(daemonManager14)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager14) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager14) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager14) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager14) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager14) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager14) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager14) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager14) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager14) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager14) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager14) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager14) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager14) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager14) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager14) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager14) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager14) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager14) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager14) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager14) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager14) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager14) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager15 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet15[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager15(cfg Config) *daemonManager15 {
	manager := new(daemonManager15)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager15) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager15) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager15) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager15) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager15) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager15) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager15) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager15) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager15) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager15) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager15) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager15) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager15) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager15) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager15) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager15) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager15) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager15) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager15) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager15) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager15) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager15) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager16 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet16[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager16(cfg Config) *daemonManager16 {
	manager := new(daemonManager16)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager16) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager16) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager16) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager16) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager16) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager16) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager16) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager16) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager16) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager16) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager16) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager16) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager16) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager16) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager16) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager16) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager16) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager16) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager16) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager16) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager16) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager16) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager17 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet17[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager17(cfg Config) *daemonManager17 {
	manager := new(daemonManager17)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager17) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager17) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager17) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager17) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager17) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager17) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager17) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager17) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager17) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager17) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager17) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager17) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager17) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager17) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager17) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager17) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager17) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager17) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager17) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager17) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager17) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager17) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager18 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet18[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager18(cfg Config) *daemonManager18 {
	manager := new(daemonManager18)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager18) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager18) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager18) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager18) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager18) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager18) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager18) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager18) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager18) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager18) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager18) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager18) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager18) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager18) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager18) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager18) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager18) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager18) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager18) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager18) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager18) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager18) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager19 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet19[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager19(cfg Config) *daemonManager19 {
	manager := new(daemonManager19)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager19) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager19) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager19) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager19) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager19) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager19) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager19) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager19) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager19) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager19) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager19) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager19) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager19) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager19) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager19) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager19) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager19) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager19) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager19) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager19) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager19) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager19) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager20 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet20[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager20(cfg Config) *daemonManager20 {
	manager := new(daemonManager20)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager20) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager20) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager20) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager20) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager20) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager20) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager20) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager20) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager20) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager20) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager20) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager20) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager20) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager20) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager20) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager20) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager20) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager20) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager20) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager20) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager20) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager20) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager21 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet21[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager21(cfg Config) *daemonManager21 {
	manager := new(daemonManager21)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager21) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager21) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager21) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager21) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager21) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager21) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager21) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager21) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager21) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager21) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager21) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager21) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager21) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager21) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager21) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager21) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager21) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager21) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager21) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager21) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager21) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager21) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager22 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet22[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager22(cfg Config) *daemonManager22 {
	manager := new(daemonManager22)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager22) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager22) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager22) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager22) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager22) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager22) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager22) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager22) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager22) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager22) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager22) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager22) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager22) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager22) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager22) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager22) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager22) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager22) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager22) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager22) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager22) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager22) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager23 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet23[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager23(cfg Config) *daemonManager23 {
	manager := new(daemonManager23)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager23) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager23) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager23) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager23) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager23) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager23) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager23) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager23) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager23) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager23) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager23) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager23) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager23) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager23) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager23) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager23) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager23) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager23) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager23) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager23) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager23) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager23) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager24 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet24[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager24(cfg Config) *daemonManager24 {
	manager := new(daemonManager24)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager24) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager24) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager24) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager24) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager24) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager24) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager24) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager24) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager24) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager24) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager24) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager24) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager24) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager24) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager24) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager24) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager24) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager24) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager24) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager24) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager24) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager24) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager25 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet25[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager25(cfg Config) *daemonManager25 {
	manager := new(daemonManager25)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager25) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager25) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager25) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager25) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager25) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager25) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager25) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager25) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager25) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager25) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager25) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager25) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager25) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager25) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager25) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager25) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager25) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager25) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager25) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager25) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager25) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager25) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager26 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet26[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager26(cfg Config) *daemonManager26 {
	manager := new(daemonManager26)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager26) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager26) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager26) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager26) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager26) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager26) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager26) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager26) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager26) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager26) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager26) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager26) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager26) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager26) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager26) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager26) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager26) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager26) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager26) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager26) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager26) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager26) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager27 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet27[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager27(cfg Config) *daemonManager27 {
	manager := new(daemonManager27)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager27) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager27) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager27) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager27) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager27) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager27) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager27) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager27) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager27) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager27) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager27) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager27) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager27) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager27) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager27) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager27) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager27) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager27) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager27) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager27) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager27) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager27) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager28 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet28[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager28(cfg Config) *daemonManager28 {
	manager := new(daemonManager28)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager28) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager28) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager28) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager28) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager28) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager28) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager28) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager28) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager28) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager28) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager28) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager28) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager28) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager28) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager28) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager28) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager28) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager28) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager28) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager28) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager28) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager28) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager29 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet29[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager29(cfg Config) *daemonManager29 {
	manager := new(daemonManager29)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager29) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager29) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager29) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager29) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager29) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager29) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager29) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager29) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager29) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager29) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager29) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager29) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager29) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager29) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager29) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager29) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager29) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager29) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager29) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager29) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager29) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager29) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager30 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet30[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager30(cfg Config) *daemonManager30 {
	manager := new(daemonManager30)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager30) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager30) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager30) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager30) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager30) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager30) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager30) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager30) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager30) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager30) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager30) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager30) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager30) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager30) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager30) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager30) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager30) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager30) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager30) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager30) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager30) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager30) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager31 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet31[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager31(cfg Config) *daemonManager31 {
	manager := new(daemonManager31)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager31) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager31) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager31) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager31) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager31) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager31) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager31) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager31) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager31) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager31) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager31) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager31) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager31) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager31) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager31) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager31) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager31) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager31) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager31) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager31) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager31) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager31) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager32 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet32[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager32(cfg Config) *daemonManager32 {
	manager := new(daemonManager32)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager32) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager32) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager32) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager32) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager32) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager32) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager32) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager32) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager32) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager32) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager32) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager32) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager32) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager32) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager32) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager32) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager32) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager32) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager32) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager32) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager32) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager32) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager33 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet33[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager33(cfg Config) *daemonManager33 {
	manager := new(daemonManager33)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager33) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager33) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager33) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager33) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager33) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager33) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager33) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager33) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager33) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager33) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager33) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager33) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager33) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager33) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager33) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager33) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager33) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager33) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager33) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager33) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager33) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager33) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}

type daemonManager34 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   struct {
		symbol   kit.CoarseMap[uuid.UUID, SymbolFactory]
		_        [32]byte
		book     kit.CoarseMap[uuid.UUID, BookFactory]
		_        [32]byte
		candle   kit.CoarseMap[uuid.UUID, CandleFactory]
		_        [32]byte
		trade    kit.CoarseMap[uuid.UUID, TradeFactory]
		_        [32]byte
		schedule kit.CoarseMap[uuid.UUID, ScheduleFactory]
		_        [32]byte
		plugin   kit.CoarseMap[uuid.UUID, PluginFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	chasm    kit.CoarseSortedSet34[container]
	counter  atomic.Uint64
	onAdd    func(DaemonInfo)
	onKill   func(DaemonInfo)
}

func newDaemonManager34(cfg Config) *daemonManager34 {
	manager := new(daemonManager34)
	kit.InitCoarseMap(&manager.lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[plugin] = manager.newPluginDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.plugin, cfg.Factory.Plugin)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
	return manager
}

func (this *daemonManager34) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}

func (this *daemonManager34) Pause(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Pause())
			}
		}
	}

	return err
}

func (this *daemonManager34) Resume(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Resume())
			}
		}
	}

	return err
}

func (this *daemonManager34) Restart(hidden bool, filter func(DaemonInfo) bool) (err error) {
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				err = errors.Join(err, container.Restart())
			}
		}
	}

	return err
}

func (this *daemonManager34) DaemonInfo(hidden bool, hint int, filter func(DaemonInfo) bool) []DaemonInfo {
	info := make([]DaemonInfo, 0, hint)
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if filter(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *daemonManager34) Hide(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = true
	return nil
}

func (this *daemonManager34) Show(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	lineage.ptr.hidden = false
	return nil
}

func (this *daemonManager34) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *daemonManager34) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager34) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *daemonManager34) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *daemonManager34) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *daemonManager34) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *daemonManager34) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *daemonManager34) newPluginDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.plugin.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *daemonManager34) AddSymbolProducer(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	if err := daemon.Initialize(this.registry.Symbol()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager34) AddBookProducer(factory BookFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Book()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager34) AddCandleProducer(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Candle()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager34) AddTradeProducer(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Trade()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager34) AddScheduleProducer(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(this.registry.Schedule()); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager34) AddPlugin(factory PluginFactory) error {
	id := factory.ID()

	_, err := this.lineage.Get(id)
	if err == nil {
		return ErrInvalid
	}

	daemon := factory.New()
	if err := daemon.Initialize(newBrokerageDataProvider(&this.registry)); err != nil {
		return err
	}

	container := newContainer(daemon, this.counter.Add(1))
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	err = daemon.Run()
	this.onAdd(daemon)
	return err
}

func (this *daemonManager34) Kill(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	container := *lineage.ptr
	if err := container.Shutdown(); err != nil {
		return err
	}

	this.lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.onKill(container)
	return err
}
