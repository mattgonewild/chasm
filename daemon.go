package chasm

import (
	"context"
	"errors"
	"sync"

	"github.com/google/uuid"
	"github.com/mattgonewild/common"
	"github.com/mattgonewild/kit"
)

type (
	Configurable = common.Configurable[[]byte]
	Identifiable = common.Identifiable[uuid.UUID]

	Daemon interface {
		Configurable
		Run(ctx context.Context, wg *sync.WaitGroup) error
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
		Version() (int, int, int)
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
		Run(ctx context.Context) error
		Shutdown(ctx context.Context) error

		Pause(hidden bool, filter func(DaemonInfo) bool) error
		Resume(hidden bool, filter func(DaemonInfo) bool) error
		Restart(hidden bool, filter func(DaemonInfo) bool) error
		Report(hidden bool, filter func(DaemonInfo) bool) []byte
		DaemonInfo(hidden bool, filter func(DaemonInfo) bool) ([]DaemonInfo, error)

		Hide(id uuid.UUID, note string) error
		Show(id uuid.UUID) error
		Get(id uuid.UUID) (Daemon, error)
		Revive(id uuid.UUID) error

		Linker
		Unlinker
	}
)

type Config struct {
	Ctx context.Context

	// Lineage tracks factories seen and entries are not deleted.
	// This is an initial bucket hint only; maps grow as needed, never shrink.
	// Per entry ≈ 40 B.
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
	return nil
}

type container struct {
	Daemon
	startUnixTime int64
	hidden        bool
}

func newContainer(daemon Daemon) container {
	return container{Daemon: daemon, startUnixTime: kit.UnixNano()}
}

func (this container) Before(that container) bool {
	return this.startUnixTime < that.startUnixTime
}

func (this container) Equal(that container) bool {
	return this.startUnixTime == that.startUnixTime
}

func (this container) After(that container) bool {
	return this.startUnixTime > that.startUnixTime
}

func (this container) Compare(that container) int {
	return kit.BoolToInt(this.startUnixTime > that.startUnixTime) - kit.BoolToInt(this.startUnixTime < that.startUnixTime)
}

type domain uint

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
	domain domain
	ptr    *container
	alive  bool
}

func newLineage(domain domain, ptr *container) lineage {
	return lineage{domain: domain, ptr: ptr, alive: true}
}

func reviveLineage(lineage lineage, ptr *container) lineage {
	lineage.ptr = ptr
	lineage.alive = true
	return lineage
}

type factoryIndex struct {
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

type newDaemonFunc func(id uuid.UUID) (Daemon, error)

var (
	ErrNotFound = errors.New("matt:chasm::daemon: not found")
	ErrLocked   = errors.New("matt:chasm::daemon: locked")
	ErrInvalid  = errors.New("matt:chasm::daemon: invalid")
)

type daemonManager7 struct {
	lineage   kit.CoarseMap[uuid.UUID, lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [16]byte
	factory   factoryIndex
	registry  brokerageDataLogRegistry
	chasm     kit.CoarseSortedSet7[container]
	ctx       context.Context
	wg        sync.WaitGroup
	cancel    context.CancelFunc
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
	manager.ctx, manager.cancel = context.WithCancel(cfg.Ctx)
	return manager
}

func (this *daemonManager7) Run(ctx context.Context) error
func (this *daemonManager7) Shutdown(ctx context.Context) error
func (this *daemonManager7) Pause(hidden bool, filter func(DaemonInfo) bool) error
func (this *daemonManager7) Resume(hidden bool, filter func(DaemonInfo) bool) error
func (this *daemonManager7) Restart(hidden bool, filter func(DaemonInfo) bool) error
func (this *daemonManager7) Report(hidden bool, filter func(DaemonInfo) bool) []byte { return nil }
func (this *daemonManager7) DaemonInfo(hidden bool, filter func(DaemonInfo) bool) ([]DaemonInfo, error)
func (this *daemonManager7) Hide(id uuid.UUID, note string) error
func (this *daemonManager7) Show(id uuid.UUID) error
func (this *daemonManager7) Get(id uuid.UUID) (Daemon, error)

func (this *daemonManager7) Revive(id uuid.UUID) error {
	lineage, err := this.lineage.Get(id)
	if err != nil {
		return ErrNotFound
	}

	if lineage.alive {
		return ErrLocked
	}

	daemon, err := this.newDaemon[lineage.domain](id)
	if err != nil {
		return err
	}

	container := newContainer(daemon)
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.lineage.Set(id, reviveLineage(lineage, &container))
	return daemon.Run(this.ctx, &this.wg)
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

	container := newContainer(daemon)
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.symbol.Set(id, factory)
	this.lineage.Set(id, newLineage(symbol, &container))
	return daemon.Run(this.ctx, &this.wg)
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

	container := newContainer(daemon)
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.book.Set(id, factory)
	this.lineage.Set(id, newLineage(book, &container))
	return daemon.Run(this.ctx, &this.wg)
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

	container := newContainer(daemon)
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.candle.Set(id, factory)
	this.lineage.Set(id, newLineage(candle, &container))
	return daemon.Run(this.ctx, &this.wg)
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

	container := newContainer(daemon)
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.trade.Set(id, factory)
	this.lineage.Set(id, newLineage(trade, &container))
	return daemon.Run(this.ctx, &this.wg)
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

	container := newContainer(daemon)
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.schedule.Set(id, factory)
	this.lineage.Set(id, newLineage(schedule, &container))
	return daemon.Run(this.ctx, &this.wg)
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

	container := newContainer(daemon)
	if err := this.chasm.Put(container); err != nil {
		return err
	}

	this.factory.plugin.Set(id, factory)
	this.lineage.Set(id, newLineage(plugin, &container))
	return daemon.Run(this.ctx, &this.wg)
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

	lineage.ptr = nil
	lineage.alive = false
	this.lineage.Set(id, lineage)
	return this.chasm.Delete(container)
}
