package core

import (
	"errors"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/mattgonewild/kit"
)

type Manager17 struct {
	Lineage   kit.CoarseMap[uuid.UUID, Lineage]
	newDaemon [domainCount]newDaemonFunc
	_         [48]byte
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
		data     kit.CoarseMap[uuid.UUID, DataFactory]
		_        [32]byte
	}
	registry brokerageDataLogRegistry
	// welcome to hell
	counter atomic.Uint64
	onAdd   func(DaemonInfo)
	onKill  func(DaemonInfo)
	alive   atomic.Int64
	chasm   kit.CoarseSortedSet17[container]
	_       [40]byte
	buf     [8192]DaemonInfo
}

func newManager17(cfg Config) *Manager17 {
	manager := new(Manager17)
	InitManager17(manager, cfg)
	return manager
}

func InitManager17(manager *Manager17, cfg Config) {
	kit.InitCoarseMap(&manager.Lineage, cfg.Lineage)
	manager.newDaemon[Symbol] = manager.newSymbolDaemon
	manager.newDaemon[Book] = manager.newBookDaemon
	manager.newDaemon[Candle] = manager.newCandleDaemon
	manager.newDaemon[Trade] = manager.newTradeDaemon
	manager.newDaemon[Schedule] = manager.newScheduleDaemon
	manager.newDaemon[Data] = manager.newDataDaemon
	kit.InitCoarseMap(&manager.factory.symbol, cfg.Factory.Symbol)
	kit.InitCoarseMap(&manager.factory.book, cfg.Factory.Book)
	kit.InitCoarseMap(&manager.factory.candle, cfg.Factory.Candle)
	kit.InitCoarseMap(&manager.factory.trade, cfg.Factory.Trade)
	kit.InitCoarseMap(&manager.factory.schedule, cfg.Factory.Schedule)
	kit.InitCoarseMap(&manager.factory.data, cfg.Factory.Data)
	kit.InitCoarseRegistry(&manager.registry.symbol, cfg.Registry.Symbol)
	kit.InitCoarseRegistry(&manager.registry.book, cfg.Registry.Book)
	kit.InitCoarseRegistry(&manager.registry.candle, cfg.Registry.Candle)
	kit.InitCoarseRegistry(&manager.registry.trade, cfg.Registry.Trade)
	kit.InitCoarseRegistry(&manager.registry.schedule, cfg.Registry.Schedule)
	manager.onAdd = cfg.OnAdd
	manager.onKill = cfg.OnKill
}

func (this *Manager17) Alive() int64 { return this.alive.Load() }

func (this *Manager17) AddSymbol(factory SymbolFactory) error {
	id := factory.ID()

	_, err := this.Lineage.Get(id)
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
	this.Lineage.Set(id, newLineage(Symbol, &container))
	err = daemon.Run()
	this.alive.Add(1)
	this.onAdd(daemon)
	return err
}

func (this *Manager17) AddBook(factory BookFactory) error {
	id := factory.ID()

	_, err := this.Lineage.Get(id)
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
	this.Lineage.Set(id, newLineage(Book, &container))
	err = daemon.Run()
	this.alive.Add(1)
	this.onAdd(daemon)
	return err
}

func (this *Manager17) AddCandle(factory CandleFactory) error {
	id := factory.ID()

	_, err := this.Lineage.Get(id)
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
	this.Lineage.Set(id, newLineage(Candle, &container))
	err = daemon.Run()
	this.alive.Add(1)
	this.onAdd(daemon)
	return err
}

func (this *Manager17) AddTrade(factory TradeFactory) error {
	id := factory.ID()

	_, err := this.Lineage.Get(id)
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
	this.Lineage.Set(id, newLineage(Trade, &container))
	err = daemon.Run()
	this.alive.Add(1)
	this.onAdd(daemon)
	return err
}

func (this *Manager17) AddSchedule(factory ScheduleFactory) error {
	id := factory.ID()

	_, err := this.Lineage.Get(id)
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
	this.Lineage.Set(id, newLineage(Schedule, &container))
	err = daemon.Run()
	this.alive.Add(1)
	this.onAdd(daemon)
	return err
}

func (this *Manager17) AddData(factory DataFactory) error {
	id := factory.ID()

	_, err := this.Lineage.Get(id)
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

	this.factory.data.Set(id, factory)
	this.Lineage.Set(id, newLineage(Data, &container))
	err = daemon.Run()
	this.alive.Add(1)
	this.onAdd(daemon)
	return err
}

func (this *Manager17) Revive(id uuid.UUID) error {
	lineage, err := this.Lineage.Get(id)
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

	this.Lineage.Set(id, reviveLineage(lineage, &container))
	err = daemon.Run()
	this.alive.Add(1)
	this.onAdd(daemon)
	return err
}

func (this *Manager17) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *Manager17) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *Manager17) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *Manager17) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *Manager17) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *Manager17) newDataDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.data.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *Manager17) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.Lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *Manager17) DaemonInfo(hidden bool, selector Selector) []DaemonInfo {
	info := this.buf[:0]
	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if selector.Select(container) {
				info = append(info, container)
			}
		}
	}

	return info
}

func (this *Manager17) Pause(hidden bool, selector Selector) (err error) {
	noOp := true

	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if selector.Select(container) {
				err = errors.Join(err, container.Pause())
				noOp = false
			}
		}
	}

	if noOp {
		return ErrNoOp
	}

	return err
}

func (this *Manager17) PauseID(id uuid.UUID) error {
	lineage, err := this.Lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	return lineage.ptr.Pause()
}

func (this *Manager17) Resume(hidden bool, selector Selector) (err error) {
	noOp := true

	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if selector.Select(container) {
				err = errors.Join(err, container.Resume())
				noOp = false
			}
		}
	}

	if noOp {
		return ErrNoOp
	}

	return err
}

func (this *Manager17) ResumeID(id uuid.UUID) error {
	lineage, err := this.Lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	return lineage.ptr.Resume()
}

func (this *Manager17) Restart(hidden bool, selector Selector) (err error) {
	noOp := true

	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if selector.Select(container) {
				err = errors.Join(err, container.Restart())
				noOp = false
			}
		}
	}

	if noOp {
		return ErrNoOp
	}

	return err
}

func (this *Manager17) SetConfig(hidden bool, selector Selector, config []byte) error {
	noOp := true

	for container := range this.chasm.All() {
		if container.hidden == hidden {
			if selector.Select(container) {
				container.SetConfig(config)
				noOp = false
			}
		}
	}

	if noOp {
		return ErrNoOp
	}

	return nil
}

func (this *Manager17) Hide(selector Selector) error {
	noOp := true

	for container := range this.chasm.All() {
		if selector.Select(container) {
			container.hidden = true
			noOp = false
		}
	}

	if noOp {
		return ErrNoOp
	}

	return nil
}

func (this *Manager17) Show(selector Selector) error {
	noOp := true

	for container := range this.chasm.All() {
		if selector.Select(container) {
			container.hidden = false
			noOp = false
		}
	}

	if noOp {
		return ErrNoOp
	}

	return nil
}

func (this *Manager17) Kill(id uuid.UUID) error {
	lineage, err := this.Lineage.Get(id)
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

	this.Lineage.Set(id, endLineage(lineage))
	err = this.chasm.Delete(container)
	this.alive.Add(-1)
	this.onKill(container)
	return err
}

func (this *Manager17) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}
