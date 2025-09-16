package core

import (
	"errors"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/mattgonewild/kit"
)

type Manager25 struct {
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
	chasm   kit.CoarseSortedSet25[container]
	_       [48]byte
	buf     [8192]DaemonInfo
}

func newManager25(cfg Config) *Manager25 {
	manager := new(Manager25)
	InitManager25(manager, cfg)
	return manager
}

func InitManager25(manager *Manager25, cfg Config) {
	kit.InitCoarseMap(&manager.Lineage, cfg.Lineage)
	manager.newDaemon[symbol] = manager.newSymbolDaemon
	manager.newDaemon[book] = manager.newBookDaemon
	manager.newDaemon[candle] = manager.newCandleDaemon
	manager.newDaemon[trade] = manager.newTradeDaemon
	manager.newDaemon[schedule] = manager.newScheduleDaemon
	manager.newDaemon[data] = manager.newDataDaemon
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

func (this *Manager25) Alive() int64 { return this.alive.Load() }

func (this *Manager25) AddSymbol(factory SymbolFactory) error {
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
	this.Lineage.Set(id, newLineage(symbol, &container))
	err = daemon.Run()
	this.alive.Add(1)
	this.onAdd(daemon)
	return err
}

func (this *Manager25) AddBook(factory BookFactory) error {
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
	this.Lineage.Set(id, newLineage(book, &container))
	err = daemon.Run()
	this.alive.Add(1)
	this.onAdd(daemon)
	return err
}

func (this *Manager25) AddCandle(factory CandleFactory) error {
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
	this.Lineage.Set(id, newLineage(candle, &container))
	err = daemon.Run()
	this.alive.Add(1)
	this.onAdd(daemon)
	return err
}

func (this *Manager25) AddTrade(factory TradeFactory) error {
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
	this.Lineage.Set(id, newLineage(trade, &container))
	err = daemon.Run()
	this.alive.Add(1)
	this.onAdd(daemon)
	return err
}

func (this *Manager25) AddSchedule(factory ScheduleFactory) error {
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
	this.Lineage.Set(id, newLineage(schedule, &container))
	err = daemon.Run()
	this.alive.Add(1)
	this.onAdd(daemon)
	return err
}

func (this *Manager25) AddData(factory DataFactory) error {
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
	this.Lineage.Set(id, newLineage(data, &container))
	err = daemon.Run()
	this.alive.Add(1)
	this.onAdd(daemon)
	return err
}

func (this *Manager25) Revive(id uuid.UUID) error {
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

func (this *Manager25) newSymbolDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.symbol.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New().WithLinker(this).WithUnlinker(this)
	return daemon, daemon.Initialize(this.registry.Symbol())
}

func (this *Manager25) newBookDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.book.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Book())
}

func (this *Manager25) newCandleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.candle.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Candle())
}

func (this *Manager25) newTradeDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.trade.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Trade())
}

func (this *Manager25) newScheduleDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.schedule.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(this.registry.Schedule())
}

func (this *Manager25) newDataDaemon(id uuid.UUID) (Daemon, error) {
	factory, err := this.factory.data.Get(id)
	if err != nil {
		return nil, err
	}

	daemon := factory.New()
	return daemon, daemon.Initialize(newBrokerageDataProvider(&this.registry))
}

func (this *Manager25) Get(id uuid.UUID) (Daemon, error) {
	lineage, err := this.Lineage.Get(id)
	if err != nil {
		return nil, err
	}

	if !lineage.alive {
		return nil, ErrInvalid
	}

	return lineage.ptr.Daemon, nil
}

func (this *Manager25) DaemonInfo(hidden bool, selector Selector) []DaemonInfo {
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

func (this *Manager25) Pause(hidden bool, selector Selector) (err error) {
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

func (this *Manager25) PauseID(id uuid.UUID) error {
	lineage, err := this.Lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	return lineage.ptr.Pause()
}

func (this *Manager25) Resume(hidden bool, selector Selector) (err error) {
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

func (this *Manager25) ResumeID(id uuid.UUID) error {
	lineage, err := this.Lineage.Get(id)
	if err != nil {
		return err
	}

	if !lineage.alive {
		return ErrInvalid
	}

	return lineage.ptr.Resume()
}

func (this *Manager25) Restart(hidden bool, selector Selector) (err error) {
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

func (this *Manager25) SetConfig(hidden bool, selector Selector, config []byte) error {
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

func (this *Manager25) Hide(selector Selector) error {
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

func (this *Manager25) Show(selector Selector) error {
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

func (this *Manager25) Kill(id uuid.UUID) error {
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

func (this *Manager25) Shutdown() (err error) {
	for container := range this.chasm.All() {
		err = errors.Join(err, container.Shutdown())
	}

	return err
}
