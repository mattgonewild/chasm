package service

import (
	"context"
	"sync/atomic"

	"github.com/mattgonewild/chasm/core"
	"github.com/mattgonewild/chasm/proto"
)

type forgeCore20 struct {
	manager  core.Manager20
	counter  atomic.Int64
	daecount proto.DaeCount
	_        [8]byte
	selector selector
	scratch  proto.RepDaeInfo
	ptr      [8192]*proto.DaeInfo
	buf      [8192]proto.DaeInfo
	id       [8192]proto.ID
	status   [8192]proto.Status
}

func initCore20(fc *forgeCore20, cfg core.Config) {
	core.InitManager20(&fc.manager, cfg)

	for index := range fc.ptr {
		bi := &fc.buf[index]
		bi.Id = &fc.id[index]
		bi.Status = &fc.status[index]

		fc.ptr[index] = bi
	}
}

type forge20 struct {
	proto.UnimplementedForgeServer
	core *forgeCore20
}

func newForge20(cfg core.Config) *forge20 {
	forge := new(forge20)
	forge.core = new(forgeCore20)
	initCore20(forge.core, cfg)
	return forge
}

func (this forge20) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[core.SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	core := this.core
	if err := core.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	core.counter.Add(1)
	return protoNil, nil
}

func (this forge20) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[core.BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	core := this.core
	if err := core.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	core.counter.Add(1)
	return protoNil, nil
}

func (this forge20) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[core.CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	core := this.core
	if err := core.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	core.counter.Add(1)
	return protoNil, nil
}

func (this forge20) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[core.TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	core := this.core
	if err := core.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	core.counter.Add(1)
	return protoNil, nil
}

func (this forge20) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[core.ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	core := this.core
	if err := core.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	core.counter.Add(1)
	return protoNil, nil
}

func (this forge20) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[core.DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	core := this.core
	if err := core.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	core.counter.Add(1)
	return protoNil, nil
}

func (this forge20) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	core := this.core
	core.daecount.Total = core.counter.Load()
	return &core.daecount, nil
}

func (this forge20) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
	core := this.core

	if in.Id != nil {
		daemon, err := core.manager.Get(proto.MergeUUID(in.Id))
		if err != nil {
			return protoRep, err
		}

		marshalDaeInfo(daemon, &core.buf[0])
		core.scratch.Daemon = core.ptr[0:1]
		return &core.scratch, nil
	}

	core.selector.LoadFilter(in)
	got := core.manager.DaemonInfo(in.Hidden, &core.selector)

	for index := range got {
		marshalDaeInfo(got[index], &core.buf[index])
	}

	core.scratch.Daemon = core.ptr[0:len(got)]
	return &core.scratch, nil
}

func (this forge20) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	core := this.core
	if err := core.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	core.counter.Add(1)
	return protoNil, nil
}

func (this forge20) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pause)
}

func (this forge20) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resume)
}

func (this forge20) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restart)
}

func (this forge20) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, set)
}

func (this forge20) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hide)
}

func (this forge20) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, show)
}

func (this forge20) doCmd(in *proto.Filter, cmd command) error {
	core := this.core

	if in.Id != nil {
		if cmd == hide || cmd == show {
			lineage, err := core.manager.Lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.Alive() {
				return ErrInvalid
			}

			lineage.HideContainer(cmd == hide)
			return nil
		}

		daemon, err := core.manager.Get(proto.MergeUUID(in.Id))
		if err != nil {
			return err
		}

		switch cmd {
		case pause:
			return daemon.Pause()
		case resume:
			return daemon.Resume()
		case restart:
			return daemon.Restart()
		case set:
			return daemon.SetConfig(in.Payload)
		default:
			return ErrUnknown
		}
	}

	core.selector.LoadFilter(in)

	switch cmd {
	case pause:
		return core.manager.Pause(in.Hidden, &core.selector)
	case resume:
		return core.manager.Resume(in.Hidden, &core.selector)
	case restart:
		return core.manager.Restart(in.Hidden, &core.selector)
	case set:
		return core.manager.SetConfig(in.Hidden, &core.selector, in.Payload)
	case hide:
		return core.manager.Hide(&core.selector)
	case show:
		return core.manager.Show(&core.selector)
	default:
		return ErrUnknown
	}
}

func (this forge20) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	core := this.core
	if err := core.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	core.counter.Add(-1)
	return protoNil, nil
}

func (this forge20) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.core.manager.Shutdown()
}
