package service

import (
	"context"
	"sync/atomic"

	"github.com/mattgonewild/chasm/core"
	"github.com/mattgonewild/chasm/proto"
)

type forgeCore33 struct {
	manager  core.Manager33
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

func initCore33(fc *forgeCore33, cfg core.Config) {
	core.InitManager33(&fc.manager, cfg)

	for index := range fc.ptr {
		bi := &fc.buf[index]
		bi.Id = &fc.id[index]
		bi.Status = &fc.status[index]

		fc.ptr[index] = bi
	}
}

type forge33 struct {
	proto.UnimplementedForgeServer
	core *forgeCore33
}

func newForge33(cfg core.Config) *forge33 {
	forge := new(forge33)
	forge.core = new(forgeCore33)
	initCore33(forge.core, cfg)
	return forge
}

func (this forge33) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
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

func (this forge33) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
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

func (this forge33) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
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

func (this forge33) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
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

func (this forge33) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
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

func (this forge33) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
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

func (this forge33) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	core := this.core
	core.daecount.Total = core.counter.Load()
	return &core.daecount, nil
}

func (this forge33) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this forge33) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	core := this.core
	if err := core.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	core.counter.Add(1)
	return protoNil, nil
}

func (this forge33) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this forge33) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this forge33) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this forge33) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this forge33) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this forge33) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this forge33) doCmd(in *proto.Filter, cmd command) error {
	core := this.core

	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := core.manager.Lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.Alive() {
				return ErrInvalid
			}

			lineage.HideContainer(cmd == hideCmd)
			return nil
		}

		daemon, err := core.manager.Get(proto.MergeUUID(in.Id))
		if err != nil {
			return err
		}

		switch cmd {
		case pauseCmd:
			return daemon.Pause()
		case resumeCmd:
			return daemon.Resume()
		case restartCmd:
			return daemon.Restart()
		case setCmd:
			return daemon.SetConfig(in.Payload)
		default:
			return ErrUnknown
		}
	}

	core.selector.LoadFilter(in)

	switch cmd {
	case pauseCmd:
		return core.manager.Pause(in.Hidden, &core.selector)
	case resumeCmd:
		return core.manager.Resume(in.Hidden, &core.selector)
	case restartCmd:
		return core.manager.Restart(in.Hidden, &core.selector)
	case setCmd:
		return core.manager.SetConfig(in.Hidden, &core.selector, in.Payload)
	case hideCmd:
		return core.manager.Hide(&core.selector)
	case showCmd:
		return core.manager.Show(&core.selector)
	default:
		return ErrUnknown
	}
}

func (this forge33) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	core := this.core
	if err := core.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	core.counter.Add(-1)
	return protoNil, nil
}

func (this forge33) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.core.manager.Shutdown()
}
