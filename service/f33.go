package service

import (
	"context"
	"sync/atomic"

	"github.com/mattgonewild/chasm/core"
	"github.com/mattgonewild/chasm/proto"
)

type forge33 struct {
	proto.UnimplementedForgeServer
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

func newForge33(cfg core.Config) *forge33 {
	forge := new(forge33)
	core.InitManager33(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge33) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[core.SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge33) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[core.BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge33) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[core.CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge33) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[core.TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge33) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[core.ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge33) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[core.DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge33) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge33) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
	if in.Id != nil {
		daemon, err := this.manager.Get(proto.MergeUUID(in.Id))
		if err != nil {
			return protoRep, err
		}

		marshalDaeInfo(daemon, &this.buf[0])
		this.scratch.Daemon = this.ptr[0:1]
		return &this.scratch, nil
	}

	this.selector.LoadFilter(in)
	got := this.manager.DaemonInfo(in.Hidden, &this.selector)

	for index := range got {
		marshalDaeInfo(got[index], &this.buf[index])
	}

	this.scratch.Daemon = this.ptr[0:len(got)]
	return &this.scratch, nil
}

func (this *forge33) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge33) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge33) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge33) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge33) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge33) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge33) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge33) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.Lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.Alive() {
				return ErrInvalid
			}

			lineage.HideContainer(cmd == hideCmd)
			return nil
		}

		daemon, err := this.manager.Get(proto.MergeUUID(in.Id))
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

	this.selector.LoadFilter(in)

	switch cmd {
	case pauseCmd:
		return this.manager.Pause(in.Hidden, &this.selector)
	case resumeCmd:
		return this.manager.Resume(in.Hidden, &this.selector)
	case restartCmd:
		return this.manager.Restart(in.Hidden, &this.selector)
	case setCmd:
		return this.manager.SetConfig(in.Hidden, &this.selector, in.Payload)
	case hideCmd:
		return this.manager.Hide(&this.selector)
	case showCmd:
		return this.manager.Show(&this.selector)
	default:
		return ErrUnknown
	}
}

func (this *forge33) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge33) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}
