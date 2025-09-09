package chasm

import (
	"context"
	"errors"
	"plugin"
	"sync/atomic"

	"github.com/mattgonewild/chasm/proto"
)

var (
	ErrIncorrectType = errors.New("matt::chasm::forge: incorrect type")
	ErrForgeInvalid  = errors.New("matt::chasm::forge: invalid")
	ErrUnknown       = errors.New("matt::chasm::forge: unknown")

	protoNil = new(proto.Nil)
	protoRep = new(proto.RepDaeInfo)
)

func NewForge(cfg Config) proto.ForgeServer {
	switch cfg.Chasm {
	case 7:
		return newForge7(cfg)
	case 8:
		return newForge8(cfg)
	case 9:
		return newForge9(cfg)
	case 10:
		return newForge10(cfg)
	case 11:
		return newForge11(cfg)
	case 12:
		return newForge12(cfg)
	case 13:
		return newForge13(cfg)
	case 14:
		return newForge14(cfg)
	case 15:
		return newForge15(cfg)
	case 16:
		return newForge16(cfg)
	case 17:
		return newForge17(cfg)
	case 18:
		return newForge18(cfg)
	case 19:
		return newForge19(cfg)
	case 20:
		return newForge20(cfg)
	case 21:
		return newForge21(cfg)
	case 22:
		return newForge22(cfg)
	case 23:
		return newForge23(cfg)
	case 24:
		return newForge24(cfg)
	case 25:
		return newForge25(cfg)
	case 26:
		return newForge26(cfg)
	case 27:
		return newForge27(cfg)
	case 28:
		return newForge28(cfg)
	case 29:
		return newForge29(cfg)
	case 30:
		return newForge30(cfg)
	case 31:
		return newForge31(cfg)
	case 32:
		return newForge32(cfg)
	case 33:
		return newForge33(cfg)
	case 34:
		return newForge34(cfg)
	default:
		panic(ErrConfig)
	}
}

func openFactory[T Daemon](symbol, path string) (Factory[T], error) {
	plugin, err := plugin.Open(path)
	if err != nil {
		return nil, err
	}

	building, err := plugin.Lookup(symbol)
	if err != nil {
		return nil, err
	}

	factory, ok := building.(Factory[T])
	if !ok {
		return nil, ErrIncorrectType
	}

	return factory, nil
}

func marshalDaeInfo(daemon DaemonInfo, out *proto.DaeInfo) {
	low, high := proto.SplitUUID(daemon.ID())
	code, since := daemon.Status()
	out.Id.Low = low
	out.Id.High = high
	out.Status.Code = int64(code)
	out.Status.Since = since
	out.Tag = daemon.Tag()
	out.Config = daemon.Config()
	out.Report = daemon.Report()
}

type command uint

const (
	reviveCmd command = iota
	pauseCmd
	resumeCmd
	restartCmd
	setCmd
	hideCmd
	showCmd
	killCmd
)

type forge7 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager7
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

func newForge7(cfg Config) *forge7 {
	forge := new(forge7)
	initDaemonManager7(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge7) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge7) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge7) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge7) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge7) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge7) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge7) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge7) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge7) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge7) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge7) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge7) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge7) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge7) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge7) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge7) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge7) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge7) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge8 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager8
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

func newForge8(cfg Config) *forge8 {
	forge := new(forge8)
	initDaemonManager8(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge8) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge8) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge8) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge8) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge8) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge8) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge8) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge8) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge8) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge8) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge8) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge8) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge8) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge8) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge8) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge8) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge8) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge8) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge9 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager9
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

func newForge9(cfg Config) *forge9 {
	forge := new(forge9)
	initDaemonManager9(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge9) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge9) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge9) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge9) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge9) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge9) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge9) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge9) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge9) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge9) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge9) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge9) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge9) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge9) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge9) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge9) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge9) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge9) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge10 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager10
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

func newForge10(cfg Config) *forge10 {
	forge := new(forge10)
	initDaemonManager10(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge10) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge10) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge10) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge10) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge10) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge10) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge10) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge10) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge10) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge10) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge10) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge10) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge10) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge10) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge10) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge10) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge10) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge10) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge11 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager11
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

func newForge11(cfg Config) *forge11 {
	forge := new(forge11)
	initDaemonManager11(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge11) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge11) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge11) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge11) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge11) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge11) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge11) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge11) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge11) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge11) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge11) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge11) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge11) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge11) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge11) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge11) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge11) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge11) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge12 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager12
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

func newForge12(cfg Config) *forge12 {
	forge := new(forge12)
	initDaemonManager12(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge12) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge12) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge12) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge12) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge12) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge12) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge12) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge12) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge12) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge12) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge12) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge12) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge12) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge12) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge12) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge12) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge12) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge12) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge13 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager13
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

func newForge13(cfg Config) *forge13 {
	forge := new(forge13)
	initDaemonManager13(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge13) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge13) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge13) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge13) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge13) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge13) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge13) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge13) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge13) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge13) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge13) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge13) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge13) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge13) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge13) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge13) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge13) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge13) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge14 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager14
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

func newForge14(cfg Config) *forge14 {
	forge := new(forge14)
	initDaemonManager14(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge14) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge14) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge14) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge14) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge14) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge14) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge14) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge14) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge14) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge14) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge14) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge14) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge14) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge14) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge14) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge14) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge14) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge14) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge15 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager15
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

func newForge15(cfg Config) *forge15 {
	forge := new(forge15)
	initDaemonManager15(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge15) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge15) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge15) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge15) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge15) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge15) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge15) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge15) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge15) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge15) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge15) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge15) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge15) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge15) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge15) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge15) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge15) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge15) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge16 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager16
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

func newForge16(cfg Config) *forge16 {
	forge := new(forge16)
	initDaemonManager16(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge16) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge16) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge16) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge16) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge16) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge16) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge16) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge16) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge16) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge16) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge16) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge16) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge16) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge16) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge16) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge16) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge16) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge16) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge17 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager17
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

func newForge17(cfg Config) *forge17 {
	forge := new(forge17)
	initDaemonManager17(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge17) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge17) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge17) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge17) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge17) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge17) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge17) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge17) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge17) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge17) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge17) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge17) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge17) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge17) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge17) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge17) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge17) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge17) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge18 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager18
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

func newForge18(cfg Config) *forge18 {
	forge := new(forge18)
	initDaemonManager18(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge18) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge18) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge18) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge18) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge18) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge18) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge18) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge18) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge18) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge18) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge18) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge18) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge18) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge18) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge18) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge18) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge18) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge18) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge19 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager19
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

func newForge19(cfg Config) *forge19 {
	forge := new(forge19)
	initDaemonManager19(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge19) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge19) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge19) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge19) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge19) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge19) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge19) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge19) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge19) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge19) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge19) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge19) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge19) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge19) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge19) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge19) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge19) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge19) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge20 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager20
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

func newForge20(cfg Config) *forge20 {
	forge := new(forge20)
	initDaemonManager20(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge20) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge20) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge20) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge20) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge20) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge20) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge20) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge20) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge20) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge20) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge20) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge20) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge20) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge20) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge20) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge20) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge20) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge20) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge21 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager21
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

func newForge21(cfg Config) *forge21 {
	forge := new(forge21)
	initDaemonManager21(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge21) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge21) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge21) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge21) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge21) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge21) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge21) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge21) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge21) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge21) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge21) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge21) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge21) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge21) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge21) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge21) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge21) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge21) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge22 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager22
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

func newForge22(cfg Config) *forge22 {
	forge := new(forge22)
	initDaemonManager22(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge22) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge22) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge22) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge22) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge22) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge22) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge22) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge22) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge22) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge22) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge22) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge22) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge22) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge22) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge22) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge22) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge22) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge22) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge23 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager23
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

func newForge23(cfg Config) *forge23 {
	forge := new(forge23)
	initDaemonManager23(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge23) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge23) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge23) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge23) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge23) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge23) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge23) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge23) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge23) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge23) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge23) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge23) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge23) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge23) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge23) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge23) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge23) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge23) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge24 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager24
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

func newForge24(cfg Config) *forge24 {
	forge := new(forge24)
	initDaemonManager24(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge24) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge24) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge24) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge24) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge24) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge24) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge24) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge24) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge24) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge24) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge24) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge24) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge24) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge24) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge24) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge24) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge24) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge24) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge25 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager25
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

func newForge25(cfg Config) *forge25 {
	forge := new(forge25)
	initDaemonManager25(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge25) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge25) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge25) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge25) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge25) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge25) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge25) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge25) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge25) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge25) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge25) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge25) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge25) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge25) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge25) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge25) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge25) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge25) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge26 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager26
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

func newForge26(cfg Config) *forge26 {
	forge := new(forge26)
	initDaemonManager26(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge26) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge26) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge26) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge26) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge26) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge26) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge26) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge26) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge26) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge26) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge26) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge26) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge26) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge26) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge26) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge26) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge26) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge26) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge27 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager27
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

func newForge27(cfg Config) *forge27 {
	forge := new(forge27)
	initDaemonManager27(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge27) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge27) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge27) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge27) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge27) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge27) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge27) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge27) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge27) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge27) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge27) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge27) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge27) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge27) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge27) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge27) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge27) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge27) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge28 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager28
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

func newForge28(cfg Config) *forge28 {
	forge := new(forge28)
	initDaemonManager28(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge28) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge28) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge28) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge28) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge28) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge28) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge28) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge28) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge28) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge28) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge28) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge28) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge28) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge28) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge28) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge28) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge28) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge28) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge29 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager29
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

func newForge29(cfg Config) *forge29 {
	forge := new(forge29)
	initDaemonManager29(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge29) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge29) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge29) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge29) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge29) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge29) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge29) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge29) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge29) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge29) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge29) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge29) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge29) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge29) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge29) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge29) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge29) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge29) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge30 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager30
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

func newForge30(cfg Config) *forge30 {
	forge := new(forge30)
	initDaemonManager30(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge30) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge30) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge30) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge30) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge30) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge30) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge30) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge30) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge30) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge30) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge30) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge30) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge30) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge30) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge30) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge30) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge30) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge30) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge31 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager31
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

func newForge31(cfg Config) *forge31 {
	forge := new(forge31)
	initDaemonManager31(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge31) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge31) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge31) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge31) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge31) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge31) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge31) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge31) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge31) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge31) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge31) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge31) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge31) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge31) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge31) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge31) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge31) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge31) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge32 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager32
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

func newForge32(cfg Config) *forge32 {
	forge := new(forge32)
	initDaemonManager32(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge32) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge32) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge32) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge32) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge32) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge32) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge32) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge32) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge32) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge32) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge32) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge32) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge32) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge32) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge32) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge32) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge32) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge32) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}

type forge33 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager33
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

func newForge33(cfg Config) *forge33 {
	forge := new(forge33)
	initDaemonManager33(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge33) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
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
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
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
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
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
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
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
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
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
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
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
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

type forge34 struct {
	proto.UnimplementedForgeServer
	manager  daemonManager34
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

func newForge34(cfg Config) *forge34 {
	forge := new(forge34)
	initDaemonManager34(&forge.manager, cfg)

	for index := range forge.ptr {
		bi := &forge.buf[index]
		bi.Id = &forge.id[index]
		bi.Status = &forge.status[index]

		forge.ptr[index] = bi
	}

	return forge
}

func (this *forge34) AddSymbol(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[SymbolDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSymbol(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge34) AddBook(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[BookDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddBook(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge34) AddCandle(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[CandleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddCandle(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge34) AddTrade(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[TradeDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddTrade(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge34) AddSchedule(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[ScheduleDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddSchedule(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge34) AddData(_ context.Context, in *proto.BinInfo) (*proto.Nil, error) {
	factory, err := openFactory[DataDaemon](in.Symbol, in.Path)
	if err != nil {
		return protoNil, err
	}

	if err := this.manager.AddData(factory); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge34) Alive(_ context.Context, _ *proto.Nil) (*proto.DaeCount, error) {
	this.daecount.Total = this.counter.Load()
	return &this.daecount, nil
}

func (this *forge34) DaemonInfo(_ context.Context, in *proto.Filter) (*proto.RepDaeInfo, error) {
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

func (this *forge34) Revive(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Revive(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(1)
	return protoNil, nil
}

func (this *forge34) Pause(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, pauseCmd)
}

func (this *forge34) Resume(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, resumeCmd)
}

func (this *forge34) Restart(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, restartCmd)
}

func (this *forge34) SetConfig(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, setCmd)
}

func (this *forge34) Hide(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, hideCmd)
}

func (this *forge34) Show(_ context.Context, in *proto.Filter) (*proto.Nil, error) {
	return protoNil, this.doCmd(in, showCmd)
}

func (this *forge34) doCmd(in *proto.Filter, cmd command) error {
	if in.Id != nil {
		if cmd == hideCmd || cmd == showCmd {
			lineage, err := this.manager.lineage.Get(proto.MergeUUID(in.Id))
			if err != nil {
				return err
			}

			if !lineage.alive {
				return ErrForgeInvalid
			}

			lineage.ptr.hidden = (cmd == hideCmd)
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

func (this *forge34) Kill(_ context.Context, in *proto.ID) (*proto.Nil, error) {
	if err := this.manager.Kill(proto.MergeUUID(in)); err != nil {
		return protoNil, err
	}

	this.counter.Add(-1)
	return protoNil, nil
}

func (this *forge34) Shutdown(_ context.Context, _ *proto.Nil) (*proto.Nil, error) {
	return protoNil, this.manager.Shutdown()
}
