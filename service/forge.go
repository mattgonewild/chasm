package service

import (
	"errors"
	"plugin"

	"github.com/mattgonewild/chasm/core"
	"github.com/mattgonewild/chasm/proto"
)

var (
	ErrIncorrectType = errors.New("matt::chasm::service: incorrect type")
	ErrInvalid       = errors.New("matt::chasm::service: invalid")
	ErrUnknown       = errors.New("matt::chasm::service: unknown")

	protoNil = new(proto.Nil)
	protoRep = new(proto.RepDaeInfo)
)

func NewForge7(cfg core.Config) proto.ForgeServer  { return newForge7(cfg) }
func NewForge8(cfg core.Config) proto.ForgeServer  { return newForge8(cfg) }
func NewForge9(cfg core.Config) proto.ForgeServer  { return newForge9(cfg) }
func NewForge10(cfg core.Config) proto.ForgeServer { return newForge10(cfg) }
func NewForge11(cfg core.Config) proto.ForgeServer { return newForge11(cfg) }
func NewForge12(cfg core.Config) proto.ForgeServer { return newForge12(cfg) }
func NewForge13(cfg core.Config) proto.ForgeServer { return newForge13(cfg) }
func NewForge14(cfg core.Config) proto.ForgeServer { return newForge14(cfg) }
func NewForge15(cfg core.Config) proto.ForgeServer { return newForge15(cfg) }
func NewForge16(cfg core.Config) proto.ForgeServer { return newForge16(cfg) }
func NewForge17(cfg core.Config) proto.ForgeServer { return newForge17(cfg) }
func NewForge18(cfg core.Config) proto.ForgeServer { return newForge18(cfg) }
func NewForge19(cfg core.Config) proto.ForgeServer { return newForge19(cfg) }
func NewForge20(cfg core.Config) proto.ForgeServer { return newForge20(cfg) }
func NewForge21(cfg core.Config) proto.ForgeServer { return newForge21(cfg) }
func NewForge22(cfg core.Config) proto.ForgeServer { return newForge22(cfg) }
func NewForge23(cfg core.Config) proto.ForgeServer { return newForge23(cfg) }
func NewForge24(cfg core.Config) proto.ForgeServer { return newForge24(cfg) }
func NewForge25(cfg core.Config) proto.ForgeServer { return newForge25(cfg) }
func NewForge26(cfg core.Config) proto.ForgeServer { return newForge26(cfg) }
func NewForge27(cfg core.Config) proto.ForgeServer { return newForge27(cfg) }
func NewForge28(cfg core.Config) proto.ForgeServer { return newForge28(cfg) }
func NewForge29(cfg core.Config) proto.ForgeServer { return newForge29(cfg) }
func NewForge30(cfg core.Config) proto.ForgeServer { return newForge30(cfg) }
func NewForge31(cfg core.Config) proto.ForgeServer { return newForge31(cfg) }
func NewForge32(cfg core.Config) proto.ForgeServer { return newForge32(cfg) }
func NewForge33(cfg core.Config) proto.ForgeServer { return newForge33(cfg) }
func NewForge34(cfg core.Config) proto.ForgeServer { return newForge34(cfg) }

func openFactory[T core.Daemon](symbol, path string) (core.Factory[T], error) {
	plugin, err := plugin.Open(path)
	if err != nil {
		return nil, err
	}

	building, err := plugin.Lookup(symbol)
	if err != nil {
		return nil, err
	}

	factory, ok := building.(core.Factory[T])
	if !ok {
		return nil, ErrIncorrectType
	}

	return factory, nil
}

func marshalDaeInfo(daemon core.DaemonInfo, out *proto.DaeInfo) {
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
	revive command = iota
	pause
	resume
	restart
	set
	hide
	show
	kill
)
