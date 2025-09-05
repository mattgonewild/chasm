package chasm

import (
	"context"

	"github.com/google/uuid"
	"github.com/mattgonewild/common"
	"github.com/mattgonewild/kit"
)

type (
	Configurable = common.Configurable[[]byte]
	Identifiable = common.Identifiable[uuid.UUID]

	Daemon interface {
		Configurable
		Run(ctx context.Context) error
		Shutdown(ctx context.Context) error
		Pause() error
		Resume() error
		Restart() error
		DaemonInfo
	}

	DaemonInfo interface {
		Identifiable
		Name() string
		Config() []byte
		Status() (int, int64)
		Report() []byte
		Error() error
		FactoryInfo() FactoryInfo
	}

	Plugin[T common.UnixTimestamped] interface {
		Daemon
		Initialize(provider Provider[T]) error
	}

	Producer[T any] interface {
		Daemon
		Initialize(registry Registry[T]) error
	}

	Control[T any] interface {
		Producer[T]
		WithLinker(linker IngressDataLinker) Control[T]
		WithUnlinker(unlinker IngressDataUnlinker) Control[T]
	}

	IngressDataLinker interface {
		AddBookProducer(BookProducerFactory) error
		AddCandleProducer(CandleProducerFactory) error
		AddTradeProducer(TradeProducerFactory) error
	}

	IngressDataUnlinker interface {
		RemoveBookProducer(id uuid.UUID) error
		RemoveCandleProducer(id uuid.UUID) error
		RemoveTradeProducer(id uuid.UUID) error
	}

	DaemonFactory[T Daemon] interface {
		New() T
		FactoryInfo
	}

	FactoryInfo interface {
		Identifiable
		Version() (int, int, int)
	}

	SymbolWatchDogFactory   = DaemonFactory[Control[common.Log[SymbolEvent]]]
	BookProducerFactory     = DaemonFactory[Producer[common.Log[BookEvent]]]
	CandleProducerFactory   = DaemonFactory[Producer[common.Log[CandleEvent]]]
	TradeProducerFactory    = DaemonFactory[Producer[common.Log[TradeEvent]]]
	BrokeragePluginFactory  = DaemonFactory[Plugin[BrokerageData]]
	ScheduleProducerFactory = DaemonFactory[Producer[common.Log[ScheduleEvent]]]

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

		AddPlugin(BrokeragePluginFactory) error
		RemovePlugin(id uuid.UUID) error
		AddSymbolWatchDog(SymbolWatchDogFactory) error
		RemoveSymbolWatchDog(id uuid.UUID) error
		AddScheduleProducer(ScheduleProducerFactory) error
		RemoveScheduleProducer(id uuid.UUID) error
	}
)

type daemonContainer struct {
	Daemon
	hidden bool
	note   string
}

func (this daemonContainer) Before(that daemonContainer) bool
func (this daemonContainer) Equal(that daemonContainer) bool
func (this daemonContainer) After(that daemonContainer) bool
func (this daemonContainer) Compare(that daemonContainer) int

type DaemonManager struct {
	daemon   kit.CoarseSortedSet13[daemonContainer]
	factory  kit.CoarseMap[uuid.UUID, DaemonFactory[Daemon]]
	_        [32]byte
	registry CoarseBrokerageDataEventLogRegistry
}

func NewDaemonManager() *DaemonManager {
	return new(DaemonManager)
}

func (this *DaemonManager) Run(ctx context.Context) error
func (this *DaemonManager) Shutdown(ctx context.Context) error

func (this *DaemonManager) Pause(hidden bool, filter func(DaemonInfo) bool) error

func (this *DaemonManager) Resume(hidden bool, filter func(DaemonInfo) bool) error
func (this *DaemonManager) Restart(hidden bool, filter func(DaemonInfo) bool) error
func (this *DaemonManager) Report(hidden bool, filter func(DaemonInfo) bool) []byte { return nil }
func (this *DaemonManager) DaemonInfo(hidden bool, filter func(DaemonInfo) bool) ([]DaemonInfo, error)

func (this *DaemonManager) Hide(id uuid.UUID, note string) error
func (this *DaemonManager) Show(id uuid.UUID) error
func (this *DaemonManager) Get(id uuid.UUID) (Daemon, error)
func (this *DaemonManager) Revive(id uuid.UUID) error

func (this *DaemonManager) AddPlugin(factory BrokeragePluginFactory) error
func (this *DaemonManager) RemovePlugin(id uuid.UUID) error
func (this *DaemonManager) AddSymbolWatchDog(factory SymbolWatchDogFactory) error
func (this *DaemonManager) RemoveSymbolWatchDog(id uuid.UUID) error
func (this *DaemonManager) AddScheduleProducer(factory ScheduleProducerFactory) error
func (this *DaemonManager) RemoveScheduleProducer(id uuid.UUID) error

func (this *DaemonManager) AddBookProducer(factory BookProducerFactory) error
func (this *DaemonManager) AddCandleProducer(factory CandleProducerFactory) error
func (this *DaemonManager) AddTradeProducer(factory TradeProducerFactory) error
func (this *DaemonManager) RemoveBookProducer(id uuid.UUID) error
func (this *DaemonManager) RemoveCandleProducer(id uuid.UUID) error
func (this *DaemonManager) RemoveTradeProducer(id uuid.UUID) error
