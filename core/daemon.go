package core

type (
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
		Tag() string
		Config() []byte
		Status() (int, int64)
		Report() []byte
		FactoryInfo
	}

	Factory[T Daemon] interface {
		New() T
		FactoryInfo
	}

	FactoryInfo interface {
		Identifiable
	}

	SymbolDaemon   = Control[EventLog[SymbolEvent]]
	BookDaemon     = Producer[EventLog[BookEvent]]
	CandleDaemon   = Producer[EventLog[CandleEvent]]
	TradeDaemon    = Producer[EventLog[TradeEvent]]
	ScheduleDaemon = Producer[EventLog[ScheduleEvent]]
	DataDaemon     = Consumer[BrokerageData]

	SymbolFactory   = Factory[SymbolDaemon]
	BookFactory     = Factory[BookDaemon]
	CandleFactory   = Factory[CandleDaemon]
	TradeFactory    = Factory[TradeDaemon]
	ScheduleFactory = Factory[ScheduleDaemon]
	DataFactory     = Factory[DataDaemon]

	Control[T any] interface {
		Producer[T]
		WithLinker(linker Linker) Control[T]
	}

	Producer[T any] interface {
		Daemon
		Initialize(registry Registry[T]) error
	}

	Consumer[T UnixTimestamped] interface {
		Daemon
		Initialize(provider Provider[T]) error
	}
)
