package daemon

import (
	"encoding/binary"
	"sync"

	"github.com/google/uuid"
	"github.com/ringboundio/chasm/core"
	"github.com/ringboundio/chasm/sink"
	"github.com/ringboundio/chasm/source"
	"github.com/ringboundio/kit"
)

const (
	herdUnitName      string = "herdUnit"
	herdUnitVersion   string = "TODO"
	herdUnitCodeBit   uint   = 3
	herdUnitSinceBit  uint   = 64 - herdUnitCodeBit
	herdUnitSinceMask uint   = (1 << herdUnitSinceBit) - 1
)

type herdUnitFactory[T core.Event] struct {
	reader source.Reader[T]
	writer sink.Writer[T]
	tag    string
	id     uuid.UUID
}

func newHerdUnitFactory[T core.Event](reader source.Reader[T], writer sink.Writer[T], tag string, id uuid.UUID,
) core.Factory[core.Producer[core.EventLog[T]]] {
	return &herdUnitFactory[T]{
		reader: reader,
		writer: writer,
		tag:    tag,
		id:     id,
	}
}

func (this *herdUnitFactory[T]) New() core.Producer[core.EventLog[T]] {
	return &herdUnit[T]{
		codesince: newCodeSince(Await),
		ctrl:      make(chan pack),
		reader:    this.reader,
		writer:    this.writer,
		tag:       this.tag,
		id:        this.id,
		report:    newStateReport(herdUnitName, herdUnitVersion, Await),
	}
}

func (this *herdUnitFactory[T]) ID() uuid.UUID { return this.id }

type herdUnit[T core.Event] struct {
	codesince uint
	ctrl      chan pack
	reader    source.Reader[T]
	writer    sink.Writer[T]
	registry  core.Registry[core.EventLog[T]]
	tag       string
	id        uuid.UUID
	_         [24]byte
	mu        sync.Mutex
	report    report
}

func (u *herdUnit[T]) SetConfig(config []byte) error {
	if len(config) < 2 || config[0] != 0x0A || config[1] > 127 {
		return errInvalid
	}

	n := int(config[1])
	if n == 0 {
		if len(config) == 2 {
			u.tag = ""
			return nil
		}

		return errInvalid
	}

	if len(config) != 2+n {
		return errInvalid
	}

	u.tag = string(config[2:])
	return nil
}

func (u *herdUnit[T]) Run() error { return u.boot() }

func (u *herdUnit[T]) boot() error {
	u.mu.Lock()

	code := stateCode(u.codesince >> herdUnitSinceBit)
	if code == Init || code == Off {
		u.state(Booting)
		u.mu.Unlock()

		go u.kernel()
		return nil
	}

	u.mu.Unlock()
	return errInvalid
}

func (u *herdUnit[T]) kernel() {
	var (
		writer      = u.getSinkWriter()
		log, ourLog = u.getEventLog()
	)

	defer u.freeEventLog(&ourLog)
start:
	reader := u.getSourceReader()
	if err := reader.Open(); err != nil {
		u.fault(errOpen, err)
		return
	}

active:
	u.state(On)
	for {
		select {
		case pack := <-u.ctrl:
			switch pack.cmd {
			case shutdown:
				u.awkOkTransitionTo(pack, reader, Off)
				return
			case pause:
				u.awkOk(pack)
				goto sleep
			case resume:
				u.awkOk(pack)
				continue
			case restart:
				u.awkOkTransitionTo(pack, reader, Booting)
				goto start
			default:
				u.awkErr(pack, errUnknown)
				continue
			}
		default:
			if err := reader.Next(); err != nil {
				reader.Close()
				u.fault(errNext, err)
				return
			}

			event, ok := reader.Read()
			if !ok {
				continue
			}

			if !ourLog {
				if ourLog = u.claimEventLog(); !ourLog {
					goto sink
				}
			}
			log.Append(event)

		sink:
			if err := writer.Write(event); err != nil {
				reader.Close()
				u.fault(errWrite, err)
				return
			}

			continue
		}
	}

sleep:
	u.state(Suspended)
	pack := <-u.ctrl
	switch pack.cmd {
	case shutdown:
		u.awkOkTransitionTo(pack, reader, Off)
		return
	case pause:
		u.awkOk(pack)
		goto sleep
	case resume:
		u.awkOk(pack)
		goto active
	case restart:
		u.awkOkTransitionTo(pack, reader, Booting)
		goto start
	default:
		u.awkErr(pack, errUnknown)
		goto sleep
	}
}

func (u *herdUnit[T]) state(code stateCode) {
	u.codesince = newCodeSince(code)
	u.report = newStateReport(herdUnitName, herdUnitVersion, code)
}

func (u *herdUnit[T]) fault(code errorCode, upstream error) {
	u.codesince = newCodeSince(Fault)
	u.report = newFaultReport(herdUnitName, herdUnitVersion, code, upstream)
}

func (u *herdUnit[T]) getSourceReader() source.Reader[T] { return u.reader }
func (u *herdUnit[T]) getSinkWriter() sink.Writer[T]     { return u.writer }

func (u *herdUnit[T]) getEventLog() (core.EventLog[T], bool) {
	return u.registry.ClaimOrRegister(u.herdUnitKey(), u.newEventLog)
}

func (u *herdUnit[T]) newEventLog() core.EventLog[T] {
	return kit.NewLog[T](newLogStepWindow, newLogRetention, newLogCapPerLinkedNode)
}

func (u *herdUnit[T]) claimEventLog() bool { return u.registry.Claim(u.herdUnitKey()) }

func (u *herdUnit[T]) freeEventLog(ourLog *bool) {
	if *ourLog {
		u.registry.Release(u.herdUnitKey())
	}
}

func (u *herdUnit[T]) herdUnitKey() core.Key {
	msb := binary.BigEndian.Uint64(u.id[:8])
	lsb := binary.BigEndian.Uint64(u.id[8:])
	domain := msb & ((1 << 3) - 1)
	symInt := lsb & ((1 << 61) - 1)
	return (domain << 61) | symInt
}

func (u *herdUnit[T]) awkOk(cmd pack)             { kit.Close(cmd.awk) }
func (u *herdUnit[T]) awkErr(cmd pack, err error) { cmd.awk <- err; kit.Close(cmd.awk) }

func (u *herdUnit[T]) awkOkTransitionTo(cmd pack, reader source.Reader[T], code stateCode) {
	kit.Close(cmd.awk)
	reader.Close()
	u.state(code)
}

func (u *herdUnit[T]) Shutdown() error { return send(u.ctrl, shutdown) }
func (u *herdUnit[T]) Pause() error    { return send(u.ctrl, pause) }
func (u *herdUnit[T]) Resume() error   { return send(u.ctrl, resume) }
func (u *herdUnit[T]) Restart() error  { return send(u.ctrl, restart) }
func (u *herdUnit[T]) Tag() string     { return u.tag }

func (u *herdUnit[T]) Config() []byte {
	n := len(u.tag)
	buf := make([]byte, 2+n)
	buf[0] = 0x0A
	buf[1] = byte(n)
	copy(buf[2:], u.tag)
	return buf
}

func (u *herdUnit[T]) Status() (int, int64) {
	return int(u.codesince >> herdUnitSinceBit), int64(u.codesince & herdUnitSinceMask)
}

func (u *herdUnit[T]) Report() []byte { return u.report[:] }
func (u *herdUnit[T]) ID() uuid.UUID  { return u.id }

func (u *herdUnit[T]) Initialize(registry core.Registry[core.EventLog[T]]) error {
	u.registry = registry
	u.state(Init)
	return nil
}

func newCodeSince(code stateCode) uint {
	return (uint(code) << herdUnitSinceBit) | uint(kit.UnixNano())
}
