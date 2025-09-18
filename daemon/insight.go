package daemon

import "fmt"

const (
	reportLen int  = 64
	lineEnd   byte = '\n'
)

type report [reportLen]byte

func newBaseReport(name, version string, code stateCode) (report report, length int) {
	n := copy(report[:], name)
	report[n] = lineEnd
	n++

	n += copy(report[n:], version)
	report[n] = lineEnd
	n++

	n += copy(report[n:], code.String())
	report[n] = lineEnd
	n++

	return report, n
}

func newStateReport(name, version string, code stateCode) (report report) {
	report, _ = newBaseReport(name, version, code)
	return report
}

func newFaultReport(name, version string, code errorCode, upstream error) (report report) {
	report, n := newBaseReport(name, version, Fault)

	n += copy(report[n:], code.String())
	report[n] = lineEnd
	n++

	var message = upstream.Error()
	for index := range len(message) {
		if n == reportLen-4 {
			report[reportLen-4] = '.'
			report[reportLen-3] = '.'
			report[reportLen-2] = '.'
			report[reportLen-1] = lineEnd
			return report
		}

		report[n] = message[index]
		n++
	}

	report[n] = lineEnd
	return report
}

type stateCode int8

const (
	Await stateCode = iota
	Init
	Booting
	On
	Off
	Suspended
	Fault
)

func (this stateCode) String() string {
	switch this {
	case Await:
		return _awaiting
	case Init:
		return _initialized
	case Booting:
		return _booting
	case On:
		return _on
	case Off:
		return _off
	case Suspended:
		return _suspended
	case Fault:
		return _fault
	default:
		return _default
	}
}

func (this stateCode) lLen() int { return lineLength(this) }

type errorCode int8

const (
	errUnknown errorCode = iota
	errNil
	errInvalid
	errOpen
	errNext
	errClose
	errWrite
	errFlush
	errDeadline
)

func (this errorCode) String() string { return mapError(this, false) }
func (this errorCode) Error() string  { return mapError(this, true) }
func (this errorCode) lLen() int      { return lineLength(this) }

func mapError(code errorCode, prefixed bool) string {
	switch code {
	case errUnknown:
		if prefixed {
			return _prefixedUnknown
		}
		return _unknown
	case errNil:
		if prefixed {
			return _prefixedNil
		}
		return _nil
	case errInvalid:
		if prefixed {
			return _prefixedInvalid
		}
		return _invalid
	case errOpen:
		if prefixed {
			return _prefixedOpen
		}
		return _open
	case errNext:
		if prefixed {
			return _prefixedNext
		}
		return _next
	case errClose:
		if prefixed {
			return _prefixedClose
		}
		return _close
	case errWrite:
		if prefixed {
			return _prefixedWrite
		}
		return _write
	case errFlush:
		if prefixed {
			return _prefixedFlush
		}
		return _flush
	case errDeadline:
		if prefixed {
			return _prefixedDeadline
		}
		return _deadline
	default:
		if prefixed {
			return _prefixedDefault
		}
		return _default
	}
}

func (this errorCode) Is(target error) bool {
	if target, ok := target.(errorCode); ok {
		return this == target
	}

	return false
}

const (
	_unknown     string = "unknown"
	_nil         string = "nil"
	_awaiting    string = "awaiting"
	_initialized string = "initialized"
	_booting     string = "booting"
	_on          string = "on"
	_off         string = "off"
	_suspended   string = "suspended"
	_fault       string = "fault"
	_invalid     string = "invalid"
	_open        string = "open"
	_next        string = "next"
	_close       string = "close"
	_write       string = "write"
	_flush       string = "flush"
	_deadline    string = "deadline"
	_default     string = "default"
)

const (
	_prefix           string = "matt::chasm::daemon: "
	_prefixedUnknown  string = _prefix + _unknown
	_prefixedNil      string = _prefix + _nil
	_prefixedInvalid  string = _prefix + _invalid
	_prefixedOpen     string = _prefix + _open
	_prefixedNext     string = _prefix + _next
	_prefixedClose    string = _prefix + _close
	_prefixedWrite    string = _prefix + _write
	_prefixedFlush    string = _prefix + _flush
	_prefixedDeadline string = _prefix + _deadline
	_prefixedDefault  string = _prefix + _default
)

func lineLength(stringer fmt.Stringer) int { return len(stringer.String()) + 1 }
