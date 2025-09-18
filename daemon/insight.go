package daemon

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
	None stateCode = iota
	Unknown
	Await
	Init
	On
	Off
	Suspended
	Fault

	noneLen      int = len(none) + 1
	unknownLen   int = len(unknown) + 1
	awaitLen     int = len(awaiting) + 1
	initLen      int = len(initialized) + 1
	onLen        int = len(on) + 1
	offLen       int = len(off) + 1
	suspendedLen int = len(suspended) + 1
	faultLen     int = len(fault) + 1

	none        string = "none"
	unknown     string = "unknown"
	awaiting    string = "awaiting"
	initialized string = "initialized"
	on          string = "on"
	off         string = "off"
	suspended   string = "suspended"
	fault       string = "fault"
)

func (this stateCode) String() string {
	switch this {
	case None:
		return none
	case Unknown:
		return unknown
	case Await:
		return awaiting
	case Init:
		return initialized
	case On:
		return on
	case Off:
		return off
	case Suspended:
		return suspended
	case Fault:
		return fault
	default:
		return unknown
	}
}

type errorCode int8

const (
	errUnknown errorCode = iota
	errNilSource
	errNilSink
	errNilIgnore
	errNilInterval
	errBadLimit
	errBadInterval
	errOverIntervalCap
	errInvalid
	errOpen
	errNext
	errClose
	errWrite
	errFlush
	errDeadlineExceeded

	unknownError     string = "unknown error"
	nilSource        string = "nil source"
	nilSink          string = "nil sink"
	nilIgnore        string = "nil ignore"
	nilInterval      string = "nil interval"
	badLimit         string = "bad limit"
	badInterval      string = "bad interval"
	overIntervalCap  string = "over interval cap"
	invalid          string = "invalid"
	open             string = "open"
	next             string = "next"
	close            string = "close"
	write            string = "write"
	flush            string = "flush"
	deadlineExceeded string = "deadline exceeded"

	prefix                   string = "matt::chasm::daemon: "
	prefixedUnknownError     string = prefix + unknownError
	prefixedNilSource        string = prefix + nilSource
	prefixedNilSink          string = prefix + nilSink
	prefixedNilIgnore        string = prefix + nilIgnore
	prefixedNilInterval      string = prefix + nilInterval
	prefixedBadLimit         string = prefix + badLimit
	prefixedBadInterval      string = prefix + badInterval
	prefixedOverIntervalCap  string = prefix + overIntervalCap
	prefixedInvalid          string = prefix + invalid
	prefixedOpen             string = prefix + open
	prefixedNext             string = prefix + next
	prefixedClose            string = prefix + close
	prefixedWrite            string = prefix + write
	prefixedFlush            string = prefix + flush
	prefixedDeadlineExceeded string = prefix + deadlineExceeded
)

func (this errorCode) String() string { return mapError(this, false) }
func (this errorCode) Error() string  { return mapError(this, true) }

func mapError(code errorCode, prefixed bool) string {
	switch code {
	case errUnknown:
		if prefixed {
			return prefixedUnknownError
		}
		return unknownError
	case errNilSource:
		if prefixed {
			return prefixedNilSource
		}
		return nilSource
	case errNilSink:
		if prefixed {
			return prefixedNilSink
		}
		return nilSink
	case errNilIgnore:
		if prefixed {
			return prefixedNilIgnore
		}
		return nilIgnore
	case errNilInterval:
		if prefixed {
			return prefixedNilInterval
		}
		return nilInterval
	case errBadLimit:
		if prefixed {
			return prefixedBadLimit
		}
		return badLimit
	case errBadInterval:
		if prefixed {
			return prefixedBadInterval
		}
		return badInterval
	case errOverIntervalCap:
		if prefixed {
			return prefixedOverIntervalCap
		}
		return overIntervalCap
	case errInvalid:
		if prefixed {
			return prefixedInvalid
		}
		return invalid
	case errOpen:
		if prefixed {
			return prefixedOpen
		}
		return open
	case errNext:
		if prefixed {
			return prefixedNext
		}
		return next
	case errClose:
		if prefixed {
			return prefixedClose
		}
		return close
	case errWrite:
		if prefixed {
			return prefixedWrite
		}
		return write
	case errFlush:
		if prefixed {
			return prefixedFlush
		}
		return flush
	case errDeadlineExceeded:
		if prefixed {
			return prefixedDeadlineExceeded
		}
		return deadlineExceeded
	default:
		if prefixed {
			return prefixedUnknownError
		}
		return unknownError
	}
}

func (this errorCode) Is(target error) bool {
	if target, ok := target.(errorCode); ok {
		return this == target
	}

	return false
}
