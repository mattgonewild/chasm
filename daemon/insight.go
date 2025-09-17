package daemon

const reportLen = 64

type report [reportLen]byte

func newReport(name, version string, code stateCode, err errorCode) (report report) {
	const lineEnd byte = '\n'
	n := copy(report[:], name)
	report[n] = lineEnd
	n++

	n += copy(report[n:], version)
	report[n] = lineEnd
	n++

	n += copy(report[n:], code.String())
	report[n] = lineEnd
	n++

	n += copy(report[n:], err.String())
	report[n] = lineEnd

	return report
}

const (
	none    string = "none"
	unknown string = "unknown"
)

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

	awaitLen     int = len(awaiting) + 1
	initLen      int = len(initialized) + 1
	onLen        int = len(on) + 1
	offLen       int = len(off) + 1
	suspendedLen int = len(suspended) + 1
	faultLen     int = len(fault) + 1

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
	errNone errorCode = iota
	errUnknown

	errNilSource
	errNilSink
	errNilIgnore
	errNilInterval
	errBadLimit
	errBadInterval
	errOverIntervalCap

	errNoneLen int = len(none) + 1

	nilSource       string = "nil source"
	nilSink         string = "nil sink"
	nilIgnore       string = "nil ignore"
	nilInterval     string = "nil interval"
	badLimit        string = "bad limit"
	badInterval     string = "bad interval"
	overIntervalCap string = "over interval cap"

	prefix                  string = "matt::chasm::daemon: "
	prefixedNone            string = prefix + none
	prefixedUnknown         string = prefix + unknown
	prefixedNilSource       string = prefix + nilSource
	prefixedNilSink         string = prefix + nilSink
	prefixedNilIgnore       string = prefix + nilIgnore
	prefixedNilInterval     string = prefix + nilInterval
	prefixedBadLimit        string = prefix + badLimit
	prefixedBadInterval     string = prefix + badInterval
	prefixedOverIntervalCap string = prefix + overIntervalCap
)

func (this errorCode) String() string {
	switch this {
	case errNone:
		return none
	case errUnknown:
		return unknown
	case errNilSource:
		return nilSource
	case errNilSink:
		return nilSink
	case errNilIgnore:
		return nilIgnore
	case errNilInterval:
		return nilInterval
	case errBadLimit:
		return badLimit
	case errBadInterval:
		return badInterval
	case errOverIntervalCap:
		return overIntervalCap
	default:
		return unknown
	}
}

func (this errorCode) Error() string {
	switch this {
	case errNone:
		return prefixedNone
	case errUnknown:
		return prefixedUnknown
	case errNilSource:
		return prefixedNilSource
	case errNilSink:
		return prefixedNilSink
	case errNilIgnore:
		return prefixedNilIgnore
	case errNilInterval:
		return prefixedNilInterval
	case errBadLimit:
		return prefixedBadLimit
	case errBadInterval:
		return prefixedBadInterval
	case errOverIntervalCap:
		return prefixedOverIntervalCap
	default:
		return prefixedUnknown
	}
}

func (this errorCode) Is(target error) bool {
	if target, ok := target.(errorCode); ok {
		return this == target
	}

	return false
}
