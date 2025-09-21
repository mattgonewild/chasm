package core

import "time"

// Key encodes an optional domain, symbol and optional interval.
type Key = uint64

const (
	DomainMask = domainMask
	SymIntBit  = symIntBit
)

const (
	domainBit  uint64 = 3
	domainMask uint64 = (1 << domainBit) - 1
	symIntBit  uint64 = 64 - domainBit
	bitPerCode uint64 = 5
	codeMask   uint64 = (1 << bitPerCode) - 1
	intBit     uint64 = 11
	intMask    uint64 = (1 << intBit) - 1

	intStep time.Duration = time.Minute
)

const (
	alphaUsed string = "ABCDEFGHIJKLMNOPQRSTUVWXYZ.-"
	base      byte   = 1
)

var (
	lookup  [256]byte = newLookup()
	reverse [32]byte  = newReverse()
)

func newLookup() [256]byte {
	var (
		alphabet = byte(len(alphaUsed))
		lookup   [256]byte
	)

	for index := range alphabet {
		lookup[alphaUsed[index]] = index + base
	}

	return lookup
}

func newReverse() [32]byte {
	var (
		alphabet = byte(len(alphaUsed))
		reverse  [32]byte
	)

	for index := range alphabet {
		reverse[index+base] = alphaUsed[index]
	}

	return reverse
}

// The symbol must consist only of [ A-Z . - ] and be at most 10 characters long.
// Domain defaults to Symbol.
func NewKey(symbol string) Key {
	var (
		sink   uint64
		length = uint64(len(symbol))
	)

	for index := range length {
		sink |= uint64(lookup[symbol[index]]) << (index * bitPerCode)
	}

	return sink << intBit
}

// NewDomainKey should only be used with Keys created by NewKey.
func NewDomainKey(domain Domain, key Key) Key { return (uint64(domain) << symIntBit) | key }

// NewCandleKey should only be used with Keys created by NewKey.
// The minute count must be greater than zero and less than 2048. Domain is set to Candle.
func NewCandleKey(key Key, minute int64) Key {
	return (uint64(Candle) << symIntBit) | (key | uint64(minute))
}

// Decode a Key's domain. Keys without a domain set will report as Symbol.
func DecodeDomain(key Key) Domain { return Domain((key >> symIntBit) & domainMask) }

// Decode a Key's symbol.
func DecodeSymbol(key Key) string {
	var (
		buf [10]byte
		n   int
		k   = key >> intBit
	)

	for index := range buf {
		code := k & codeMask
		if code == 0 {
			break
		}

		buf[index] = reverse[code]
		n++
		k >>= bitPerCode
	}

	return string(buf[:n])
}

// Decode a Key's interval.
func DecodeInterval(key Key) time.Duration { return time.Duration((key & intMask)) * intStep }

// OkInterval validates that a duration given in minutes is acceptable for use in a Key.
// The minute count must be greater than zero and less than 2048.
func OkInterval(minute int64) bool {
	const (
		min int64 = 1
		max int64 = int64(intMask)
	)

	return (minute >= min) && (minute <= max)
}
