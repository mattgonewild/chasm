package core

import "time"

// Key encodes a symbol and an optional interval.
//
// The symbol must consist only of [ A-Z . - ] and be at most 10 characters long.
type Key = uint64

const (
	alphaUsed string = "ABCDEFGHIJKLMNOPQRSTUVWXYZ.-"
	base      byte   = 1

	bitPerCode uint64 = 5
	codeMask   uint64 = (1 << bitPerCode) - 1

	intBit  uint64        = 11
	intMask uint64        = (1 << intBit) - 1
	intStep time.Duration = time.Minute
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

// EncodeSymbolKey encodes a symbol into a Key.
//
// The symbol must consist only of [ A-Z . - ] and be at most 10 characters long.
func EncodeSymbolKey(symbol string) Key {
	var (
		sink   uint64
		length = uint64(len(symbol))
	)

	for index := range length {
		sink |= uint64(lookup[symbol[index]]) << (index * bitPerCode)
	}

	return Key(sink)
}

// DecodeSymbolKey decodes a Key back to its symbol.
func DecodeSymbolKey(key Key) string {
	var (
		buf [10]byte
		n   int
		k   = uint64(key)
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

// EncodeCandleKey encodes a symbol and interval into a Key.
//
// The symbol must consist only of [ A-Z . - ] and be at most 10 characters long,
// while interval should be no greater than a day.
func EncodeCandleKey(symbol string, interval time.Duration) Key {
	var (
		sink   uint64
		length = uint64(len(symbol))
	)

	for index := range length {
		sink |= uint64(lookup[symbol[index]]) << (index * bitPerCode)
	}

	return Key((sink << intBit) | uint64(interval/intStep))
}

// DecodeCandleKey decodes a Key back to its symbol and interval.
func DecodeCandleKey(key Key) (string, time.Duration) {
	var (
		buf [10]byte
		n   int
		k   = uint64(key) >> intBit
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

	return string(buf[:n]), time.Duration((uint64(key) & intMask)) * intStep
}

// OkInterval validates that a duration given in nanoseconds is acceptable for use in a Key.
// It must be a positive multiple of one minute (not zero) and no greater than 2047 minutes.
func OkInterval(duration int64) bool {
	const (
		step = int64(intStep)
		max  = int64(intMask) * step
	)

	return (duration >= step) && (duration <= max) && (duration%step == 0)
}

// OkIntervalMinute validates that a duration given in minutes is acceptable for use in a Key.
// It must be greater than zero and less than 2048.
func OkIntervalMinute(minute int64) bool {
	const (
		min int64 = 1
		max int64 = int64(intMask)
	)

	return (minute >= min) && (minute <= max)
}
