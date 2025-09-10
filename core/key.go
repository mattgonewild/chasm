package core

import "time"

type Key uint

const (
	alphaUsed  string = "ABCDEFGHIJKLMNOPQRSTUVWXYZ.-"
	base       uint8  = 1
	bitPerCode uint   = 5
	codeMask   uint   = (1 << bitPerCode) - 1
	interBit   uint   = 11
	interMask  uint   = (1 << interBit) - 1
)

var (
	lookup  [256]uint8 = nl()
	reverse [32]uint8  = nr()
)

func nl() [256]uint8 {
	var (
		alphabet = uint8(len(alphaUsed))
		lookup   [256]uint8
	)

	for index := range alphabet {
		lookup[alphaUsed[index]] = index + base
	}

	return lookup
}

func nr() [32]uint8 {
	var (
		alphabet = uint8(len(alphaUsed))
		reverse  [32]uint8
	)

	for index := range alphabet {
		reverse[index+base] = alphaUsed[index]
	}

	return reverse
}

// symbol should only contain ['A-Z', '.', '-'] and be no longer than 10 characters
func EncodeSymbolKey(symbol string) Key {
	var (
		sink   uint
		length = uint(len(symbol))
	)

	for index := range length {
		sink |= uint(lookup[symbol[index]]) << (index * bitPerCode)
	}

	return Key(sink)
}

func DecodeSymbolKey(key Key) string {
	var (
		buf [10]byte
		n   int
		k   = uint(key)
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

// symbol should only contain ['A-Z', '.', '-'] and be no longer than 10 characters
//
// interval should be no greater than one day
func EncodeCandleKey(symbol string, interval time.Duration) Key {
	var (
		sink   uint
		length = uint(len(symbol))
	)

	for index := range length {
		sink |= uint(lookup[symbol[index]]) << (index * bitPerCode)
	}

	return Key((sink << interBit) | uint(interval/time.Minute))
}

func DecodeCandleKey(key Key) (string, time.Duration) {
	var (
		buf [10]byte
		n   int
		k   = uint(key) >> interBit
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

	return string(buf[:n]), time.Duration((uint(key) & interMask)) * time.Minute
}
