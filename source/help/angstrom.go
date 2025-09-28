package help

import "github.com/ringboundio/chasm/source"

// Bid returns a Bid with angstrom precision.
// The total byte count must not exceed 21.
func Bid(raw []byte) (source.Bid, bool)

// Ask returns an Ask with angstrom precision.
// The total byte count must not exceed 21.
func Ask(raw []byte) (source.Ask, bool)

// Uint64 returns a uint64 with angstrom precision.
// The total byte count must not exceed 21.
func Uint64(raw []byte) (uint64, bool)
