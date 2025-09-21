package daemon

import (
	"encoding/binary"
	"time"

	"github.com/google/uuid"
	"github.com/mattgonewild/chasm/core"
)

const (
	newLogStepWindow       time.Duration = 0
	newLogRetention        time.Duration = 0
	newLogCapPerLinkedNode int           = 0
)

/*

-------------------------------------------
field     bits value
-------------------------------------------
custom_a  48   0x5c146b143c52
ver        4   0x8
custom_b  12   0xafd
var        2   0b10
custom_c  62   0b00, 0x38a375d0df1fbf6
-------------------------------------------
total     128
-------------------------------------------
final: 5c146b14-3c52-8afd-938a-375d0df1fbf6

*/

const (
	high16Mask = (1 << 16) - 1
	keyBit     = 61
	keyMask    = (1 << keyBit) - 1
)

func spawnID(parent uuid.UUID, domain core.Domain, key core.Key) uuid.UUID {
	msb := binary.BigEndian.Uint64(parent[:8])
	msb &= ^uint64(high16Mask)
	msb |= uint64(0x8) << 12
	msb |= uint64(domain)

	lsb := uint64(key)
	lsb |= uint64(2) << 62

	id := uuid.Nil
	binary.BigEndian.PutUint64(id[:8], msb)
	binary.BigEndian.PutUint64(id[8:], lsb)
	return id
}

func spawnIdKey(id uuid.UUID) core.Key {
	lsb := binary.BigEndian.Uint64(id[8:])
	return core.Key(lsb & keyMask)
}
