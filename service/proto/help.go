package proto

import (
	"github.com/google/uuid"
	"github.com/mattgonewild/kit"
)

func SplitUUID(id uuid.UUID) (uint64, uint64) { return kit.Unpack16(id) }
func MergeUUID(id *ID) uuid.UUID              { return uuid.UUID(kit.Pack16(id.Low, id.High)) }
