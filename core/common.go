package core

import (
	"github.com/google/uuid"
	"github.com/mattgonewild/common"
)

type (
	Identifiable                = common.Identifiable[uuid.UUID]
	Configurable                = common.Configurable[[]byte]
	UnixTimestamped             = common.UnixTimestamped
	Cursor[T UnixTimestamped]   = common.Cursor[T]
	EventLog[T UnixTimestamped] = common.Log[T]
	Registry[T any]             = common.Registry[Key, T]
)
