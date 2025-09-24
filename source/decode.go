package source

import "github.com/ringboundio/chasm/core"

type (
	SingleDecoder[T core.Event] interface {
		Decode(frame []byte) (T, bool, error)
	}

	MultiDecoder[T core.Event] interface {
		Decode(frame []byte, destination []T) (int, bool, error)
	}

	SinkDecoder[T core.Event] interface {
		Sink(key core.Key, frame []byte) (T, bool, error)
		Rebuild(key core.Key) (T, bool, error)
	}
)
