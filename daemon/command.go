package daemon

import "time"

type command uint

const (
	shutdown command = iota
	pause
	resume
	restart
)

func send(ch chan<- command, cmd command) error {
	deadline := time.NewTimer(time.Second * 3)

	select {
	case ch <- cmd:
		deadline.Stop()
		return nil
	case <-deadline.C:
		deadline.Stop()
		return errDeadlineExceeded
	}
}
