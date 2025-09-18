package daemon

import "time"

type command uint

const (
	shutdown command = iota
	pause
	resume
	restart
)

type pack struct {
	cmd command
	awk chan<- error
}

func send(ch chan<- pack, cmd command) error {
	var (
		awk      = make(chan error)
		pack     = pack{cmd: cmd, awk: awk}
		deadline = time.NewTimer(time.Second * 3)
	)

	select {
	case ch <- pack:
		deadline.Stop()
		return <-awk
	case <-deadline.C:
		return errDeadlineExceeded
	}
}
