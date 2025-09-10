package core

type Selector interface {
	Select(DaemonInfo) bool
}
