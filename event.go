package chasm

import "github.com/mattgonewild/common"

type SymbolEvent struct {
	symbol string
	online bool
	common.UnixTimestamped
}

type BookEvent struct {
	common.UnixTimestamped
}

type CandleEvent struct {
	common.UnixTimestamped
}

type TradeEvent struct {
	common.UnixTimestamped
}

type ScheduleEvent struct {
	common.UnixTimestamped
}
