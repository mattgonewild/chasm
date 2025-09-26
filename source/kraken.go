package source

func NewKrakenSymbolCodec() SymbolCodec {
	return NewSymbolCodec(
		"",
		nil,
		nil,
		nil,
	)
}

func NewKrakenBookCodec() BookCodec {
	return NewBookCodec(
		"",
		nil,
		nil,
		nil,
	)
}

func NewKrakenCandleCodec() CandleCodec {
	return NewCandleCodec(
		"",
		nil,
		nil,
		nil,
		nil,
	)
}

func NewKrakenTradeCodec() TradeCodec {
	return NewTradeCodec(
		"",
		nil,
		nil,
		nil,
	)
}
