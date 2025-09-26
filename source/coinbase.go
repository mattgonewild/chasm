package source

func NewCoinbaseSymbolCodec() SymbolCodec {
	return NewSymbolCodec(
		"",
		nil,
		nil,
		nil,
	)
}

func NewCoinbaseBookCodec() BookCodec {
	return NewBookCodec(
		"",
		nil,
		nil,
		nil,
	)
}

func NewCoinbaseCandleCodec() CandleCodec {
	return NewCandleCodec(
		"",
		nil,
		nil,
		nil,
		nil,
	)
}

func NewCoinbaseTradeCodec() TradeCodec {
	return NewTradeCodec(
		"",
		nil,
		nil,
		nil,
	)
}
