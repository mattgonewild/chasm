package template

import (
	"github.com/ringboundio/chasm/core"
	"github.com/ringboundio/chasm/source"
)

func NewSymbolCodec() source.SymbolCodec {
	return source.NewSymbolCodec(
		"",
		symbolSubMsg,
		symbolUnsubMsg,
		decodeBatchSymbol,
	)
}

func symbolSubMsg(key core.Key) []byte
func symbolUnsubMsg(key core.Key) []byte
func decodeBatchSymbol(frame []byte, destination []core.SymbolEvent) (int, bool, error)

func NewBookCodec() source.BookCodec {
	return source.NewBookCodec(
		"",
		bookSubMsg,
		bookUnsubMsg,
		lossyDecodeBook,
	)
}

func bookSubMsg(key core.Key) []byte
func bookUnsubMsg(key core.Key) []byte
func lossyDecodeBook(base *source.BookBase, frame []byte) (core.BookEvent, bool, error)

func NewCandleCodec() source.CandleCodec {
	return source.NewCandleCodec(
		"",
		candleSubMsg,
		candleUnsubMsg,
		sinkCandleTrade,
		tryCandle,
	)
}

func candleSubMsg(key core.Key) []byte
func candleUnsubMsg(key core.Key) []byte
func sinkCandleTrade(base *source.CandleBase, key core.Key, frame []byte) (core.CandleEvent, bool, error)
func tryCandle(base *source.CandleBase, key core.Key) (core.CandleEvent, bool, error)

func NewTradeCodec() source.TradeCodec {
	return source.NewTradeCodec(
		"",
		tradeSubMsg,
		tradeUnsubMsg,
		decodeBatchTrade,
	)
}

func tradeSubMsg(key core.Key) []byte
func tradeUnsubMsg(key core.Key) []byte
func decodeBatchTrade(frame []byte, destination []core.TradeEvent) (int, bool, error)
