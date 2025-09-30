package kraken

import (
	"github.com/ringboundio/chasm/core"
	"github.com/ringboundio/chasm/source"
	"github.com/ringboundio/chasm/source/help"
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
func lossyDecodeBook(state *source.OrderBook, frame []byte) (core.BookEvent, bool, error)

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
func sinkCandleTrade(state *source.CandleStore, key core.Key, frame []byte) (core.CandleEvent, bool, error)
func tryCandle(state *source.CandleStore, key core.Key) (core.CandleEvent, bool, error)

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

func decodeBatchTrade(frame []byte, destination []core.TradeEvent) (length int, ok bool, err error) {
	if len(frame) <= 161 || frame[11] == 's' {
		return
	}

	if frame[43] != '{' || frame[len(frame)-3] != '}' {
		return length, false, help.Error(frame)
	}

	var (
		cursor = 43
		end    = len(frame) - 1
		sell   bool
	)

side:
	cursor += 26
	for cursor < end {
		switch frame[cursor] {
		case 'b':
			if frame[cursor+1] != 'u' {
				return length, false, help.Error(frame)
			}

			sell = false
			goto typ
		case 's':
			if frame[cursor+1] != 'e' {
				return length, false, help.Error(frame)
			}

			sell = true
			goto typ
		default:
			cursor++
			continue
		}
	}

typ:
	cursor += 43
	for cursor < end {
		switch frame[cursor] {
		case 'm':
			if frame[cursor+1] != 'a' {
				return length, false, help.Error(frame)
			}

			goto time
		case 'l':
			if frame[cursor+1] != 'i' {
				return length, false, help.Error(frame)
			}

			cursor += 33
			goto end
		default:
			cursor++
			continue
		}
	}

time:
	cursor += 33
	for cursor < end {
		switch frame[cursor] {
		case '2':
			tradeUnixTime, ok := help.RFC3339(frame[cursor : cursor+27])
			if !ok {
				return length, false, help.Error(frame)
			}

			if sell {
				destination[length] = core.TradeEvent(-tradeUnixTime)
				length++
				goto end
			}

			destination[length] = core.TradeEvent(tradeUnixTime)
			length++
			goto end
		default:
			cursor++
			continue
		}
	}

end:
	cursor += 29
	for cursor < end {
		switch frame[cursor] {
		case ',':
			goto side
		case ']':
			return length, length != 0, nil
		default:
			cursor++
			continue
		}
	}

	return length, false, help.Error(frame)
}
