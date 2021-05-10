package futures

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

// Endpoints
const (
	baseWsMainUrl    = "wss://fstream.binance.com/ws"
	baseWsTestnetUrl = "wss://stream.binancefuture.com/ws"

	compWsMainUrl = "wss://fstream.binance.com/stream?streams="
)

var (
	// WebsocketTimeout is an interval for sending ping/pong messages if WebsocketKeepalive is enabled
	WebsocketTimeout = time.Second * 60
	// WebsocketKeepalive enables sending ping/pong messages to check the connection stability
	WebsocketKeepalive = false
	// UseTestnet switch all the WS streams from production to the testnet
	UseTestnet = false

	ErrInvalid = errors.New("invalid")
)

// getWsEndpoint return the base endpoint of the WS according the UseTestnet flag
func getWsEndpoint() string {
	if UseTestnet {
		return baseWsTestnetUrl
	}
	return baseWsMainUrl
}

// WsAggTradeEvent define websocket aggTrde event.
type WsAggTradeEvent struct {
	Event            string `json:"e"`
	Time             int64  `json:"E"`
	Symbol           string `json:"s"`
	AggregateTradeID int64  `json:"a"`
	Price            string `json:"p"`
	Quantity         string `json:"q"`
	FirstTradeID     int64  `json:"f"`
	LastTradeID      int64  `json:"l"`
	TradeTime        int64  `json:"T"`
	Maker            bool   `json:"m"`
}

// WsAggTradeHandler handle websocket that push trade information that is aggregated for a single taker order.
type WsAggTradeHandler func(event *WsAggTradeEvent)

// WsAggTradeServe serve websocket that push trade information that is aggregated for a single taker order.
func WsAggTradeServe(symbol string, handler WsAggTradeHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@aggTrade", getWsEndpoint(), strings.ToLower(symbol))
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsAggTradeEvent)
		err := json.Unmarshal(message, &event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsMarkPriceEvent define websocket markPriceUpdate event.
type WsMarkPriceEvent struct {
	Event           string `json:"e"`
	Time            int64  `json:"E"`
	Symbol          string `json:"s"`
	MarkPrice       string `json:"p"`
	IndexPrice      string `json:"i"`
	FundingRate     string `json:"r"`
	NextFundingTime int64  `json:"T"`
}

// WsMarkPriceHandler handle websocket that pushes price and funding rate for a single symbol.
type WsMarkPriceHandler func(event *WsMarkPriceEvent)

func wsMarkPriceServe(endpoint string, handler WsMarkPriceHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsMarkPriceEvent)
		err := json.Unmarshal(message, &event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsMarkPriceServe serve websocket that pushes price and funding rate for a single symbol.
func WsMarkPriceServe(symbol string, handler WsMarkPriceHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@markPrice", getWsEndpoint(), strings.ToLower(symbol))
	return wsMarkPriceServe(endpoint, handler, errHandler)
}

// WsMarkPriceServeWithRate serve websocket that pushes price and funding rate for a single symbol and rate.
func WsMarkPriceServeWithRate(symbol string, rate time.Duration, handler WsMarkPriceHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	var rateStr string
	switch rate {
	case 3 * time.Second:
		rateStr = ""
	case 1 * time.Second:
		rateStr = "@1s"
	default:
		return nil, nil, errors.New("Invalid rate")
	}
	endpoint := fmt.Sprintf("%s/%s@markPrice%s", getWsEndpoint(), strings.ToLower(symbol), rateStr)
	return wsMarkPriceServe(endpoint, handler, errHandler)
}

func WsMarkPriceServeWithRateMulti(symbols []string, rate time.Duration, handler WsMarkPriceHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	var rateStr string
	switch rate {
	case 3 * time.Second:
		rateStr = ""
	case 1 * time.Second:
		rateStr = "@1s"
	default:
		return nil, nil, errors.New("Invalid rate")
	}
	var ss []string
	for _, s := range symbols {
		ss = append(ss, fmt.Sprintf("%s@markPrice%s", strings.ToLower(s), rateStr))
	}

	endpoint := fmt.Sprintf("%s%s", compWsMainUrl, strings.Join(ss, "/"))
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		j, err := newJSON(message)
		if err != nil {
			errHandler(err)
			return
		}
		event := new(WsMarkPriceEvent)
		j = j.Get("data")
		event.Event = j.Get("e").MustString()
		event.Time = j.Get("E").MustInt64()
		event.Symbol = j.Get("s").MustString()
		event.MarkPrice = j.Get("p").MustString()
		event.IndexPrice = j.Get("i").MustString()
		event.FundingRate = j.Get("r").MustString()
		event.NextFundingTime = j.Get("T").MustInt64()
		handler(event)
	}
	return wsServe2(cfg, wsHandler, errHandler, make([]byte, 0, 1024*1024))
}

func WsMarkPriceServeWithRateMultiRaw(symbols []string, rate time.Duration, handler func([]byte), errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	var rateStr string
	switch rate {
	case 3 * time.Second:
		rateStr = ""
	case 1 * time.Second:
		rateStr = "@1s"
	default:
		return nil, nil, errors.New("Invalid rate")
	}
	var ss []string
	for _, s := range symbols {
		ss = append(ss, fmt.Sprintf("%s@markPrice%s", strings.ToLower(s), rateStr))
	}

	endpoint := fmt.Sprintf("%s%s", compWsMainUrl, strings.Join(ss, "/"))
	cfg := newWsConfig(endpoint)

	return wsServe2(cfg, handler, errHandler, make([]byte, 0, 1024*1024))
}

// WsAllMarkPriceEvent defines an array of websocket markPriceUpdate events.
type WsAllMarkPriceEvent []*WsMarkPriceEvent

// WsAllMarkPriceHandler handle websocket that pushes price and funding rate for all symbol.
type WsAllMarkPriceHandler func(event WsAllMarkPriceEvent)

func wsAllMarkPriceServe(endpoint string, handler WsAllMarkPriceHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		var event WsAllMarkPriceEvent
		err := json.Unmarshal(message, &event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsAllMarkPriceServe serve websocket that pushes price and funding rate for all symbol.
func WsAllMarkPriceServe(handler WsAllMarkPriceHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/!markPrice@arr", getWsEndpoint())
	return wsAllMarkPriceServe(endpoint, handler, errHandler)
}

// WsAllMarkPriceServeWithRate serve websocket that pushes price and funding rate for all symbol and rate.
func WsAllMarkPriceServeWithRate(rate time.Duration, handler WsAllMarkPriceHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	var rateStr string
	switch rate {
	case 3 * time.Second:
		rateStr = ""
	case 1 * time.Second:
		rateStr = "@1s"
	default:
		return nil, nil, errors.New("Invalid rate")
	}
	endpoint := fmt.Sprintf("%s/!markPrice@arr%s", getWsEndpoint(), rateStr)
	return wsAllMarkPriceServe(endpoint, handler, errHandler)
}

// WsKlineEvent define websocket kline event
type WsKlineEvent struct {
	Event  string  `json:"e"`
	Time   int64   `json:"E"`
	Symbol string  `json:"s"`
	Kline  WsKline `json:"k"`
}

// WsKline define websocket kline
type WsKline struct {
	StartTime            int64  `json:"t"`
	EndTime              int64  `json:"T"`
	Symbol               string `json:"s"`
	Interval             string `json:"i"`
	FirstTradeID         int64  `json:"f"`
	LastTradeID          int64  `json:"L"`
	Open                 string `json:"o"`
	Close                string `json:"c"`
	High                 string `json:"h"`
	Low                  string `json:"l"`
	Volume               string `json:"v"`
	TradeNum             int64  `json:"n"`
	IsFinal              bool   `json:"x"`
	QuoteVolume          string `json:"q"`
	ActiveBuyVolume      string `json:"V"`
	ActiveBuyQuoteVolume string `json:"Q"`
}

// WsKlineHandler handle websocket kline event
type WsKlineHandler func(event *WsKlineEvent)

// WsKlineServe serve websocket kline handler with a symbol and interval like 15m, 30s
func WsKlineServe(symbol string, interval string, handler WsKlineHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@kline_%s", getWsEndpoint(), strings.ToLower(symbol), interval)
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsKlineEvent)
		err := json.Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsMiniMarketTickerEvent define websocket mini market ticker event.
type WsMiniMarketTickerEvent struct {
	Event       string `json:"e"`
	Time        int64  `json:"E"`
	Symbol      string `json:"s"`
	ClosePrice  string `json:"c"`
	OpenPrice   string `json:"o"`
	HighPrice   string `json:"h"`
	LowPrice    string `json:"l"`
	Volume      string `json:"v"`
	QuoteVolume string `json:"q"`
}

// WsMiniMarketTickerHandler handle websocket that pushes 24hr rolling window mini-ticker statistics for a single symbol.
type WsMiniMarketTickerHandler func(event *WsMiniMarketTickerEvent)

// WsMiniMarketTickerServe serve websocket that pushes 24hr rolling window mini-ticker statistics for a single symbol.
func WsMiniMarketTickerServe(symbol string, handler WsMiniMarketTickerHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@miniTicker", getWsEndpoint(), strings.ToLower(symbol))
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsMiniMarketTickerEvent)
		err := json.Unmarshal(message, &event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsAllMiniMarketTickerEvent define an array of websocket mini market ticker events.
type WsAllMiniMarketTickerEvent []*WsMiniMarketTickerEvent

// WsAllMiniMarketTickerHandler handle websocket that pushes price and funding rate for all markets.
type WsAllMiniMarketTickerHandler func(event WsAllMiniMarketTickerEvent)

// WsAllMiniMarketTickerServe serve websocket that pushes price and funding rate for all markets.
func WsAllMiniMarketTickerServe(handler WsAllMiniMarketTickerHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/!miniTicker@arr", getWsEndpoint())
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		var event WsAllMiniMarketTickerEvent
		err := json.Unmarshal(message, &event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsMarketTickerEvent define websocket market ticker event.
type WsMarketTickerEvent struct {
	Event              string `json:"e"`
	Time               int64  `json:"E"`
	Symbol             string `json:"s"`
	PriceChange        string `json:"p"`
	PriceChangePercent string `json:"P"`
	WeightedAvgPrice   string `json:"w"`
	ClosePrice         string `json:"c"`
	CloseQty           string `json:"Q"`
	OpenPrice          string `json:"o"`
	HighPrice          string `json:"h"`
	LowPrice           string `json:"l"`
	BaseVolume         string `json:"v"`
	QuoteVolume        string `json:"q"`
	OpenTime           int64  `json:"O"`
	CloseTime          int64  `json:"C"`
	FirstID            int64  `json:"F"`
	LastID             int64  `json:"L"`
	TradeCount         int64  `json:"n"`
}

// WsMarketTickerHandler handle websocket that pushes 24hr rolling window mini-ticker statistics for a single symbol.
type WsMarketTickerHandler func(event *WsMarketTickerEvent)

// WsMarketTickerServe serve websocket that pushes 24hr rolling window mini-ticker statistics for a single symbol.
func WsMarketTickerServe(symbol string, handler WsMarketTickerHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@ticker", getWsEndpoint(), strings.ToLower(symbol))
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsMarketTickerEvent)
		err := json.Unmarshal(message, &event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsAllMarketTickerEvent define an array of websocket mini ticker events.
type WsAllMarketTickerEvent []*WsMarketTickerEvent

// WsAllMarketTickerHandler handle websocket that pushes price and funding rate for all markets.
type WsAllMarketTickerHandler func(event WsAllMarketTickerEvent)

// WsAllMarketTickerServe serve websocket that pushes price and funding rate for all markets.
func WsAllMarketTickerServe(handler WsAllMarketTickerHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/!ticker@arr", getWsEndpoint())
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		var event WsAllMarketTickerEvent
		err := json.Unmarshal(message, &event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsBookTickerEvent define websocket best book ticker event.
type WsBookTickerEvent struct {
	Event           string `json:"e"`
	UpdateID        int64  `json:"u"`
	Time            int64  `json:"E"`
	TransactionTime int64  `json:"T"`
	Symbol          string `json:"s"`
	BestBidPrice    string `json:"b"`
	BestBidQty      string `json:"B"`
	BestAskPrice    string `json:"a"`
	BestAskQty      string `json:"A"`
}

// WsBookTickerHandler handle websocket that pushes updates to the best bid or ask price or quantity in real-time for a specified symbol.
type WsBookTickerHandler func(event *WsBookTickerEvent)

// WsBookTickerServe serve websocket that pushes updates to the best bid or ask price or quantity in real-time for a specified symbol.
func WsBookTickerServe(symbol string, handler WsBookTickerHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@bookTicker", getWsEndpoint(), strings.ToLower(symbol))
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsBookTickerEvent)
		err := json.Unmarshal(message, &event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsAllBookTickerServe serve websocket that pushes updates to the best bid or ask price or quantity in real-time for all symbols.
func WsAllBookTickerServe(handler WsBookTickerHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/!bookTicker", getWsEndpoint())
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsBookTickerEvent)
		err := json.Unmarshal(message, &event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

type WsBookTickerEventMulti struct {
	Update       int64
	RecvTime     time.Time
	Symbol       string
	BestBidPrice string
	BestBidQty   string
	BestAskPrice string
	BestAskQty   string
}

// WsBookTickerHandler handle websocket that pushes updates to the best bid or ask price or quantity in real-time for a specified symbol.
type WsBookTickerHandlerMulti func(event *WsBookTickerEventMulti)

func WsBookTickerServeMultiRaw(symbols []string, handler func([]byte), errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	if len(symbols) > 200 {
		return nil, nil, errors.New("max 200 symbols")
	}

	var ss []string
	for _, s := range symbols {
		ss = append(ss, fmt.Sprintf("%s@bookTicker", strings.ToLower(s)))
	}

	endpoint := fmt.Sprintf("%s%s", compWsMainUrl, strings.Join(ss, "/"))
	cfg := newWsConfig(endpoint)
	return wsServe2(cfg, handler, errHandler, make([]byte, 0, 1024*1024))
}

func WsBookTickerServeMulti(symbols []string, handler WsBookTickerHandlerMulti, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	if len(symbols) > 200 {
		return nil, nil, errors.New("max 200 symbols")
	}

	var ss []string
	for _, s := range symbols {
		ss = append(ss, fmt.Sprintf("%s@bookTicker", strings.ToLower(s)))
	}

	endpoint := fmt.Sprintf("%s%s", compWsMainUrl, strings.Join(ss, "/"))
	cfg := newWsConfig(endpoint)
	wsHandler := func(b []byte) {
		event := &WsBookTickerEventMulti{
			RecvTime: time.Now(),
		}

		var found bool
		var from, to int
		for i := 0; i < len(b)-2; i++ {
			if b[i] == '"' {
				if b[i+1] == 's' && b[i+2] == '"' {
					//symbol
					from, to, i, found = findString(b, i+3)
					if found {
						event.Symbol = toString(b[from:to])
					} else {
						errHandler(ErrInvalid)
						return
					}
				} else if b[i+1] == 'b' && b[i+2] == '"' {
					//symbol
					from, to, i, found = findString(b, i+3)
					if found {
						event.BestBidPrice = toString(b[from:to])
					} else {
						errHandler(ErrInvalid)
						return
					}
				} else if b[i+1] == 'B' && b[i+2] == '"' {
					//symbol
					from, to, i, found = findString(b, i+3)
					if found {
						event.BestBidQty = toString(b[from:to])
					} else {
						errHandler(ErrInvalid)
						return
					}
				} else if b[i+1] == 'a' && b[i+2] == '"' {
					//symbol
					from, to, i, found = findString(b, i+3)
					if found {
						event.BestAskPrice = toString(b[from:to])
					} else {
						errHandler(ErrInvalid)
						return
					}
				} else if b[i+1] == 'A' && b[i+2] == '"' {
					//symbol
					from, to, i, found = findString(b, i+3)
					if found {
						event.BestAskQty = toString(b[from:to])
					} else {
						errHandler(ErrInvalid)
						return
					}
				}
			}
		}

		handler(event)
	}
	return wsServe2(cfg, wsHandler, errHandler, make([]byte, 0, 1024*1024))
}

func WsParseBookTickerSymbol(b []byte, outEvent *WsBookTickerEventMulti) bool {

	var found bool
	var from, to int
	for i := 0; i < len(b)-2; i++ {
		if b[i] == '"' {
			if b[i+1] == 'u' && b[i+2] == '"' {
				//update
				from, to, i, found = findInt(b, i+3)
				if found {
					us := toString(b[from:to])
					u, _ := strconv.Atoi(us)
					outEvent.Update = int64(u)
				} else {
					return false
				}
			} else if b[i+1] == 's' && b[i+2] == '"' {
				//symbol
				from, to, i, found = findString(b, i+3)
				if found {
					outEvent.Symbol = toString(b[from:to])
				} else {
					return false
				}
			} else if b[i+1] == 'b' && b[i+2] == '"' {
				//bid
				from, to, i, found = findString(b, i+3)
				if found {
					outEvent.BestBidPrice = toString(b[from:to])
				} else {
					return false
				}
			} else if b[i+1] == 'B' && b[i+2] == '"' {
				//bid qty
				from, to, i, found = findString(b, i+3)
				if found {
					outEvent.BestBidQty = toString(b[from:to])
				} else {
					return false
				}
			} else if b[i+1] == 'a' && b[i+2] == '"' {
				//ask
				from, to, i, found = findString(b, i+3)
				if found {
					outEvent.BestAskPrice = toString(b[from:to])
				} else {
					return false
				}
			} else if b[i+1] == 'A' && b[i+2] == '"' {
				//ask qty
				from, to, i, found = findString(b, i+3)
				if found {
					outEvent.BestAskQty = toString(b[from:to])
				} else {
					return false
				}
			}
		}
	}

	return true
}

func toString(s []byte) string {
	return *(*string)(unsafe.Pointer(&s))
}

func findString(b []byte, off int) (fromIndex, toIndex int, offsetOut int, found bool) {

	for j := off; j < len(b); j++ {
		if b[j] == ' ' || b[j] == ':' || b[j] == '"' {
			continue
		}
		fromIndex = j
		break
	}
	if fromIndex != -1 {
		k := 0
		for j := fromIndex; j < len(b); j++ {
			if b[j] == '"' {
				toIndex = j
				break
			}
			k++
		}
		if toIndex != -1 {
			offsetOut = toIndex + 1
			found = true
			return
		}
	}
	return
}

func findInt(b []byte, off int) (fromIndex, toIndex int, offsetOut int, found bool) {

	for j := off; j < len(b); j++ {
		if b[j] < '0' || b[j] > '9' {
			continue
		}
		fromIndex = j
		break
	}
	if fromIndex != -1 {
		toIndex = -1
		for j := fromIndex + 1; j < len(b); j++ {
			if b[j] >= '0' && b[j] <= '9' {
				toIndex = j
			} else {
				break
			}
		}
		if toIndex != -1 {
			offsetOut = toIndex + 1
			found = true
			return
		}
	}
	return
}

// WsLiquidationOrderEvent define websocket liquidation order event.
type WsLiquidationOrderEvent struct {
	Event            string             `json:"e"`
	Time             int64              `json:"E"`
	LiquidationOrder WsLiquidationOrder `json:"o"`
}

// WsLiquidationOrder define websocket liquidation order.
type WsLiquidationOrder struct {
	Symbol               string          `json:"s"`
	Side                 SideType        `json:"S"`
	OrderType            OrderType       `json:"o"`
	TimeInForce          TimeInForceType `json:"f"`
	OrigQuantity         string          `json:"q"`
	Price                string          `json:"p"`
	AvgPrice             string          `json:"ap"`
	OrderStatus          OrderStatusType `json:"X"`
	LastFilledQty        string          `json:"l"`
	AccumulatedFilledQty string          `json:"z"`
	TradeTime            int64           `json:"T"`
}

// WsLiquidationOrderHandler handle websocket that pushes force liquidation order information for specific symbol.
type WsLiquidationOrderHandler func(event *WsLiquidationOrderEvent)

// WsLiquidationOrderServe serve websocket that pushes force liquidation order information for specific symbol.
func WsLiquidationOrderServe(symbol string, handler WsLiquidationOrderHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@forceOrder", getWsEndpoint(), strings.ToLower(symbol))
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsLiquidationOrderEvent)
		err := json.Unmarshal(message, &event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsAllLiquidationOrderServe serve websocket that pushes force liquidation order information for all symbols.
func WsAllLiquidationOrderServe(handler WsLiquidationOrderHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/!forceOrder@arr", getWsEndpoint())
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsLiquidationOrderEvent)
		err := json.Unmarshal(message, &event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsDepthEvent define websocket depth book event
type WsDepthEvent struct {
	Event            string `json:"e"`
	Time             int64  `json:"E"`
	TransactionTime  int64  `json:"T"`
	Symbol           string `json:"s"`
	FirstUpdateID    int64  `json:"U"`
	LastUpdateID     int64  `json:"u"`
	PrevLastUpdateID int64  `json:"pu"`
	Bids             []Bid  `json:"b"`
	Asks             []Ask  `json:"a"`
}

// WsDepthHandler handle websocket depth event
type WsDepthHandler func(event *WsDepthEvent)

func wsPartialDepthServe(symbol string, levels int, rate *time.Duration, handler WsDepthHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	if levels != 5 && levels != 10 && levels != 20 {
		return nil, nil, errors.New("Invalid levels")
	}
	levelsStr := fmt.Sprintf("%d", levels)
	return wsDepthServe(symbol, levelsStr, rate, handler, errHandler)
}

// WsPartialDepthServe serve websocket partial depth handler.
func WsPartialDepthServe(symbol string, levels int, handler WsDepthHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	return wsPartialDepthServe(symbol, levels, nil, handler, errHandler)
}

// WsPartialDepthServeWithRate serve websocket partial depth handler with rate.
func WsPartialDepthServeWithRate(symbol string, levels int, rate time.Duration, handler WsDepthHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	return wsPartialDepthServe(symbol, levels, &rate, handler, errHandler)
}

func WsPartialDepthServeWithRateMulti(symbols []string, levels int, rate time.Duration, handler WsDepthHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	if levels != 5 && levels != 10 && levels != 20 {
		return nil, nil, errors.New("Invalid levels")
	}
	levelsStr := fmt.Sprintf("%d", levels)

	var rateStr string
	switch rate {
	case 250 * time.Millisecond:
		rateStr = ""
	case 500 * time.Millisecond:
		rateStr = "@500ms"
	case 100 * time.Millisecond:
		rateStr = "@100ms"
	default:
		return nil, nil, errors.New("Invalid rate")
	}

	if len(symbols) > 200 {
		return nil, nil, errors.New("max 200 symbols")
	}

	var ss []string
	for _, s := range symbols {
		ss = append(ss, fmt.Sprintf("%s@depth%s%s", strings.ToLower(s), levelsStr, rateStr))
	}

	endpoint := fmt.Sprintf("%s%s", compWsMainUrl, strings.Join(ss, "/"))
	cfg := newWsConfig(endpoint)
	event := &WsDepthEvent{
		Asks: make([]Ask, levels),
		Bids: make([]Bid, levels),
	}
	valueOut := make([]byte, 10000)

	wsHandler := func(b []byte) {

		valueOutLen := 0

		now := time.Now()
		found := false
		for i := 0; i < len(b)-2; i++ {
			if b[i] == '"' {
				if b[i+1] == 's' && b[i+2] == '"' {
					//symbol
					valueOutLen, i, found = findStringValue(b, i+3, valueOut)
					if !found {
						errHandler(ErrInvalid)
						return
					}
					event.Symbol = string(valueOut[:valueOutLen])
				} else if b[i+1] == 'a' && b[i+2] == '"' {
					//asks
					valueOutLen, i, found = findDepthAsk(b, i+3, event.Asks, valueOut)
					if !found {
						errHandler(ErrInvalid)
						return
					}
				} else if b[i+1] == 'b' && b[i+2] == '"' {
					//bids
					valueOutLen, i, found = findDepthBid(b, i+3, event.Bids, valueOut)
					if !found {
						errHandler(ErrInvalid)
						return
					}
				}
			}
		}

		dt := time.Now().Sub(now).Microseconds()
		if dt > 5 {
			fmt.Println(dt, event.Symbol)
		}

		handler(event)

		//j, err := newJSON(message)
		//if err != nil {
		//	errHandler(err)
		//	return
		//}
		//event := new(WsDepthEvent)
		//j = j.Get("data")
		//event.Event = j.Get("e").MustString()
		//event.Time = j.Get("E").MustInt64()
		//event.TransactionTime = j.Get("T").MustInt64()
		//event.Symbol = j.Get("s").MustString()
		//event.FirstUpdateID = j.Get("U").MustInt64()
		//event.LastUpdateID = j.Get("u").MustInt64()
		//event.PrevLastUpdateID = j.Get("pu").MustInt64()
		//bidsLen := len(j.Get("b").MustArray())
		//event.Bids = make([]Bid, bidsLen)
		//for i := 0; i < bidsLen; i++ {
		//	item := j.Get("b").GetIndex(i)
		//	event.Bids[i] = Bid{
		//		Price:    item.GetIndex(0).MustString(),
		//		Quantity: item.GetIndex(1).MustString(),
		//	}
		//}
		//asksLen := len(j.Get("a").MustArray())
		//event.Asks = make([]Ask, asksLen)
		//for i := 0; i < asksLen; i++ {
		//	item := j.Get("a").GetIndex(i)
		//	event.Asks[i] = Ask{
		//		Price:    item.GetIndex(0).MustString(),
		//		Quantity: item.GetIndex(1).MustString(),
		//	}
		//}
		//
		//handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

func findStringValue(b []byte, off int, valueOut []byte) (valueOutLen int, offsetOut int, found bool) {
	from := -1
	to := -1
	for j := off; j < len(b); j++ {
		if b[j] == ' ' || b[j] == ':' || b[j] == '"' {
			continue
		}
		from = j
		break
	}
	if from != -1 {
		k := 0
		for j := from; j < len(b); j++ {
			if b[j] == '"' {
				to = j
				break
			}
			valueOut[valueOutLen] = b[j]
			valueOutLen++
			k++
		}
		if to != -1 {
			offsetOut = to + 1
			found = true
			return
		}
	}
	return
}

func findDepthAsk(b []byte, off int, depthOut []Ask, valueOut []byte) (valueOutLen int, offsetOut int, found bool) {
	from := -1
	for j := off; j < len(b); j++ {
		if b[j] == '[' {
			from = j
			break
		}
	}
	if from != -1 {
		left := -1
		foundX := 0
		for x := 0; x < len(depthOut); x++ {
			for j := from + 1; j < len(b); j++ {
				if b[j] == '[' {
					left = j
					break
				}
			}
			if left != -1 {
				valueOutLen, left, found := findStringValue(b, left+1, valueOut)
				if found {
					depthOut[x].Price = string(valueOut[:valueOutLen])
					valueOutLen, left, found := findStringValue(b, left+1, valueOut)
					if found {
						depthOut[x].Quantity = string(valueOut[:valueOutLen])
						foundX++
						from = left
					}
				}
			}
		}
		if foundX == len(depthOut) {
			offsetOut = left + 1
			found = true
			//fmt.Println(depthOut)
		}
	}
	return
}

func findDepthBid(b []byte, off int, depthOut []Bid, valueOut []byte) (valueOutLen int, offsetOut int, found bool) {
	from := -1
	for j := off; j < len(b); j++ {
		if b[j] == '[' {
			from = j
			break
		}
	}
	if from != -1 {
		left := -1
		foundX := 0
		for x := 0; x < len(depthOut); x++ {
			for j := from + 1; j < len(b); j++ {
				if b[j] == '[' {
					left = j
					break
				}
			}
			if left != -1 {
				valueOutLen, left, found := findStringValue(b, left+1, valueOut)
				if found {
					depthOut[x].Price = string(valueOut[:valueOutLen])
					valueOutLen, left, found := findStringValue(b, left+1, valueOut)
					if found {
						depthOut[x].Quantity = string(valueOut[:valueOutLen])
						foundX++
						from = left
					}
				}
			}
		}
		if foundX == len(depthOut) {
			offsetOut = left + 1
			found = true
			//fmt.Println(depthOut)
		}
	}
	return
}

// WsDiffDepthServe serve websocket diff. depth handler.
func WsDiffDepthServe(symbol string, handler WsDepthHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	return wsDepthServe(symbol, "", nil, handler, errHandler)
}

// WsDiffDepthServeWithRate serve websocket diff. depth handler with rate.
func WsDiffDepthServeWithRate(symbol string, rate time.Duration, handler WsDepthHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	return wsDepthServe(symbol, "", &rate, handler, errHandler)
}

func wsDepthServe(symbol string, levels string, rate *time.Duration, handler WsDepthHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	var rateStr string
	if rate != nil {
		switch *rate {
		case 250 * time.Millisecond:
			rateStr = ""
		case 500 * time.Millisecond:
			rateStr = "@500ms"
		case 100 * time.Millisecond:
			rateStr = "@100ms"
		default:
			return nil, nil, errors.New("Invalid rate")
		}
	}
	endpoint := fmt.Sprintf("%s/%s@depth%s%s", getWsEndpoint(), strings.ToLower(symbol), levels, rateStr)
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		j, err := newJSON(message)
		if err != nil {
			errHandler(err)
			return
		}
		event := new(WsDepthEvent)
		event.Event = j.Get("e").MustString()
		event.Time = j.Get("E").MustInt64()
		event.TransactionTime = j.Get("T").MustInt64()
		event.Symbol = j.Get("s").MustString()
		event.FirstUpdateID = j.Get("U").MustInt64()
		event.LastUpdateID = j.Get("u").MustInt64()
		event.PrevLastUpdateID = j.Get("pu").MustInt64()
		bidsLen := len(j.Get("b").MustArray())
		event.Bids = make([]Bid, bidsLen)
		for i := 0; i < bidsLen; i++ {
			item := j.Get("b").GetIndex(i)
			event.Bids[i] = Bid{
				Price:    item.GetIndex(0).MustString(),
				Quantity: item.GetIndex(1).MustString(),
			}
		}
		asksLen := len(j.Get("a").MustArray())
		event.Asks = make([]Ask, asksLen)
		for i := 0; i < asksLen; i++ {
			item := j.Get("a").GetIndex(i)
			event.Asks[i] = Ask{
				Price:    item.GetIndex(0).MustString(),
				Quantity: item.GetIndex(1).MustString(),
			}
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsBLVTInfoEvent define websocket BLVT info event
type WsBLVTInfoEvent struct {
	Event          string         `json:"e"`
	Time           int64          `json:"E"`
	Symbol         string         `json:"s"`
	Issued         float64        `json:"m"`
	Baskets        []WsBLVTBasket `json:"b"`
	Nav            float64        `json:"n"`
	Leverage       float64        `json:"l"`
	TargetLeverage int64          `json:"t"`
	FundingRate    float64        `json:"f"`
}

// WsBLVTBasket define websocket BLVT basket
type WsBLVTBasket struct {
	Symbol   string `json:"s"`
	Position int64  `json:"n"`
}

// WsBLVTInfoHandler handle websocket BLVT event
type WsBLVTInfoHandler func(event *WsBLVTInfoEvent)

// WsBLVTInfoServe serve BLVT info stream
func WsBLVTInfoServe(name string, handler WsBLVTInfoHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@tokenNav", getWsEndpoint(), strings.ToUpper(name))
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsBLVTInfoEvent)
		err := json.Unmarshal(message, &event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsBLVTKlineEvent define BLVT kline event
type WsBLVTKlineEvent struct {
	Event  string      `json:"e"`
	Time   int64       `json:"E"`
	Symbol string      `json:"s"`
	Kline  WsBLVTKline `json:"k"`
}

// WsBLVTKline BLVT kline
type WsBLVTKline struct {
	StartTime       int64  `json:"t"`
	CloseTime       int64  `json:"T"`
	Symbol          string `json:"s"`
	Interval        string `json:"i"`
	FirstUpdateTime int64  `json:"f"`
	LastUpdateTime  int64  `json:"L"`
	OpenPrice       string `json:"o"`
	ClosePrice      string `json:"c"`
	HighPrice       string `json:"h"`
	LowPrice        string `json:"l"`
	Leverage        string `json:"v"`
	Count           int64  `json:"n"`
}

// WsBLVTKlineHandler BLVT kline handler
type WsBLVTKlineHandler func(event *WsBLVTKlineEvent)

// WsBLVTKlineServe serve BLVT kline stream
func WsBLVTKlineServe(name string, interval string, handler WsBLVTKlineHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@nav_Kline_%s", getWsEndpoint(), strings.ToUpper(name), interval)
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsBLVTKlineEvent)
		err := json.Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsCompositeIndexEvent websocket composite index event
type WsCompositeIndexEvent struct {
	Event       string          `json:"e"`
	Time        int64           `json:"E"`
	Symbol      string          `json:"s"`
	Price       string          `json:"p"`
	Composition []WsComposition `json:"c"`
}

// WsComposition websocket composite index event composition
type WsComposition struct {
	BaseAsset    string `json:"b"`
	WeightQty    string `json:"w"`
	WeighPercent string `json:"W"`
}

// WsCompositeIndexHandler websocket composite index handler
type WsCompositeIndexHandler func(event *WsCompositeIndexEvent)

// WsCompositiveIndexServe serve composite index information for index symbols
func WsCompositiveIndexServe(symbol string, handler WsCompositeIndexHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s@compositeIndex", getWsEndpoint(), strings.ToLower(symbol))
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsCompositeIndexEvent)
		err := json.Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}

// WsUserDataEvent define user data event
type WsUserDataEvent struct {
	Event               UserDataEventType     `json:"e"`
	Time                int64                 `json:"E"`
	CrossWalletBalance  string                `json:"cw"`
	MarginCallPositions []WsPosition          `json:"p"`
	TransactionTime     int64                 `json:"T"`
	AccountUpdate       WsAccountUpdate       `json:"a"`
	OrderTradeUpdate    WsOrderTradeUpdate    `json:"o"`
	AccountConfigUpdate WsAccountConfigUpdate `json:"ac"`
}

// WsAccountUpdate define account update
type WsAccountUpdate struct {
	Reason    UserDataEventReasonType `json:"m"`
	Balances  []WsBalance             `json:"B"`
	Positions []WsPosition            `json:"P"`
}

// WsBalance define balance
type WsBalance struct {
	Asset              string `json:"a"`
	Balance            string `json:"wb"`
	CrossWalletBalance string `json:"cw"`
	BalanceChange      string `json:"bc"`
}

// WsPosition define position
type WsPosition struct {
	Symbol                    string           `json:"s"`
	Side                      PositionSideType `json:"ps"`
	Amount                    string           `json:"pa"`
	MarginType                MarginType       `json:"mt"`
	IsolatedWallet            string           `json:"iw"`
	EntryPrice                string           `json:"ep"`
	MarkPrice                 string           `json:"mp"`
	UnrealizedPnL             string           `json:"up"`
	AccumulatedRealized       string           `json:"cr"`
	MaintenanceMarginRequired string           `json:"mm"`
}

// WsOrderTradeUpdate define order trade update
type WsOrderTradeUpdate struct {
	Symbol               string             `json:"s"`
	ClientOrderID        string             `json:"c"`
	Side                 SideType           `json:"S"`
	Type                 OrderType          `json:"o"`
	TimeInForce          TimeInForceType    `json:"f"`
	OriginalQty          string             `json:"q"`
	OriginalPrice        string             `json:"p"`
	AveragePrice         string             `json:"ap"`
	StopPrice            string             `json:"sp"`
	ExecutionType        OrderExecutionType `json:"x"`
	Status               OrderStatusType    `json:"X"`
	ID                   int64              `json:"i"`
	LastFilledQty        string             `json:"l"`
	AccumulatedFilledQty string             `json:"z"`
	LastFilledPrice      string             `json:"L"`
	CommissionAsset      string             `json:"N"`
	Commission           string             `json:"n"`
	TradeTime            int64              `json:"T"`
	TradeID              int64              `json:"t"`
	BidsNotional         string             `json:"b"`
	AsksNotional         string             `json:"a"`
	IsMaker              bool               `json:"m"`
	IsReduceOnly         bool               `json:"R"`
	WorkingType          WorkingType        `json:"wt"`
	OriginalType         OrderType          `json:"ot"`
	PositionSide         PositionSideType   `json:"ps"`
	IsClosingPosition    bool               `json:"cp"`
	ActivationPrice      string             `json:"AP"`
	CallbackRate         string             `json:"cr"`
	RealizedPnL          string             `json:"rp"`
}

// WsAccountConfigUpdate define account config update
type WsAccountConfigUpdate struct {
	Symbol   string `json:"s"`
	Leverage int64  `json:"l"`
}

// WsUserDataHandler handle WsUserDataEvent
type WsUserDataHandler func(event *WsUserDataEvent)

// WsUserDataServe serve user data handler with listen key
func WsUserDataServe(listenKey string, handler WsUserDataHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	endpoint := fmt.Sprintf("%s/%s", getWsEndpoint(), listenKey)
	cfg := newWsConfig(endpoint)
	wsHandler := func(message []byte) {
		event := new(WsUserDataEvent)
		err := json.Unmarshal(message, event)
		if err != nil {
			errHandler(err)
			return
		}
		handler(event)
	}
	return wsServe(cfg, wsHandler, errHandler)
}
