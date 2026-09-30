package protocol

import "github.com/fintech/matching-engine/internal/domain"

// PlaceOrderDTO represents an external client order submission request.
type PlaceOrderDTO struct {
	Symbol      string             `json:"symbol"`
	Side        string             `json:"side"`        // "BUY" or "SELL"
	Type        string             `json:"type"`        // "LIMIT" or "MARKET"
	Price       uint64             `json:"price"`       // in micro-units
	Quantity    uint64             `json:"quantity"`    // in lots
	TimeInForce string             `json:"time_in_force"` // "GTC", "IOC", "FOK"
}

// OrderResponseDTO is returned to the client upon placement.
type OrderResponseDTO struct {
	OrderID      uint64             `json:"order_id"`
	Symbol       string             `json:"symbol"`
	Status       string             `json:"status"`
	LeavesQty    uint64             `json:"leaves_quantity"`
	FilledQty    uint64             `json:"filled_quantity"`
	TradesCount  int                `json:"trades_count"`
	Trades       []TradeDTO         `json:"trades,omitempty"`
	RejectReason string             `json:"reject_reason,omitempty"`
	Timestamp    int64              `json:"timestamp"`
}

// CancelOrderDTO is sent to cancel an active resting order.
type CancelOrderDTO struct {
	Symbol  string `json:"symbol"`
	OrderID uint64 `json:"order_id"`
}

// TradeDTO represents an execution transaction for streaming.
type TradeDTO struct {
	MakerOrderID uint64 `json:"maker_order_id"`
	TakerOrderID uint64 `json:"taker_order_id"`
	Price        uint64 `json:"price"`
	Quantity     uint64 `json:"quantity"`
	Timestamp    int64  `json:"timestamp"`
	MakerSide    string `json:"maker_side"`
}

func FromDomainTrade(t domain.Trade) TradeDTO {
	return TradeDTO{
		MakerOrderID: uint64(t.MakerOrderID),
		TakerOrderID: uint64(t.TakerOrderID),
		Price:        uint64(t.Price),
		Quantity:     uint64(t.Quantity),
		Timestamp:    t.Timestamp,
		MakerSide:    t.MakerSide.String(),
	}
}
