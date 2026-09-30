package domain

import "fmt"

// EventType categorizes execution and lifecycle events emitted by the engine.
type EventType uint8

const (
	EventOrderPlaced EventType = iota + 1
	EventOrderPartiallyFilled
	EventOrderFilled
	EventOrderCanceled
	EventOrderRejected
	EventTradeExecuted
)

func (e EventType) String() string {
	switch e {
	case EventOrderPlaced:
		return "ORDER_PLACED"
	case EventOrderPartiallyFilled:
		return "ORDER_PARTIALLY_FILLED"
	case EventOrderFilled:
		return "ORDER_FILLED"
	case EventOrderCanceled:
		return "ORDER_CANCELED"
	case EventOrderRejected:
		return "ORDER_REJECTED"
	case EventTradeExecuted:
		return "TRADE_EXECUTED"
	default:
		return "UNKNOWN_EVENT"
	}
}

// ExecutionReport represents the atomic outcome of processing a command (Place, Cancel) in the matching loop.
// It bundles the order state transition and any trades generated during matching.
type ExecutionReport struct {
	SubjectOrder *Order       // The order submitted or acted upon
	Trades       []Trade      // Any trades executed as a taker (matched against maker orders)
	Status       OrderStatus  // Resulting status
	EventType    EventType    // Specific event classifier
	RejectReason string       // Error or rejection cause (if Status == StatusRejected)
	Timestamp    int64        // Engine time in Unix nanoseconds
}

func (er *ExecutionReport) Reset() {
	er.SubjectOrder = nil
	// Slice header reset while retaining underlying capacity to eliminate slice reallocations!
	er.Trades = er.Trades[:0]
	er.Status = StatusUnknown
	er.EventType = 0
	er.RejectReason = ""
	er.Timestamp = 0
}

func (er *ExecutionReport) String() string {
	return fmt.Sprintf("ExecutionReport[Type=%s, Order=%v, Trades=%d, Status=%s, Reason=%s]",
		er.EventType, er.SubjectOrder, len(er.Trades), er.Status, er.RejectReason)
}

// PriceLevelSnapshot represents aggregated depth at a specific price tick for market data streaming (L2 book).
type PriceLevelSnapshot struct {
	Price      Price    `json:"price"`
	Quantity   Quantity `json:"quantity"`
	OrderCount uint32   `json:"order_count"`
}

// MarketDepth represents a Level-2 (L2) market data snapshot for bids and asks.
type MarketDepth struct {
	Symbol    string               `json:"symbol"`
	Timestamp int64                `json:"timestamp"`
	Bids      []PriceLevelSnapshot `json:"bids"`
	Asks      []PriceLevelSnapshot `json:"asks"`
}
