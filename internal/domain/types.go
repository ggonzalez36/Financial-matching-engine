// Package domain defines the core domain entities, value types, and zero-allocation
// pooling primitives for the High-Performance Order Matching Engine.
//
// In High-Frequency Trading (HFT) and ultra-low-latency financial systems:
// 1. Floating-point numbers (float32/float64) are NEVER used for financial values due to
//    IEEE-754 binary floating-point rounding errors and non-deterministic behavior across CPU architectures.
//    Prices and quantities are represented as fixed-point integers (e.g. uint64 representing micro-units: 1 USD = 1,000,000 units).
// 2. Enum types are represented as byte-sized uint8 integers to minimize memory footprint and struct alignment gaps.
package domain

import "fmt"

// Price represents a financial asset price in fixed-point integer units (e.g. 1 unit = 10^-4 or 10^-6 of the currency).
// This eliminates floating-point rounding errors and allows single-cycle CPU integer arithmetic.
type Price uint64

// Quantity represents a discrete volume or lot size in base units.
type Quantity uint64

// OrderID is the unique identifier for an order within the engine lifetime.
type OrderID uint64

// Side denotes whether the order is an offer to buy or an offer to sell.
type Side uint8

const (
	SideUnknown Side = 0
	SideBuy     Side = 1 // Bid: Buyer willing to buy at or below specified price
	SideSell    Side = 2 // Ask: Seller willing to sell at or above specified price
)

func (s Side) String() string {
	switch s {
	case SideBuy:
		return "BUY"
	case SideSell:
		return "SELL"
	default:
		return "UNKNOWN"
	}
}

// OrderType specifies how the matching engine must execute the order.
type OrderType uint8

const (
	OrderTypeUnknown OrderType = 0
	OrderTypeLimit   OrderType = 1 // Limit order: placed in order book if not fully matched
	OrderTypeMarket  OrderType = 2 // Market order: crosses the book aggressively at best available prices
)

func (t OrderType) String() string {
	switch t {
	case OrderTypeLimit:
		return "LIMIT"
	case OrderTypeMarket:
		return "MARKET"
	default:
		return "UNKNOWN"
	}
}

// TimeInForce dictates order expiration and cancellation policies upon crossing.
type TimeInForce uint8

const (
	// GTC (Good 'Til Canceled): Resting order remains on the book until filled or explicitly canceled.
	TimeInForceGTC TimeInForce = 1
	// IOC (Immediate Or Cancel): Matches whatever volume is immediately available at or better than price; remainder canceled.
	TimeInForceIOC TimeInForce = 2
	// FOK (Fill Or Kill): Must be filled completely immediately, otherwise entire order is rejected/canceled with 0 fill.
	TimeInForceFOK TimeInForce = 3
)

func (tif TimeInForce) String() string {
	switch tif {
	case TimeInForceGTC:
		return "GTC"
	case TimeInForceIOC:
		return "IOC"
	case TimeInForceFOK:
		return "FOK"
	default:
		return "UNKNOWN"
	}
}

// OrderStatus defines the operational lifecycle state of an order.
type OrderStatus uint8

const (
	StatusUnknown         OrderStatus = 0
	StatusNew             OrderStatus = 1 // Placed on the book (resting)
	StatusPartiallyFilled OrderStatus = 2 // Filled partially, remainder resting or canceled
	StatusFilled          OrderStatus = 3 // Fully executed
	StatusCanceled        OrderStatus = 4 // Canceled by user request or IOC remainder
	StatusRejected        OrderStatus = 5 // Rejected due to invalid parameters or FOK failure
)

func (s OrderStatus) String() string {
	switch s {
	case StatusNew:
		return "NEW"
	case StatusPartiallyFilled:
		return "PARTIALLY_FILLED"
	case StatusFilled:
		return "FILLED"
	case StatusCanceled:
		return "CANCELED"
	case StatusRejected:
		return "REJECTED"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", s)
	}
}
