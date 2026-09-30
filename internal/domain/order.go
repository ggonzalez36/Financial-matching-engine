package domain

import (
	"fmt"
	"unsafe"
)

// Order represents an order submitted to the matching engine.
//
// MEMORY LAYOUT & CPU CACHE LINE OPTIMIZATION:
// On modern x86_64 and ARM64 architectures, the CPU L1 data cache line is 64 bytes.
// When a data structure is not a multiple of 64 bytes or fields cross cache line boundaries,
// memory fetches cause "cache line splits", leading to increased latency.
//
// Here, fields are packed strictly by alignment to ensure:
// 1. Zero wasted internal padding between fields.
// 2. Exactly 64 bytes total struct size (verified by compile-time assert).
// 3. Intrusive pointers (*Next, *Prev) eliminate the need for container/list or extra pointer node allocations.
type Order struct {
	ID        OrderID     // 8 bytes (offset 0..7)
	Price     Price       // 8 bytes (offset 8..15)
	Quantity  Quantity    // 8 bytes (offset 16..23) - Original submitted quantity
	LeavesQty Quantity    // 8 bytes (offset 24..31) - Remaining open/unfilled quantity
	Timestamp int64       // 8 bytes (offset 32..39) - Entry timestamp in Unix nanoseconds
	Next      *Order      // 8 bytes (offset 40..47) - Intrusive pointer for FIFO queue
	Prev      *Order      // 8 bytes (offset 48..55) - Intrusive pointer for FIFO queue
	Side      Side        // 1 byte  (offset 56)
	Type      OrderType   // 1 byte  (offset 57)
	TIF       TimeInForce // 1 byte  (offset 58)
	Status    OrderStatus // 1 byte  (offset 59)
	_pad      [4]byte     // 4 bytes (offset 60..63) - Explicit padding to guarantee 64B cache line alignment
}

// Compile-time verification that Order is exactly 64 bytes.
// If the struct layout is altered and exceeds or drops below 64 bytes, this will fail to compile.
const _ = uint(64 - unsafe.Sizeof(Order{}))
const _ = uint(unsafe.Sizeof(Order{}) - 64)

// Reset zeroes out the order fields to prepare it for recycling back into sync.Pool.
// Intrusive pointers are unlinked to prevent dangling memory references in GC.
func (o *Order) Reset() {
	o.ID = 0
	o.Price = 0
	o.Quantity = 0
	o.LeavesQty = 0
	o.Timestamp = 0
	o.Next = nil
	o.Prev = nil
	o.Side = SideUnknown
	o.Type = OrderTypeUnknown
	o.TIF = 0
	o.Status = StatusUnknown
}

// FilledQty returns the volume of the order that has already been executed.
func (o *Order) FilledQty() Quantity {
	if o.Quantity >= o.LeavesQty {
		return o.Quantity - o.LeavesQty
	}
	return 0
}

// IsFilled returns true if no remaining quantity is open.
func (o *Order) IsFilled() bool {
	return o.LeavesQty == 0
}

func (o *Order) String() string {
	return fmt.Sprintf("Order[ID=%d, Side=%s, Type=%s, Px=%d, Qty=%d, Leaves=%d, Status=%s]",
		o.ID, o.Side, o.Type, o.Price, o.Quantity, o.LeavesQty, o.Status)
}
