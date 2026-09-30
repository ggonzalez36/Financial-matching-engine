package engine

import (
	"sync"

	"github.com/fintech/matching-engine/internal/domain"
)

// PriceLevel represents all resting orders at a discrete price tick.
//
// PRICE-TIME (FIFO) PRIORITY:
// All orders at the exact same price are served strictly in the order they arrived (Time priority).
// To achieve O(1) append, O(1) pop, and O(1) arbitrary removal (for cancellations),
// we use an intrusive doubly linked list where Order itself stores *Prev and *Next pointers.
// This completely avoids allocating list node wrappers (like container/list.Element).
type PriceLevel struct {
	Price       domain.Price    // The price tick
	TotalVolume domain.Quantity // Aggregate open volume across all orders at this level
	OrderCount  uint32          // Count of active orders
	head        *domain.Order   // Oldest resting order (highest time priority)
	tail        *domain.Order   // Newest resting order
}

var priceLevelPool = sync.Pool{
	New: func() any {
		return new(PriceLevel)
	},
}

// acquirePriceLevel gets a recycled PriceLevel from the pool.
func acquirePriceLevel(price domain.Price) *PriceLevel {
	pl := priceLevelPool.Get().(*PriceLevel)
	pl.Price = price
	pl.TotalVolume = 0
	pl.OrderCount = 0
	pl.head = nil
	pl.tail = nil
	return pl
}

// releasePriceLevel resets and returns the PriceLevel to the pool.
func releasePriceLevel(pl *PriceLevel) {
	if pl == nil {
		return
	}
	pl.head = nil
	pl.tail = nil
	pl.OrderCount = 0
	pl.TotalVolume = 0
	priceLevelPool.Put(pl)
}

// Enqueue adds an order to the tail of the price level in O(1) time.
func (pl *PriceLevel) Enqueue(o *domain.Order) {
	o.Next = nil
	o.Prev = pl.tail

	if pl.tail != nil {
		pl.tail.Next = o
	} else {
		// Queue was empty; this order becomes both head and tail.
		pl.head = o
	}
	pl.tail = o

	pl.TotalVolume += o.LeavesQty
	pl.OrderCount++
}

// Dequeue pops the oldest order (highest time priority) from the head in O(1) time.
func (pl *PriceLevel) Dequeue() *domain.Order {
	if pl.head == nil {
		return nil
	}
	o := pl.head
	pl.Remove(o)
	return o
}

// Remove unlinks an arbitrary order from this price level in O(1) time.
// Since the order holds direct Prev and Next pointers, no list traversal is needed.
func (pl *PriceLevel) Remove(o *domain.Order) {
	if o.Prev != nil {
		o.Prev.Next = o.Next
	} else {
		// o was head
		pl.head = o.Next
	}

	if o.Next != nil {
		o.Next.Prev = o.Prev
	} else {
		// o was tail
		pl.tail = o.Prev
	}

	o.Next = nil
	o.Prev = nil

	if pl.TotalVolume >= o.LeavesQty {
		pl.TotalVolume -= o.LeavesQty
	} else {
		pl.TotalVolume = 0
	}

	if pl.OrderCount > 0 {
		pl.OrderCount--
	}
}

// Head returns the first order in the FIFO queue without removing it.
func (pl *PriceLevel) Head() *domain.Order {
	return pl.head
}

// IsEmpty returns true if there are no remaining orders at this price level.
func (pl *PriceLevel) IsEmpty() bool {
	return pl.head == nil || pl.OrderCount == 0
}
