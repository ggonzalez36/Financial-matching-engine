package domain

import "fmt"

// Trade represents an executed fill transaction between a passive maker order resting on the book
// and an aggressive taker order entering the engine.
//
// IN FINANCIAL MARKET MECHANICS:
// - Maker: Liquidity provider whose order was resting in the Limit Order Book.
// - Taker: Liquidity consumer whose order aggressively crossed the bid-ask spread.
// - Execution Price: Always determined by the maker's limit price in standard continuous double auctions.
//
// MEMORY LAYOUT:
// Aligned to 48 bytes with 7 bytes explicit padding, preventing false sharing across adjacent slice items.
type Trade struct {
	MakerOrderID OrderID // 8 bytes
	TakerOrderID OrderID // 8 bytes
	Price        Price   // 8 bytes (execution price)
	Quantity     Quantity// 8 bytes (executed volume)
	Timestamp    int64   // 8 bytes (execution timestamp in Unix nanoseconds)
	MakerSide    Side    // 1 byte  (the side of the resting maker order)
	_pad         [7]byte // 7 bytes (explicit padding to 48 bytes)
}

func (t *Trade) Reset() {
	t.MakerOrderID = 0
	t.TakerOrderID = 0
	t.Price = 0
	t.Quantity = 0
	t.Timestamp = 0
	t.MakerSide = SideUnknown
}

func (t *Trade) String() string {
	return fmt.Sprintf("Trade[Maker=%d, Taker=%d, Px=%d, Qty=%d, MakerSide=%s, Ts=%d]",
		t.MakerOrderID, t.TakerOrderID, t.Price, t.Quantity, t.MakerSide, t.Timestamp)
}
