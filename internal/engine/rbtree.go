package engine

import (
	"sync"

	"github.com/fintech/matching-engine/internal/domain"
)

const (
	red   = true
	black = false
)

type rbNode struct {
	price  domain.Price
	level  *PriceLevel
	left   *rbNode
	right  *rbNode
	parent *rbNode
	color  bool
}

var rbNodePool = sync.Pool{
	New: func() any {
		return new(rbNode)
	},
}

func acquireRBNode(price domain.Price, level *PriceLevel, color bool) *rbNode {
	n := rbNodePool.Get().(*rbNode)
	n.price = price
	n.level = level
	n.left = nil
	n.right = nil
	n.parent = nil
	n.color = color
	return n
}

func releaseRBNode(n *rbNode) {
	if n == nil {
		return
	}
	n.left = nil
	n.right = nil
	n.parent = nil
	n.level = nil
	rbNodePool.Put(n)
}

// rbTree is a memory-pooled Red-Black Tree specialized for price levels.
// It guarantees O(log N) worst-case insertion, search, and deletion with minimal pointer indirection.
type rbTree struct {
	root  *rbNode
	count int
}

func newRBTree() *rbTree {
	return &rbTree{}
}

func (t *rbTree) Len() int {
	return t.count
}

// Search retrieves the PriceLevel for a specific price in O(log N) time.
func (t *rbTree) Search(price domain.Price) *PriceLevel {
	n := t.root
	for n != nil {
		if price < n.price {
			n = n.left
		} else if price > n.price {
			n = n.right
		} else {
			return n.level
		}
	}
	return nil
}

// Min returns the price level with the minimum price (Best Ask) in O(log N) time.
func (t *rbTree) Min() *PriceLevel {
	if t.root == nil {
		return nil
	}
	n := t.root
	for n.left != nil {
		n = n.left
	}
	return n.level
}

// Max returns the price level with the maximum price (Best Bid) in O(log N) time.
func (t *rbTree) Max() *PriceLevel {
	if t.root == nil {
		return nil
	}
	n := t.root
	for n.right != nil {
		n = n.right
	}
	return n.level
}

// Insert places a new PriceLevel into the tree and rebalances in O(log N) time.
func (t *rbTree) Insert(price domain.Price, level *PriceLevel) {
	var parent *rbNode
	curr := t.root

	for curr != nil {
		parent = curr
		if price < curr.price {
			curr = curr.left
		} else if price > curr.price {
			curr = curr.right
		} else {
			// Update existing level
			curr.level = level
			return
		}
	}

	newNode := acquireRBNode(price, level, red)
	newNode.parent = parent

	if parent == nil {
		t.root = newNode
	} else if price < parent.price {
		parent.left = newNode
	} else {
		parent.right = newNode
	}

	t.insertFixup(newNode)
	t.count++
}

func (t *rbTree) insertFixup(z *rbNode) {
	for z.parent != nil && z.parent.color == red {
		if z.parent.parent == nil {
			break
		}
		if z.parent == z.parent.parent.left {
			y := z.parent.parent.right
			if y != nil && y.color == red {
				z.parent.color = black
				y.color = black
				z.parent.parent.color = red
				z = z.parent.parent
			} else {
				if z == z.parent.right {
					z = z.parent
					t.rotateLeft(z)
				}
				z.parent.color = black
				z.parent.parent.color = red
				t.rotateRight(z.parent.parent)
			}
		} else {
			y := z.parent.parent.left
			if y != nil && y.color == red {
				z.parent.color = black
				y.color = black
				z.parent.parent.color = red
				z = z.parent.parent
			} else {
				if z == z.parent.left {
					z = z.parent
					t.rotateRight(z)
				}
				z.parent.color = black
				z.parent.parent.color = red
				t.rotateLeft(z.parent.parent)
			}
		}
	}
	t.root.color = black
}

// Delete removes a price level from the tree and restores RB properties in O(log N) time.
func (t *rbTree) Delete(price domain.Price) {
	z := t.root
	for z != nil {
		if price < z.price {
			z = z.left
		} else if price > z.price {
			z = z.right
		} else {
			break
		}
	}
	if z == nil {
		return
	}

	var y, x *rbNode
	yOriginalColor := z.color

	if z.left == nil {
		x = z.right
		t.transplant(z, z.right)
	} else if z.right == nil {
		x = z.left
		t.transplant(z, z.left)
	} else {
		// Node has two children; find successor in right subtree
		y = z.right
		for y.left != nil {
			y = y.left
		}
		yOriginalColor = y.color
		x = y.right

		if y.parent == z {
			if x != nil {
				x.parent = y
			}
		} else {
			t.transplant(y, y.right)
			y.right = z.right
			if y.right != nil {
				y.right.parent = y
			}
		}

		t.transplant(z, y)
		y.left = z.left
		if y.left != nil {
			y.left.parent = y
		}
		y.color = z.color
	}

	releaseRBNode(z)
	t.count--

	if yOriginalColor == black && x != nil {
		t.deleteFixup(x)
	}
}

func (t *rbTree) transplant(u, v *rbNode) {
	if u.parent == nil {
		t.root = v
	} else if u == u.parent.left {
		u.parent.left = v
	} else {
		u.parent.right = v
	}
	if v != nil {
		v.parent = u.parent
	}
}

func (t *rbTree) deleteFixup(x *rbNode) {
	for x != t.root && x != nil && x.color == black {
		if x.parent == nil {
			break
		}
		if x == x.parent.left {
			w := x.parent.right
			if w != nil && w.color == red {
				w.color = black
				x.parent.color = red
				t.rotateLeft(x.parent)
				w = x.parent.right
			}
			if w != nil && (w.left == nil || w.left.color == black) && (w.right == nil || w.right.color == black) {
				w.color = red
				x = x.parent
			} else {
				if w != nil && (w.right == nil || w.right.color == black) {
					if w.left != nil {
						w.left.color = black
					}
					w.color = red
					t.rotateRight(w)
					w = x.parent.right
				}
				if w != nil {
					w.color = x.parent.color
					x.parent.color = black
					if w.right != nil {
						w.right.color = black
					}
					t.rotateLeft(x.parent)
				}
				x = t.root
			}
		} else {
			w := x.parent.left
			if w != nil && w.color == red {
				w.color = black
				x.parent.color = red
				t.rotateRight(x.parent)
				w = x.parent.left
			}
			if w != nil && (w.right == nil || w.right.color == black) && (w.left == nil || w.left.color == black) {
				w.color = red
				x = x.parent
			} else {
				if w != nil && (w.left == nil || w.left.color == black) {
					if w.right != nil {
						w.right.color = black
					}
					w.color = red
					t.rotateLeft(w)
					w = x.parent.left
				}
				if w != nil {
					w.color = x.parent.color
					x.parent.color = black
					if w.left != nil {
						w.left.color = black
					}
					t.rotateRight(x.parent)
				}
				x = t.root
			}
		}
	}
	if x != nil {
		x.color = black
	}
}

func (t *rbTree) rotateLeft(x *rbNode) {
	y := x.right
	if y == nil {
		return
	}
	x.right = y.left
	if y.left != nil {
		y.left.parent = x
	}
	y.parent = x.parent
	if x.parent == nil {
		t.root = y
	} else if x == x.parent.left {
		x.parent.left = y
	} else {
		x.parent.right = y
	}
	y.left = x
	x.parent = y
}

func (t *rbTree) rotateRight(x *rbNode) {
	y := x.left
	if y == nil {
		return
	}
	x.left = y.right
	if y.right != nil {
		y.right.parent = x
	}
	y.parent = x.parent
	if x.parent == nil {
		t.root = y
	} else if x == x.parent.right {
		x.parent.right = y
	} else {
		x.parent.left = y
	}
	y.right = x
	x.parent = y
}

// InOrder traverses nodes in ascending order of price (used for Asks: lowest to highest).
// If fn returns false, traversal terminates early.
func (t *rbTree) InOrder(fn func(level *PriceLevel) bool) {
	t.inOrderHelper(t.root, fn)
}

func (t *rbTree) inOrderHelper(n *rbNode, fn func(level *PriceLevel) bool) bool {
	if n == nil {
		return true
	}
	if !t.inOrderHelper(n.left, fn) {
		return false
	}
	if !fn(n.level) {
		return false
	}
	return t.inOrderHelper(n.right, fn)
}

// ReverseInOrder traverses nodes in descending order of price (used for Bids: highest to lowest).
// If fn returns false, traversal terminates early.
func (t *rbTree) ReverseInOrder(fn func(level *PriceLevel) bool) {
	t.reverseInOrderHelper(t.root, fn)
}

func (t *rbTree) reverseInOrderHelper(n *rbNode, fn func(level *PriceLevel) bool) bool {
	if n == nil {
		return true
	}
	if !t.reverseInOrderHelper(n.right, fn) {
		return false
	}
	if !fn(n.level) {
		return false
	}
	return t.reverseInOrderHelper(n.left, fn)
}
