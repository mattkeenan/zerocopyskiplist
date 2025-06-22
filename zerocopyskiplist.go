// zerocopyskiplist.go - Zero copy skiplist implementation with context support

package zerocopyskiplist

import (
	"fmt"
	"math/rand"
	"sync"
	"syscall"
	"unsafe"
)

// MergeStrategy defines how to handle conflicts during merge operations
type MergeStrategy int

const (
	MergeTheirs MergeStrategy = iota
	MergeOurs
	MergeError
)

// ItemPtr represents a node in the skiplist with context support
type ItemPtr[T any, K comparable, C comparable] struct {
	item     *T
	key      K
	context  C // Changed from *C to C (value semantics)
	forward  []*ItemPtr[T, K, C]
	backward *ItemPtr[T, K, C]
	level    int
}

// ZeroCopySkiplist is the main skiplist structure with context support
type ZeroCopySkiplist[T any, K comparable, C comparable] struct {
	header         *ItemPtr[T, K, C]
	maxLevel       int
	level          int
	length         int
	getKeyFromItem func(*T) K
	getItemSize    func(*T) int
	cmpKey         func(K, K) int
	rw             sync.RWMutex
}

// MakeZeroCopySkiplist creates a new skiplist with context support
func MakeZeroCopySkiplist[T any, K comparable, C comparable](
	maxLevel int,
	getKeyFromItem func(*T) K,
	getItemSize func(*T) int,
	cmpKey func(K, K) int,
) *ZeroCopySkiplist[T, K, C] {

	header := &ItemPtr[T, K, C]{
		forward: make([]*ItemPtr[T, K, C], maxLevel+1),
		level:   maxLevel,
	}

	return &ZeroCopySkiplist[T, K, C]{
		header:         header,
		maxLevel:       maxLevel,
		level:          0,
		length:         0,
		getKeyFromItem: getKeyFromItem,
		getItemSize:    getItemSize,
		cmpKey:         cmpKey,
	}
}

// makeZeroCopySkiplist creates a skiplist - always requires explicit context type parameter
func makeZeroCopySkiplist[T any, K comparable, C comparable](
	maxLevel int,
	getKeyFromItem func(*T) K,
	getItemSize func(*T) int,
	cmpKey func(K, K) int,
) *ZeroCopySkiplist[T, K, C] {
	return MakeZeroCopySkiplist[T, K, C](maxLevel, getKeyFromItem, getItemSize, cmpKey)
}

// Insert adds an item to the skiplist with optional context
func (sl *ZeroCopySkiplist[T, K, C]) Insert(item *T, context C) bool {
	sl.rw.Lock()
	defer sl.rw.Unlock()

	key := sl.getKeyFromItem(item)

	// Find position for insertion
	update := make([]*ItemPtr[T, K, C], sl.maxLevel+1)
	current := sl.header

	for i := sl.level; i >= 0; i-- {
		for current.forward[i] != nil && sl.cmpKey(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]

	// If key already exists, update item and context, return false
	if current != nil && sl.cmpKey(current.key, key) == 0 {
		current.item = item
		current.context = context // Always update context (no nil check needed for value types)
		return false
	}

	// Generate random level for new node
	newLevel := sl.randomLevel()
	if newLevel > sl.level {
		for i := sl.level + 1; i <= newLevel; i++ {
			update[i] = sl.header
		}
		sl.level = newLevel
	}

	// Create new node
	newNode := &ItemPtr[T, K, C]{
		item:    item,
		key:     key,
		context: context,
		forward: make([]*ItemPtr[T, K, C], newLevel+1),
		level:   newLevel,
	}

	// Update forward pointers
	for i := 0; i <= newLevel; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	// Update backward pointer
	if newNode.forward[0] != nil {
		newNode.forward[0].backward = newNode
	}
	if update[0] != sl.header {
		newNode.backward = update[0]
	}

	sl.length++
	return true
}

// Delete removes an item with the given key
func (sl *ZeroCopySkiplist[T, K, C]) Delete(key K) bool {
	sl.rw.Lock()
	defer sl.rw.Unlock()

	update := make([]*ItemPtr[T, K, C], sl.maxLevel+1)
	current := sl.header

	// Find the node to delete
	for i := sl.level; i >= 0; i-- {
		for current.forward[i] != nil && sl.cmpKey(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]

	// If key doesn't exist, return false
	if current == nil || sl.cmpKey(current.key, key) != 0 {
		return false
	}

	// Update forward pointers
	for i := 0; i <= current.level; i++ {
		update[i].forward[i] = current.forward[i]
	}

	// Update backward pointer
	if current.forward[0] != nil {
		current.forward[0].backward = current.backward
	}

	// Update level if necessary
	for sl.level > 0 && sl.header.forward[sl.level] == nil {
		sl.level--
	}

	sl.length--
	return true
}

// First returns the first item in the skiplist
func (sl *ZeroCopySkiplist[T, K, C]) First() *ItemPtr[T, K, C] {
	sl.rw.RLock()
	defer sl.rw.RUnlock()
	return sl.header.forward[0]
}

// Last returns the last item in the skiplist
func (sl *ZeroCopySkiplist[T, K, C]) Last() *ItemPtr[T, K, C] {
	sl.rw.RLock()
	defer sl.rw.RUnlock()

	current := sl.header
	for i := sl.level; i >= 0; i-- {
		for current.forward[i] != nil {
			current = current.forward[i]
		}
	}

	if current == sl.header {
		return nil
	}
	return current
}

// Length returns the number of items in the skiplist
func (sl *ZeroCopySkiplist[T, K, C]) Length() int {
	sl.rw.RLock()
	defer sl.rw.RUnlock()
	return sl.length
}

// IsEmpty returns true if the skiplist is empty
func (sl *ZeroCopySkiplist[T, K, C]) IsEmpty() bool {
	sl.rw.RLock()
	defer sl.rw.RUnlock()
	return sl.length == 0
}

// Find returns the item and context for the given key
func (sl *ZeroCopySkiplist[T, K, C]) Find(key K) (*ItemPtr[T, K, C], C) {
	return sl.search(key)
}

// Copy creates a deep copy of the skiplist structure (zero-copy for items)
func (sl *ZeroCopySkiplist[T, K, C]) Copy() *ZeroCopySkiplist[T, K, C] {
	sl.rw.RLock()
	defer sl.rw.RUnlock()

	newSL := MakeZeroCopySkiplist[T, K, C](sl.maxLevel, sl.getKeyFromItem, sl.getItemSize, sl.cmpKey)

	current := sl.First()
	for current != nil {
		newSL.Insert(current.item, current.context)
		current = current.Next()
	}

	return newSL
}

// CallbackToIovecSlice generates Iovec slices for items that match the callback filter
func (sl *ZeroCopySkiplist[T, K, C]) CallbackToIovecSlice(callback func(*ItemPtr[T, K, C]) bool) []syscall.Iovec {
	sl.rw.RLock()
	defer sl.rw.RUnlock()

	iovecs := make([]syscall.Iovec, 0, sl.length/2)

	current := sl.First()
	for current != nil {
		// Save current.Next() in case the user wants to do something crazy like delete current
		tmp := current.Next()
		if callback(current) { // Fixed: removed negation and pass current directly (not &current)
			iovec := syscall.Iovec{
				Base: (*byte)(unsafe.Pointer(current.item)),
				Len:  uint64(sl.getItemSize(current.item)),
			}
			iovecs = append(iovecs, iovec)
		}
		current = tmp
	}
	return iovecs
}

// ToIovecSlice generates Iovec slices for all items (ignoring context parameter for backward compatibility)
func (sl *ZeroCopySkiplist[T, K, C]) ToIovecSlice(context C) []syscall.Iovec {
	// Note: context parameter is ignored to maintain backward compatibility with existing ToIovecSlice() calls
	return sl.CallbackToIovecSlice(func(item *ItemPtr[T, K, C]) bool {
		return true // Include all items
	})
}

// ToContextIovecSlice generates Iovec slices for items that match the context
func (sl *ZeroCopySkiplist[T, K, C]) ToContextIovecSlice(context C) []syscall.Iovec {
	return sl.CallbackToIovecSlice(func(item *ItemPtr[T, K, C]) bool {
		return item.context == context // Direct value comparison (no pointer dereferencing)
	})
}

// ToNotContextIovecSlice generates Iovec slices for items that don't match the context
func (sl *ZeroCopySkiplist[T, K, C]) ToNotContextIovecSlice(context C) []syscall.Iovec {
	return sl.CallbackToIovecSlice(func(item *ItemPtr[T, K, C]) bool {
		return item.context != context // Direct value comparison (no pointer dereferencing)
	})
}

// Merge merges another skiplist into this one with conflict resolution
func (sl *ZeroCopySkiplist[T, K, C]) Merge(other *ZeroCopySkiplist[T, K, C], strategy MergeStrategy) error {
	other.rw.RLock()
	defer other.rw.RUnlock()

	current := other.First()
	for current != nil {
		existing, _ := sl.search(current.key)

		if existing != nil {
			// Handle conflict based on strategy
			switch strategy {
			case MergeTheirs:
				sl.Insert(current.item, current.context)
			case MergeOurs:
				// Keep existing, do nothing
			case MergeError:
				return fmt.Errorf("key conflict during merge: %v", current.key)
			}
		} else {
			sl.Insert(current.item, current.context)
		}
		current = current.Next()
	}

	return nil
}

// Item returns the pointer to the original data structure
func (ip *ItemPtr[T, K, C]) Item() *T {
	return ip.item
}

// FindItem returns only the ItemPtr for backward compatibility
func (sl *ZeroCopySkiplist[T, K, C]) FindItem(key K) *ItemPtr[T, K, C] {
	item, _ := sl.search(key)
	return item
}

// Context returns the context value (changed from pointer to value)
func (ip *ItemPtr[T, K, C]) Context() C {
	return ip.context
}

// UpdateContext updates the context for an existing key (changed parameter from *C to C)
func (sl *ZeroCopySkiplist[T, K, C]) UpdateContext(key K, context C) bool {
	item, _ := sl.search(key)
	if item != nil {
		sl.rw.Lock()
		item.context = context
		sl.rw.Unlock()
		return true
	}
	return false
}

// SetContext updates the context value (changed parameter from *C to C)
func (ip *ItemPtr[T, K, C]) SetContext(context C) {
	ip.context = context
}

// Key returns the cached key value
func (ip *ItemPtr[T, K, C]) Key() K {
	return ip.key
}

// search function updated to return context value (changed from *C to C)
func (sl *ZeroCopySkiplist[T, K, C]) search(key K) (*ItemPtr[T, K, C], C) {
	sl.rw.RLock()
	defer sl.rw.RUnlock()

	current := sl.header

	// Search from top level down
	for i := sl.level; i >= 0; i-- {
		for current.forward[i] != nil && sl.cmpKey(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
	}

	// Move to next node which should contain the key if it exists
	current = current.forward[0]

	if current != nil && sl.cmpKey(current.key, key) == 0 {
		return current, current.context
	}

	// Return zero value for context when not found (instead of nil)
	var zeroContext C
	return nil, zeroContext
}

// Next returns the next item in sorted order
func (ip *ItemPtr[T, K, C]) Next() *ItemPtr[T, K, C] {
	return ip.forward[0]
}

// Prev returns the previous item in sorted order
func (ip *ItemPtr[T, K, C]) Prev() *ItemPtr[T, K, C] {
	return ip.backward
}

// randomLevel generates a random level for new nodes
func (sl *ZeroCopySkiplist[T, K, C]) randomLevel() int {
	level := 0
	for rand.Float32() < 0.5 && level < sl.maxLevel {
		level++
	}
	return level
}
