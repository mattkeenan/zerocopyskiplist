// Package zerocopyskiplist provides an optimized skiplist implementation for accessing
// streamed or existing mmap() arrays of homogeneous data structures.
//
// This package is designed for scenarios where you have an existing array of data
// structures (potentially memory-mapped) and need fast ordered access through
// a skiplist index that points to the original data without copying it.
package zerocopyskiplist

import (
	"errors"
	"math/rand"
	"sync"
	"time"
	"unsafe"
)

const (
	// Probability factor for level generation
	P = 0.25
)

// MergeStrategy defines how to handle key conflicts during merge operations
type MergeStrategy int

const (
	// MergeTheirs uses items from the second skiplist when keys conflict
	MergeTheirs MergeStrategy = iota
	// MergeOurs keeps items from the first skiplist when keys conflict
	MergeOurs
	// MergeError returns an error when any key conflict is encountered
	MergeError
)

// ItemPtr represents a node in the skiplist that points to actual data.
// It maintains pointers to the original data structure and provides
// forward/backward navigation through the skiplist levels.
type ItemPtr[T any, K comparable] struct {
	item     *T               // pointer to the actual data item in itemArray
	key      K                // cached key for this item (extracted via getKeyFromItem)
	forward  []*ItemPtr[T, K] // forward pointers for skiplist levels
	backward *ItemPtr[T, K]   // backward pointer for double linking
}

// ZeroCopySkiplist represents the main skiplist structure that provides
// optimized ordered access to an existing array of data structures.
type ZeroCopySkiplist[T any, K comparable] struct {
	header         *ItemPtr[T, K] // header sentinel node
	level          int            // current maximum level of the skiplist
	maxLevel       int            // maximum allowed levels in the skiplist
	length         int            // number of elements in the skiplist
	getKeyFromItem func(*T) K     // function to extract key from item
	getItemSize    func(*T) int   // function to get size of item in bytes
	cmpKey         func(K, K) int // function to compare two keys
	rnd            *rand.Rand     // random number generator for level assignment
	mu             sync.RWMutex   // mutex for thread-safe operations
}

// makeZeroCopySkiplist creates and initializes a new skiplist.
//
// Parameters:
//   - maxLevel: maximum number of levels in the skiplist (must be > 0)
//   - getKeyFromItem: function that takes a pointer to an item and returns its key
//   - getItemSize: function that takes a pointer to an item and returns its size in bytes
//   - cmpKey: comparison function that returns:
//     -1 if the first key should be ordered before the second key
//     0 if the keys have the same order
//     1 if the first key should be ordered after the second key
//
// Returns a new ZeroCopySkiplist ready for use.
func MakeZeroCopySkiplist[T any, K comparable](
	maxLevel int,
	getKeyFromItem func(*T) K,
	getItemSize func(*T) int,
	cmpKey func(K, K) int,
) *ZeroCopySkiplist[T, K] {
	if maxLevel <= 0 {
		maxLevel = 16 // default fallback
	}

	// Create header node with maximum forward pointers
	header := &ItemPtr[T, K]{
		forward: make([]*ItemPtr[T, K], maxLevel),
	}

	return &ZeroCopySkiplist[T, K]{
		header:         header,
		level:          0,
		maxLevel:       maxLevel,
		length:         0,
		getKeyFromItem: getKeyFromItem,
		getItemSize:    getItemSize,
		cmpKey:         cmpKey,
		rnd:            rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// randomLevel generates a random level for a new node using geometric distribution.
// This maintains the skiplist's probabilistic balance properties.
func (sl *ZeroCopySkiplist[T, K]) randomLevel() int {
	level := 0
	for sl.rnd.Float64() < P && level < sl.maxLevel-1 {
		level++
	}
	return level
}

// search performs the core skiplist search operation.
// It returns the update array needed for insertion and the found node (if any).
func (sl *ZeroCopySkiplist[T, K]) search(key K) ([]*ItemPtr[T, K], *ItemPtr[T, K]) {
	update := make([]*ItemPtr[T, K], sl.maxLevel)
	current := sl.header

	// Start from the highest level and work down
	for i := sl.level; i >= 0; i-- {
		// Move forward while the next node's key is less than target key
		for current.forward[i] != nil && sl.cmpKey(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	// Move to the next node at level 0 (potential match)
	current = current.forward[0]

	return update, current
}

// Insert adds a new item to the skiplist.
// The item pointer should point to an existing data structure in your itemArray.
// The skiplist will extract the key using the provided getKeyFromItem function
// and maintain pointers to the original data without copying it.
//
// This operation is thread-safe and acquires a write lock during insertion.
//
// Parameters:
//   - item: pointer to the data structure in your existing itemArray
//
// Returns true if the item was inserted, false if an item with the same key already exists.
func (sl *ZeroCopySkiplist[T, K]) Insert(item *T) bool {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	if item == nil {
		return false
	}

	// Extract the key from the item
	key := sl.getKeyFromItem(item)

	// Find insertion point
	update, current := sl.search(key)

	// Check if key already exists
	if current != nil && sl.cmpKey(current.key, key) == 0 {
		return false // Key already exists
	}

	// Generate random level for new node
	newLevel := sl.randomLevel()

	// Update skiplist level if necessary
	if newLevel > sl.level {
		for i := sl.level + 1; i <= newLevel; i++ {
			update[i] = sl.header
		}
		sl.level = newLevel
	}

	// Create new node
	newNode := &ItemPtr[T, K]{
		item:    item,
		key:     key,
		forward: make([]*ItemPtr[T, K], newLevel+1),
	}

	// Update forward pointers
	for i := 0; i <= newLevel; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	// Update backward pointers for double linking
	if newNode.forward[0] != nil {
		newNode.forward[0].backward = newNode
	}
	if update[0] != sl.header {
		newNode.backward = update[0]
	}

	sl.length++
	return true
}

// Delete removes an item with the given key from the skiplist.
// This operation is thread-safe and acquires a write lock during deletion.
//
// Parameters:
//   - key: the key of the item to delete
//
// Returns true if the item was found and deleted, false if no item with the key exists.
func (sl *ZeroCopySkiplist[T, K]) Delete(key K) bool {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	// Find the node to delete
	update, current := sl.search(key)

	// Check if key exists
	if current == nil || sl.cmpKey(current.key, key) != 0 {
		return false // Key not found
	}

	// Remove node from all levels
	for i := 0; i < len(current.forward); i++ {
		update[i].forward[i] = current.forward[i]
	}

	// Update backward pointer of the next node
	if current.forward[0] != nil {
		current.forward[0].backward = current.backward
	}

	// Reduce skiplist level if necessary
	for sl.level > 0 && sl.header.forward[sl.level] == nil {
		sl.level--
	}

	sl.length--
	return true
}

// Merge merges another skiplist into this skiplist. The other skiplist is not modified.
// This operation is thread-safe and acquires write lock on this skiplist and read lock on the other.
//
// Parameters:
//   - other: the skiplist to merge into this one
//   - strategy: how to handle key conflicts between the two skiplists
//
// Key conflict strategies:
//   - MergeTheirs: use items from the other skiplist when keys conflict
//   - MergeOurs: keep items from this skiplist when keys conflict
//   - MergeError: return error on any key conflict, leaving this skiplist in intermediate state
//
// Returns nil on success, error on failure (only possible with MergeError strategy).
//
// Note: If an error occurs, this skiplist may be left in an intermediate state with some
// items from the other skiplist already merged. Consider copying this skiplist before
// merging if you need to preserve the original state on error.
func (sl *ZeroCopySkiplist[T, K]) Merge(other *ZeroCopySkiplist[T, K], strategy MergeStrategy) error {
	if other == nil {
		return nil // Nothing to merge
	}

	// Verify skiplists are compatible (same configuration)
	if sl.maxLevel != other.maxLevel {
		return errors.New("cannot merge skiplists with different maxLevel configurations")
	}

	// Lock both skiplists: write lock on this, read lock on other
	// Always lock in consistent order to prevent deadlocks (this first, then other)
	sl.mu.Lock()
	defer sl.mu.Unlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	// Iterate through all items in the other skiplist
	for current := other.header.forward[0]; current != nil; current = current.forward[0] {
		key := current.key
		item := current.item

		// Check if key already exists in this skiplist
		_, existing := sl.search(key)
		hasConflict := existing != nil && sl.cmpKey(existing.key, key) == 0

		if hasConflict {
			// Handle key conflict based on strategy
			switch strategy {
			case MergeTheirs:
				// Delete existing item and insert new one
				sl.deleteUnsafe(key)
				if !sl.insertUnsafe(item) {
					return errors.New("failed to insert item during merge (this should not happen)")
				}
			case MergeOurs:
				// Keep existing item, skip the new one
				continue
			case MergeError:
				// Return error immediately
				return errors.New("key conflict detected during merge")
			default:
				return errors.New("invalid merge strategy")
			}
		} else {
			// No conflict, insert the item
			if !sl.insertUnsafe(item) {
				return errors.New("failed to insert item during merge")
			}
		}
	}

	return nil
}

// deleteUnsafe is an internal version of Delete that assumes locks are already held
func (sl *ZeroCopySkiplist[T, K]) deleteUnsafe(key K) bool {
	// Find the node to delete
	update, current := sl.search(key)

	// Check if key exists
	if current == nil || sl.cmpKey(current.key, key) != 0 {
		return false // Key not found
	}

	// Remove node from all levels
	for i := 0; i < len(current.forward); i++ {
		update[i].forward[i] = current.forward[i]
	}

	// Update backward pointer of the next node
	if current.forward[0] != nil {
		current.forward[0].backward = current.backward
	}

	// Reduce skiplist level if necessary
	for sl.level > 0 && sl.header.forward[sl.level] == nil {
		sl.level--
	}

	sl.length--
	return true
}

// insertUnsafe is an internal version of Insert that assumes locks are already held
func (sl *ZeroCopySkiplist[T, K]) insertUnsafe(item *T) bool {
	if item == nil {
		return false
	}

	// Extract the key from the item
	key := sl.getKeyFromItem(item)

	// Find insertion point
	update, current := sl.search(key)

	// Check if key already exists
	if current != nil && sl.cmpKey(current.key, key) == 0 {
		return false // Key already exists
	}

	// Generate random level for new node
	newLevel := sl.randomLevel()

	// Update skiplist level if necessary
	if newLevel > sl.level {
		for i := sl.level + 1; i <= newLevel; i++ {
			update[i] = sl.header
		}
		sl.level = newLevel
	}

	// Create new node
	newNode := &ItemPtr[T, K]{
		item:    item,
		key:     key,
		forward: make([]*ItemPtr[T, K], newLevel+1),
	}

	// Update forward pointers
	for i := 0; i <= newLevel; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	// Update backward pointers for double linking
	if newNode.forward[0] != nil {
		newNode.forward[0].backward = newNode
	}
	if update[0] != sl.header {
		newNode.backward = update[0]
	}

	sl.length++
	return true
}

// Length returns the number of items in the skiplist.
// This operation is thread-safe and acquires a read lock.
func (sl *ZeroCopySkiplist[T, K]) Length() int {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.length
}

// IsEmpty returns true if the skiplist contains no items.
// This operation is thread-safe and acquires a read lock.
func (sl *ZeroCopySkiplist[T, K]) IsEmpty() bool {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.length == 0
}

// First returns a pointer to the first ItemPtr in the skiplist (smallest key).
// Returns nil if the skiplist is empty.
// This operation is thread-safe and acquires a read lock.
func (sl *ZeroCopySkiplist[T, K]) First() *ItemPtr[T, K] {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.header.forward[0]
}

// Last returns a pointer to the last ItemPtr in the skiplist (largest key).
// Returns nil if the skiplist is empty.
// This operation is thread-safe and acquires a read lock.
func (sl *ZeroCopySkiplist[T, K]) Last() *ItemPtr[T, K] {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	if sl.length == 0 {
		return nil
	}

	current := sl.header
	for i := sl.level; i >= 0; i-- {
		for current.forward[i] != nil {
			current = current.forward[i]
		}
	}
	return current
}

// Find searches for an item with the given key.
// Returns the ItemPtr if found, nil otherwise.
// This operation is thread-safe and acquires a read lock.
func (sl *ZeroCopySkiplist[T, K]) Find(key K) *ItemPtr[T, K] {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	_, current := sl.search(key)
	if current != nil && sl.cmpKey(current.key, key) == 0 {
		return current
	}
	return nil
}

// Copy creates a deep copy of the skiplist structure while preserving
// references to the original items. The new skiplist will have the same
// configuration and will point to the same items as the original, but
// will have completely independent skiplist nodes and structure.
//
// This operation is thread-safe and acquires a read lock during copying.
//
// Returns a new ZeroCopySkiplist with the same items but independent structure.
func (sl *ZeroCopySkiplist[T, K]) Copy() *ZeroCopySkiplist[T, K] {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	// Create new skiplist with same configuration
	newSl := MakeZeroCopySkiplist(sl.maxLevel, sl.getKeyFromItem, sl.getItemSize, sl.cmpKey)

	// If original is empty, return empty copy
	if sl.length == 0 {
		return newSl
	}

	// Traverse original skiplist and insert all items into new skiplist
	for current := sl.header.forward[0]; current != nil; current = current.forward[0] {
		newSl.Insert(current.item)
	}

	return newSl
}

// ToPwritevSlice creates an ordered list of byte slices suitable for Pwritev()
// to write all items in the skiplist to disk in sorted order.
//
// The function takes a serializer function that converts each item to its
// byte representation. The returned slice contains byte slices for each item,
// ordered by the skiplist's sort order.
//
// This operation is thread-safe and acquires a read lock during traversal.
//
// Parameters:
//   - getItemBytes: function that takes an item pointer and returns its byte representation.
//     The returned byte slice must remain valid until the Pwritev() operation completes.
//
// Returns a slice of byte slices ordered by skiplist sort order, suitable for Pwritev().
//
// Example usage:
//
//	buffers := skiplist.ToPwritevSlice(func(item *MyStruct) []byte {
//	    size := int(unsafe.Sizeof(*item))
//	    return (*[1024]byte)(unsafe.Pointer(item))[:size:size]
//	})
//	n, err := unix.Pwritev(fd, buffers, offset)
func (sl *ZeroCopySkiplist[T, K]) ToPwritevSlice(getItemBytes func(*T) []byte) [][]byte {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	// Pre-allocate slice with known capacity
	buffers := make([][]byte, 0, sl.length)

	// Traverse skiplist in order and create byte slice for each item
	for current := sl.header.forward[0]; current != nil; current = current.forward[0] {
		data := getItemBytes(current.item)
		if len(data) > 0 {
			buffers = append(buffers, data)
		}
	}

	return buffers
}

// ToPwritevSliceRaw creates an ordered list of byte slices suitable for Pwritev()
// using the skiplist's built-in getItemSize function to create raw byte representations
// of each item. This is useful for writing items directly as binary data.
//
// This operation is thread-safe and acquires a read lock during traversal.
//
// Returns a slice of byte slices ordered by skiplist sort order, suitable for Pwritev().
// Each byte slice represents the raw memory of the item as determined by getItemSize.
//
// Example usage:
//
//	buffers := skiplist.ToPwritevSliceRaw()
//	n, err := unix.Pwritev(fd, buffers, offset)
func (sl *ZeroCopySkiplist[T, K]) ToPwritevSliceRaw() [][]byte {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	// Pre-allocate slice with known capacity
	buffers := make([][]byte, 0, sl.length)

	// Traverse skiplist in order and create byte slice for each item
	for current := sl.header.forward[0]; current != nil; current = current.forward[0] {
		size := sl.getItemSize(current.item)
		if size > 0 {
			// Create byte slice pointing directly to item memory
			data := (*[1 << 30]byte)(unsafe.Pointer(current.item))[:size:size]
			buffers = append(buffers, data)
		}
	}

	return buffers
}

// Item returns the pointer to the actual data structure.
func (ip *ItemPtr[T, K]) Item() *T {
	if ip == nil {
		return nil
	}
	return ip.item
}

// Key returns the cached key for this item.
func (ip *ItemPtr[T, K]) Key() K {
	return ip.key
}

// Next returns the next ItemPtr in the skiplist order.
// Returns nil if this is the last item.
func (ip *ItemPtr[T, K]) Next() *ItemPtr[T, K] {
	if ip == nil {
		return nil
	}
	return ip.forward[0]
}

// Prev returns the previous ItemPtr in the skiplist order.
// Returns nil if this is the first item.
func (ip *ItemPtr[T, K]) Prev() *ItemPtr[T, K] {
	if ip == nil {
		return nil
	}
	return ip.backward
}
