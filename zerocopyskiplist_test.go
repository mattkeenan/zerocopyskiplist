package zerocopyskiplist

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Test data structures
type TestItem struct {
	ID   int
	Name string
	Data []byte
}

type StringItem struct {
	Key   string
	Value int
}

type LargeTestItem struct {
	Key  string
	ID   int
	Data string
}

// Helper functions for testing
func getIntKey(item *TestItem) int {
	return item.ID
}

func getIntItemSize(item *TestItem) int {
	return int(unsafe.Sizeof(*item))
}

func cmpInt(a, b int) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func getStringKey(item *StringItem) string {
	return item.Key
}

func getStringItemSize(item *StringItem) int {
	return int(unsafe.Sizeof(*item))
}

func getLargeTestKey(item *LargeTestItem) string {
	return item.Key
}

func getLargeTestItemSize(item *LargeTestItem) int {
	return int(unsafe.Sizeof(*item))
}

func cmpString(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// generateDeterministicKey converts a number to a deterministic string key
// 0 => "a", 1 => "b", ..., 25 => "z", 26 => "aa", 27 => "ab", etc.
func generateDeterministicKey(n int) string {
	if n < 0 {
		return ""
	}

	var result []byte
	for {
		result = append([]byte{byte('a' + (n % 26))}, result...)
		n = n / 26
		if n == 0 {
			break
		}
		n-- // Adjust for 1-indexed base-26
	}
	return string(result)
}

// TestMakeZeroCopySkiplist tests the constructor with various parameters
func TestMakeZeroCopySkiplist(t *testing.T) {
	tests := []struct {
		name     string
		maxLevel int
		expected int
	}{
		{"Valid maxLevel", 10, 10},
		{"Zero maxLevel", 0, 16},      // should default to 16
		{"Negative maxLevel", -5, 16}, // should default to 16
		{"Large maxLevel", 100, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sl := makeZeroCopySkiplist(tt.maxLevel, getIntKey, getIntItemSize, cmpInt)

			if sl == nil {
				t.Fatal("Expected non-nil skiplist")
			}
			if sl.maxLevel != tt.expected {
				t.Errorf("Expected maxLevel %d, got %d", tt.expected, sl.maxLevel)
			}
			if sl.length != 0 {
				t.Errorf("Expected initial length 0, got %d", sl.length)
			}
			if !sl.IsEmpty() {
				t.Error("Expected skiplist to be empty initially")
			}
		})
	}
}

// TestInsertBasic tests basic insertion functionality
func TestInsertBasic(t *testing.T) {
	sl := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

	// Test inserting nil item
	if sl.Insert(nil) {
		t.Error("Expected Insert(nil) to return false")
	}

	// Create test items
	items := []*TestItem{
		{ID: 5, Name: "five"},
		{ID: 2, Name: "two"},
		{ID: 8, Name: "eight"},
		{ID: 1, Name: "one"},
		{ID: 9, Name: "nine"},
	}

	// Insert items
	for _, item := range items {
		if !sl.Insert(item) {
			t.Errorf("Failed to insert item with ID %d", item.ID)
		}
	}

	// Check length
	if sl.Length() != len(items) {
		t.Errorf("Expected length %d, got %d", len(items), sl.Length())
	}

	// Check that skiplist is no longer empty
	if sl.IsEmpty() {
		t.Error("Expected skiplist to not be empty after insertions")
	}
}

// TestInsertDuplicates tests handling of duplicate keys
func TestInsertDuplicates(t *testing.T) {
	sl := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

	item1 := &TestItem{ID: 5, Name: "first"}
	item2 := &TestItem{ID: 5, Name: "second"}

	// First insert should succeed
	if !sl.Insert(item1) {
		t.Error("First insert should succeed")
	}

	// Second insert with same key should fail
	if sl.Insert(item2) {
		t.Error("Second insert with duplicate key should fail")
	}

	// Length should still be 1
	if sl.Length() != 1 {
		t.Errorf("Expected length 1, got %d", sl.Length())
	}

	// The original item should still be there
	found := sl.Find(5)
	if found == nil {
		t.Fatal("Expected to find item with key 5")
	}
	if found.Item().Name != "first" {
		t.Errorf("Expected original item to remain, got %s", found.Item().Name)
	}
}

// TestFind tests the Find functionality
func TestFind(t *testing.T) {
	sl := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

	// Test finding in empty skiplist
	if found := sl.Find(5); found != nil {
		t.Error("Expected Find to return nil for empty skiplist")
	}

	// Insert test items
	items := []*TestItem{
		{ID: 3, Name: "three"},
		{ID: 1, Name: "one"},
		{ID: 7, Name: "seven"},
		{ID: 5, Name: "five"},
	}

	for _, item := range items {
		sl.Insert(item)
	}

	// Test finding existing items
	for _, item := range items {
		found := sl.Find(item.ID)
		if found == nil {
			t.Errorf("Expected to find item with ID %d", item.ID)
			continue
		}
		if found.Key() != item.ID {
			t.Errorf("Expected key %d, got %d", item.ID, found.Key())
		}
		if found.Item().Name != item.Name {
			t.Errorf("Expected name %s, got %s", item.Name, found.Item().Name)
		}
	}

	// Test finding non-existing item
	if found := sl.Find(99); found != nil {
		t.Error("Expected Find to return nil for non-existing key")
	}
}

// TestFirstLast tests First and Last functionality
func TestFirstLast(t *testing.T) {
	sl := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

	// Test empty skiplist
	if first := sl.First(); first != nil {
		t.Error("Expected First() to return nil for empty skiplist")
	}
	if last := sl.Last(); last != nil {
		t.Error("Expected Last() to return nil for empty skiplist")
	}

	// Insert items in random order
	keys := []int{5, 2, 8, 1, 9, 3, 7}
	for _, key := range keys {
		item := &TestItem{ID: key, Name: fmt.Sprintf("item-%d", key)}
		sl.Insert(item)
	}

	// Check first item (should be smallest key)
	first := sl.First()
	if first == nil {
		t.Fatal("Expected First() to return non-nil")
	}
	if first.Key() != 1 {
		t.Errorf("Expected first key to be 1, got %d", first.Key())
	}

	// Check last item (should be largest key)
	last := sl.Last()
	if last == nil {
		t.Fatal("Expected Last() to return non-nil")
	}
	if last.Key() != 9 {
		t.Errorf("Expected last key to be 9, got %d", last.Key())
	}
}

// TestNavigation tests Next and Prev functionality
func TestNavigation(t *testing.T) {
	sl := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

	// Insert items
	keys := []int{1, 3, 5, 7, 9}
	for _, key := range keys {
		item := &TestItem{ID: key, Name: fmt.Sprintf("item-%d", key)}
		sl.Insert(item)
	}

	// Test forward navigation
	current := sl.First()
	for i, expectedKey := range keys {
		if current == nil {
			t.Fatalf("Expected non-nil node at position %d", i)
		}
		if current.Key() != expectedKey {
			t.Errorf("Expected key %d at position %d, got %d", expectedKey, i, current.Key())
		}
		current = current.Next()
	}

	// Last Next() should return nil
	if current != nil {
		t.Error("Expected nil after last item")
	}

	// Test backward navigation
	current = sl.Last()
	for i := len(keys) - 1; i >= 0; i-- {
		expectedKey := keys[i]
		if current == nil {
			t.Fatalf("Expected non-nil node at position %d", i)
		}
		if current.Key() != expectedKey {
			t.Errorf("Expected key %d at position %d, got %d", expectedKey, i, current.Key())
		}
		current = current.Prev()
	}

	// Last Prev() should return nil
	if current != nil {
		t.Error("Expected nil before first item")
	}
}

// TestOrdering tests that items are properly ordered
func TestOrdering(t *testing.T) {
	sl := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

	// Insert random keys
	rand.Seed(time.Now().UnixNano())
	keys := make([]int, 20)
	for i := range keys {
		keys[i] = rand.Intn(1000)
	}

	// Insert items
	for _, key := range keys {
		item := &TestItem{ID: key, Name: fmt.Sprintf("item-%d", key)}
		sl.Insert(item)
	}

	// Collect keys in skiplist order
	var orderedKeys []int
	for current := sl.First(); current != nil; current = current.Next() {
		orderedKeys = append(orderedKeys, current.Key())
	}

	// Remove duplicates and sort original keys for comparison
	uniqueKeys := make(map[int]bool)
	for _, key := range keys {
		uniqueKeys[key] = true
	}
	var expectedKeys []int
	for key := range uniqueKeys {
		expectedKeys = append(expectedKeys, key)
	}
	sort.Ints(expectedKeys)

	// Compare
	if len(orderedKeys) != len(expectedKeys) {
		t.Fatalf("Expected %d unique keys, got %d", len(expectedKeys), len(orderedKeys))
	}

	for i, key := range orderedKeys {
		if key != expectedKeys[i] {
			t.Errorf("Keys not in order at position %d: expected %d, got %d", i, expectedKeys[i], key)
		}
	}
}

// TestStringKeys tests skiplist with string keys
func TestStringKeys(t *testing.T) {
	sl := makeZeroCopySkiplist(16, getStringKey, getStringItemSize, cmpString)

	items := []*StringItem{
		{Key: "delta", Value: 4},
		{Key: "alpha", Value: 1},
		{Key: "gamma", Value: 3},
		{Key: "beta", Value: 2},
	}

	// Insert items
	for _, item := range items {
		if !sl.Insert(item) {
			t.Errorf("Failed to insert item with key %s", item.Key)
		}
	}

	// Check ordering
	expectedOrder := []string{"alpha", "beta", "delta", "gamma"}
	i := 0
	for current := sl.First(); current != nil; current = current.Next() {
		if i >= len(expectedOrder) {
			t.Error("More items than expected")
			break
		}
		if current.Key() != expectedOrder[i] {
			t.Errorf("Expected key %s at position %d, got %s", expectedOrder[i], i, current.Key())
		}
		i++
	}

	if i != len(expectedOrder) {
		t.Errorf("Expected %d items, got %d", len(expectedOrder), i)
	}
}

// TestSingleItem tests skiplist with a single item
func TestSingleItem(t *testing.T) {
	sl := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

	item := &TestItem{ID: 42, Name: "answer"}
	sl.Insert(item)

	// First and last should be the same
	first := sl.First()
	last := sl.Last()

	if first != last {
		t.Error("First and last should be the same for single item")
	}

	if first.Key() != 42 {
		t.Errorf("Expected key 42, got %d", first.Key())
	}

	// Navigation should return nil
	if first.Next() != nil {
		t.Error("Expected Next() to return nil for single item")
	}
	if first.Prev() != nil {
		t.Error("Expected Prev() to return nil for single item")
	}
}

// TestLargeDataset tests performance with larger dataset using string keys
func TestLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test in short mode")
	}

	sl := makeZeroCopySkiplist(20, getLargeTestKey, getLargeTestItemSize, cmpString)

	// Insert 1,000,000 items with deterministic string keys
	n := 1000000
	t.Logf("Creating %d items with deterministic string keys...", n)

	items := make([]*LargeTestItem, n)
	for i := 0; i < n; i++ {
		key := generateDeterministicKey(i)
		items[i] = &LargeTestItem{
			Key:  key,
			ID:   i,
			Data: fmt.Sprintf("data-for-%s", key),
		}
	}

	// Measure insertion time
	t.Log("Starting insertions...")
	start := time.Now()
	inserted := 0
	for _, item := range items {
		if sl.Insert(item) {
			inserted++
		}
	}
	insertTime := time.Since(start)

	t.Logf("Inserted %d items in %v (%.2f items/sec)", inserted, insertTime, float64(inserted)/insertTime.Seconds())

	// Verify length
	if sl.Length() != inserted {
		t.Errorf("Expected length %d, got %d", inserted, sl.Length())
	}

	// Test some searches to verify data integrity
	t.Log("Testing search operations...")
	searchCount := 1000
	found := 0
	start = time.Now()
	for i := 0; i < searchCount; i++ {
		// Search for deterministic keys
		testIndex := (i * 997) % n // Use prime number to get good distribution
		expectedKey := generateDeterministicKey(testIndex)
		if result := sl.Find(expectedKey); result != nil {
			found++
			// Verify the data is correct
			if result.Item().ID != testIndex {
				t.Errorf("Key %s: expected ID %d, got %d", expectedKey, testIndex, result.Item().ID)
			}
			if result.Item().Key != expectedKey {
				t.Errorf("Key %s: expected key %s, got %s", expectedKey, expectedKey, result.Item().Key)
			}
		}
	}
	searchTime := time.Since(start)

	t.Logf("Performed %d searches in %v, found %d items (%.2f searches/sec)",
		searchCount, searchTime, found, float64(searchCount)/searchTime.Seconds())

	if found != searchCount {
		t.Errorf("Expected to find all %d searched items, found %d", searchCount, found)
	}

	// Test multiple deletions
	t.Log("Testing deletions...")
	deleteIndices := []int{
		0,      // First item (key "a")
		1,      // Second item (key "b")
		100,    // Middle item
		999,    // Another middle item
		n - 1,  // Last item
		n / 2,  // Middle item
		12345,  // Random item
		56789,  // Another random item
		777777, // Another random item
		123456, // Another random item
	}

	deletedKeys := make([]string, 0, len(deleteIndices))
	successfulDeletes := 0

	start = time.Now()
	for _, idx := range deleteIndices {
		if idx >= n {
			continue // Skip invalid indices
		}
		key := generateDeterministicKey(idx)
		deletedKeys = append(deletedKeys, key)
		if sl.Delete(key) {
			successfulDeletes++
		} else {
			t.Errorf("Failed to delete key %s (index %d)", key, idx)
		}
	}
	deleteTime := time.Since(start)

	t.Logf("Deleted %d items in %v", successfulDeletes, deleteTime)

	// Verify deleted items no longer exist
	t.Log("Verifying deleted items are gone...")
	for _, key := range deletedKeys {
		if found := sl.Find(key); found != nil {
			t.Errorf("Deleted key %s should not be found, but was found", key)
		}
	}

	// Verify length is correct after deletions
	expectedLength := inserted - successfulDeletes
	if sl.Length() != expectedLength {
		t.Errorf("After deletions: expected length %d, got %d", expectedLength, sl.Length())
	}

	// Count items by iterating through the list using Next()
	t.Log("Counting items by iteration...")
	start = time.Now()
	iterationCount := 0
	prev := ""
	for current := sl.First(); current != nil; current = current.Next() {
		iterationCount++

		// Verify ordering (strings should be in lexicographical order)
		currentKey := current.Key()
		if prev != "" && cmpString(prev, currentKey) >= 0 {
			t.Errorf("Ordering violation: prev=%s, current=%s", prev, currentKey)
		}
		prev = currentKey

		// Verify this key was not deleted
		keyFound := false
		for _, deletedKey := range deletedKeys {
			if currentKey == deletedKey {
				t.Errorf("Found deleted key %s during iteration", currentKey)
				keyFound = true
				break
			}
		}
		if keyFound {
			continue
		}

		// Spot check: verify some items have correct data
		if iterationCount%100000 == 0 {
			expectedData := fmt.Sprintf("data-for-%s", currentKey)
			if current.Item().Data != expectedData {
				t.Errorf("Key %s: expected data %s, got %s", currentKey, expectedData, current.Item().Data)
			}
		}
	}
	iterationTime := time.Since(start)

	t.Logf("Counted %d items by iteration in %v", iterationCount, iterationTime)

	// Verify iteration count matches Length()
	if iterationCount != sl.Length() {
		t.Errorf("Iteration count %d does not match Length() %d", iterationCount, sl.Length())
	}

	// Verify iteration count matches expected count
	if iterationCount != expectedLength {
		t.Errorf("Iteration count %d does not match expected length %d", iterationCount, expectedLength)
	}

	// Test backward iteration for a sample
	t.Log("Testing backward iteration...")
	backwardCount := 0
	maxBackwardTest := 10000 // Only test first 10k items in reverse to save time
	prev = ""
	for current := sl.Last(); current != nil && backwardCount < maxBackwardTest; current = current.Prev() {
		backwardCount++
		currentKey := current.Key()

		// Verify reverse ordering
		if prev != "" && cmpString(currentKey, prev) >= 0 {
			t.Errorf("Reverse ordering violation: current=%s, prev=%s", currentKey, prev)
		}
		prev = currentKey
	}

	t.Logf("Verified backward iteration for %d items", backwardCount)

	t.Logf("Large dataset test completed successfully:")
	t.Logf("  - Inserted: %d items", inserted)
	t.Logf("  - Deleted: %d items", successfulDeletes)
	t.Logf("  - Final count: %d items", iterationCount)
	t.Logf("  - All counts consistent: Length()=%d, Iteration=%d", sl.Length(), iterationCount)
}

// BenchmarkInsert benchmarks insertion performance
func BenchmarkInsert(b *testing.B) {
	sl := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)
	items := make([]*TestItem, b.N)

	for i := 0; i < b.N; i++ {
		items[i] = &TestItem{ID: i, Name: fmt.Sprintf("item-%d", i)}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sl.Insert(items[i])
	}
}

// BenchmarkFind benchmarks search performance
func BenchmarkFind(b *testing.B) {
	sl := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

	// Pre-populate with 10,000 items
	for i := 0; i < 10000; i++ {
		item := &TestItem{ID: i, Name: fmt.Sprintf("item-%d", i)}
		sl.Insert(item)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sl.Find(i % 10000)
	}
}

// BenchmarkDelete benchmarks deletion performance
func BenchmarkDelete(b *testing.B) {
	sl := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

	// Pre-populate with items
	for i := 0; i < b.N; i++ {
		item := &TestItem{ID: i, Name: fmt.Sprintf("item-%d", i)}
		sl.Insert(item)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sl.Delete(i)
	}
}

// BenchmarkMerge benchmarks merge performance
func BenchmarkMerge(b *testing.B) {
	// Create two skiplists with non-overlapping keys for fair benchmark
	sl1 := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)
	sl2 := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

	// Populate first skiplist with even numbers
	for i := 0; i < b.N; i += 2 {
		item := &TestItem{ID: i, Name: fmt.Sprintf("item-%d", i)}
		sl1.Insert(item)
	}

	// Populate second skiplist with odd numbers
	for i := 1; i < b.N; i += 2 {
		item := &TestItem{ID: i, Name: fmt.Sprintf("item-%d", i)}
		sl2.Insert(item)
	}

	b.ResetTimer()
	err := sl1.Merge(sl2, MergeTheirs)
	if err != nil {
		b.Fatalf("Merge failed: %v", err)
	}
}

// TestConcurrentAccess tests thread safety with concurrent operations
func TestConcurrentAccess(t *testing.T) {
	sl := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

	// Number of goroutines and operations
	numWriters := 5
	numReaders := 10
	itemsPerWriter := 100

	// Create items for each writer
	allItems := make([][]*TestItem, numWriters)
	for w := 0; w < numWriters; w++ {
		allItems[w] = make([]*TestItem, itemsPerWriter)
		for i := 0; i < itemsPerWriter; i++ {
			allItems[w][i] = &TestItem{
				ID:   w*itemsPerWriter + i,
				Name: fmt.Sprintf("writer-%d-item-%d", w, i),
			}
		}
	}

	// Use WaitGroup to coordinate goroutines
	var wg sync.WaitGroup

	// Start writer goroutines
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for _, item := range allItems[writerID] {
				sl.Insert(item)
				// Small delay to increase chance of concurrent access
				time.Sleep(time.Microsecond)
			}
		}(w)
	}

	// Start reader goroutines
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for i := 0; i < itemsPerWriter; i++ {
				// Read operations
				_ = sl.Length()
				_ = sl.IsEmpty()
				_ = sl.First()
				_ = sl.Last()

				// Search for random keys
				key := rand.Intn(numWriters * itemsPerWriter)
				_ = sl.Find(key)

				// Create copies
				if i%10 == 0 { // Only occasionally to avoid too much overhead
					_ = sl.Copy()
				}

				time.Sleep(time.Microsecond)
			}
		}(r)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Verify final state
	expectedLength := numWriters * itemsPerWriter
	if sl.Length() != expectedLength {
		t.Errorf("Expected final length %d, got %d", expectedLength, sl.Length())
	}

	// Verify all items are present and ordered
	count := 0
	prev := -1
	for current := sl.First(); current != nil; current = current.Next() {
		if current.Key() <= prev {
			t.Errorf("Items not in order: prev=%d, current=%d", prev, current.Key())
		}
		prev = current.Key()
		count++
	}

	if count != expectedLength {
		t.Errorf("Expected to traverse %d items, got %d", expectedLength, count)
	}
}

// TestCopy tests the Copy functionality
func TestCopy(t *testing.T) {
	original := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

	// Test copying empty skiplist
	emptyCopy := original.Copy()
	if !emptyCopy.IsEmpty() {
		t.Error("Expected copy of empty skiplist to be empty")
	}
	if emptyCopy.Length() != 0 {
		t.Errorf("Expected copy length 0, got %d", emptyCopy.Length())
	}

	// Insert items into original
	items := []*TestItem{
		{ID: 5, Name: "five"},
		{ID: 2, Name: "two"},
		{ID: 8, Name: "eight"},
		{ID: 1, Name: "one"},
		{ID: 9, Name: "nine"},
	}

	for _, item := range items {
		original.Insert(item)
	}

	// Create copy
	copy := original.Copy()

	// Verify copy has same length
	if copy.Length() != original.Length() {
		t.Errorf("Expected copy length %d, got %d", original.Length(), copy.Length())
	}

	// Verify copy has same items in same order
	origCurrent := original.First()
	copyCurrent := copy.First()

	for origCurrent != nil && copyCurrent != nil {
		if origCurrent.Key() != copyCurrent.Key() {
			t.Errorf("Keys don't match: original %d, copy %d", origCurrent.Key(), copyCurrent.Key())
		}

		// Verify they point to the same underlying item (zero-copy)
		if origCurrent.Item() != copyCurrent.Item() {
			t.Error("Expected copy to point to same underlying items (zero-copy)")
		}

		// Verify they are different ItemPtr instances (deep copy of structure)
		if origCurrent == copyCurrent {
			t.Error("Expected different ItemPtr instances (deep copy of structure)")
		}

		origCurrent = origCurrent.Next()
		copyCurrent = copyCurrent.Next()
	}

	// Both should reach end at same time
	if origCurrent != nil || copyCurrent != nil {
		t.Error("Copy and original have different number of items")
	}

	// Verify independence: insert into copy shouldn't affect original
	newItem := &TestItem{ID: 10, Name: "ten"}
	copy.Insert(newItem)

	if copy.Length() != original.Length()+1 {
		t.Error("Expected copy to be independent after insertion")
	}

	if original.Find(10) != nil {
		t.Error("Expected original to be unaffected by copy modification")
	}

	if copy.Find(10) == nil {
		t.Error("Expected copy to contain newly inserted item")
	}
}

// TestToPwritevSlice tests the ToPwritevSlice functionality
func TestToPwritevSlice(t *testing.T) {
	sl := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

	// Test empty skiplist
	emptyBuffers := sl.ToPwritevSlice(func(item *TestItem) []byte {
		size := int(unsafe.Sizeof(*item))
		return (*[1024]byte)(unsafe.Pointer(item))[:size:size]
	})
	if len(emptyBuffers) != 0 {
		t.Errorf("Expected 0 buffers for empty skiplist, got %d", len(emptyBuffers))
	}

	// Insert test items in random order
	items := []*TestItem{
		{ID: 5, Name: "five"},
		{ID: 2, Name: "two"},
		{ID: 8, Name: "eight"},
		{ID: 1, Name: "one"},
		{ID: 9, Name: "nine"},
	}

	for _, item := range items {
		sl.Insert(item)
	}

	// Get buffer list
	buffers := sl.ToPwritevSlice(func(item *TestItem) []byte {
		size := int(unsafe.Sizeof(*item))
		return (*[1024]byte)(unsafe.Pointer(item))[:size:size]
	})

	// Verify we got the right number of buffers
	if len(buffers) != len(items) {
		t.Errorf("Expected %d buffers, got %d", len(items), len(buffers))
	}

	// Verify buffers are in sorted order by checking the underlying data
	expectedOrder := []int{1, 2, 5, 8, 9}
	for i, buffer := range buffers {
		// Convert buffer back to TestItem pointer to verify ordering
		itemPtr := (*TestItem)(unsafe.Pointer(&buffer[0]))
		if itemPtr.ID != expectedOrder[i] {
			t.Errorf("Buffer %d: expected ID %d, got %d", i, expectedOrder[i], itemPtr.ID)
		}

		// Verify buffer length
		expectedSize := int(unsafe.Sizeof(TestItem{}))
		if len(buffer) != expectedSize {
			t.Errorf("Buffer %d: expected length %d, got %d", i, expectedSize, len(buffer))
		}
	}
}

// TestToPwritevSliceWithCustomSerializer tests ToPwritevSlice with custom serialization
func TestToPwritevSliceWithCustomSerializer(t *testing.T) {
	sl := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

	// Insert items
	items := []*TestItem{
		{ID: 3, Name: "three"},
		{ID: 1, Name: "one"},
		{ID: 2, Name: "two"},
	}

	for _, item := range items {
		sl.Insert(item)
	}

	// Custom serializer that returns only the ID as bytes
	buffers := sl.ToPwritevSlice(func(item *TestItem) []byte {
		// Create a byte slice containing just the ID
		return (*[4]byte)(unsafe.Pointer(&item.ID))[:]
	})

	// Verify results
	if len(buffers) != 3 {
		t.Fatalf("Expected 3 buffers, got %d", len(buffers))
	}

	// Check that buffers contain the IDs in sorted order
	expectedIDs := []int{1, 2, 3}
	for i, buffer := range buffers {
		if len(buffer) != 4 {
			t.Errorf("Buffer %d: expected length 4, got %d", i, len(buffer))
		}

		// Read the ID from the buffer data
		id := *(*int)(unsafe.Pointer(&buffer[0]))
		if id != expectedIDs[i] {
			t.Errorf("Buffer %d: expected ID %d, got %d", i, expectedIDs[i], id)
		}
	}
}

// TestToPwritevSliceZeroSize tests ToPwritevSlice with zero-size serialization
func TestToPwritevSliceZeroSize(t *testing.T) {
	sl := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

	item := &TestItem{ID: 5, Name: "test"}
	sl.Insert(item)

	// Serializer that returns zero-length slice (should be skipped)
	buffers := sl.ToPwritevSlice(func(item *TestItem) []byte {
		return []byte{}
	})

	// Should get no buffers since length is 0
	if len(buffers) != 0 {
		t.Errorf("Expected 0 buffers for zero-size serialization, got %d", len(buffers))
	}
}

// TestMerge tests the Merge functionality with different strategies
func TestMerge(t *testing.T) {
	// Test MergeTheirs strategy
	t.Run("MergeTheirs", func(t *testing.T) {
		sl1 := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)
		sl2 := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

		// Add items to first skiplist
		items1 := []*TestItem{
			{ID: 1, Name: "one-old"},
			{ID: 3, Name: "three-old"},
			{ID: 5, Name: "five-old"},
		}
		for _, item := range items1 {
			sl1.Insert(item)
		}

		// Add items to second skiplist (some overlapping keys)
		items2 := []*TestItem{
			{ID: 2, Name: "two-new"},
			{ID: 3, Name: "three-new"}, // conflict
			{ID: 4, Name: "four-new"},
			{ID: 5, Name: "five-new"}, // conflict
		}
		for _, item := range items2 {
			sl2.Insert(item)
		}

		// Merge using MergeTheirs strategy
		err := sl1.Merge(sl2, MergeTheirs)
		if err != nil {
			t.Fatalf("Merge failed: %v", err)
		}

		// Verify results
		expectedItems := map[int]string{
			1: "one-old",   // from sl1, no conflict
			2: "two-new",   // from sl2, no conflict
			3: "three-new", // from sl2, theirs wins
			4: "four-new",  // from sl2, no conflict
			5: "five-new",  // from sl2, theirs wins
		}

		if sl1.Length() != len(expectedItems) {
			t.Errorf("Expected length %d, got %d", len(expectedItems), sl1.Length())
		}

		for key, expectedName := range expectedItems {
			found := sl1.Find(key)
			if found == nil {
				t.Errorf("Expected to find key %d", key)
				continue
			}
			if found.Item().Name != expectedName {
				t.Errorf("Key %d: expected name %s, got %s", key, expectedName, found.Item().Name)
			}
		}

		// Verify sl2 is unchanged
		if sl2.Length() != len(items2) {
			t.Errorf("Second skiplist should be unchanged, expected length %d, got %d", len(items2), sl2.Length())
		}
	})

	// Test MergeOurs strategy
	t.Run("MergeOurs", func(t *testing.T) {
		sl1 := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)
		sl2 := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

		// Add items to first skiplist
		items1 := []*TestItem{
			{ID: 1, Name: "one-old"},
			{ID: 3, Name: "three-old"},
		}
		for _, item := range items1 {
			sl1.Insert(item)
		}

		// Add items to second skiplist
		items2 := []*TestItem{
			{ID: 2, Name: "two-new"},
			{ID: 3, Name: "three-new"}, // conflict - should keep old
		}
		for _, item := range items2 {
			sl2.Insert(item)
		}

		// Merge using MergeOurs strategy
		err := sl1.Merge(sl2, MergeOurs)
		if err != nil {
			t.Fatalf("Merge failed: %v", err)
		}

		// Verify results
		expectedItems := map[int]string{
			1: "one-old",   // from sl1
			2: "two-new",   // from sl2, no conflict
			3: "three-old", // from sl1, ours wins
		}

		if sl1.Length() != len(expectedItems) {
			t.Errorf("Expected length %d, got %d", len(expectedItems), sl1.Length())
		}

		for key, expectedName := range expectedItems {
			found := sl1.Find(key)
			if found == nil {
				t.Errorf("Expected to find key %d", key)
				continue
			}
			if found.Item().Name != expectedName {
				t.Errorf("Key %d: expected name %s, got %s", key, expectedName, found.Item().Name)
			}
		}
	})

	// Test MergeError strategy
	t.Run("MergeError", func(t *testing.T) {
		sl1 := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)
		sl2 := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

		// Add items to first skiplist
		item1 := &TestItem{ID: 1, Name: "one"}
		sl1.Insert(item1)

		// Add items to second skiplist with conflict
		items2 := []*TestItem{
			{ID: 1, Name: "one-conflict"}, // This should cause error
			{ID: 2, Name: "two"},
		}
		for _, item := range items2 {
			sl2.Insert(item)
		}

		// Merge using MergeError strategy should fail
		err := sl1.Merge(sl2, MergeError)
		if err == nil {
			t.Error("Expected merge to fail with MergeError strategy")
		}

		// Original skiplist should be unchanged since error occurred immediately
		if sl1.Length() != 1 {
			t.Errorf("Expected original length 1, got %d", sl1.Length())
		}

		found := sl1.Find(1)
		if found == nil || found.Item().Name != "one" {
			t.Error("Original item should be unchanged")
		}
	})
}

// TestMergeEdgeCases tests edge cases for Merge functionality
func TestMergeEdgeCases(t *testing.T) {
	// Test merging with nil skiplist
	t.Run("MergeNil", func(t *testing.T) {
		sl := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

		err := sl.Merge(nil, MergeTheirs)
		if err != nil {
			t.Errorf("Merging with nil should succeed, got error: %v", err)
		}
	})

	// Test merging empty skiplists
	t.Run("MergeEmpty", func(t *testing.T) {
		sl1 := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)
		sl2 := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

		err := sl1.Merge(sl2, MergeTheirs)
		if err != nil {
			t.Errorf("Merging empty skiplists should succeed, got error: %v", err)
		}

		if !sl1.IsEmpty() {
			t.Error("Result should be empty")
		}
	})

	// Test merging into empty skiplist
	t.Run("MergeIntoEmpty", func(t *testing.T) {
		sl1 := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)
		sl2 := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

		// Add items only to second skiplist
		items := []*TestItem{
			{ID: 1, Name: "one"},
			{ID: 2, Name: "two"},
		}
		for _, item := range items {
			sl2.Insert(item)
		}

		err := sl1.Merge(sl2, MergeTheirs)
		if err != nil {
			t.Fatalf("Merge failed: %v", err)
		}

		if sl1.Length() != 2 {
			t.Errorf("Expected length 2, got %d", sl1.Length())
		}

		// Verify all items copied correctly
		for _, item := range items {
			found := sl1.Find(item.ID)
			if found == nil {
				t.Errorf("Expected to find key %d", item.ID)
				continue
			}
			if found.Item().Name != item.Name {
				t.Errorf("Key %d: expected name %s, got %s", item.ID, item.Name, found.Item().Name)
			}
		}
	})

	// Test merging from empty skiplist
	t.Run("MergeFromEmpty", func(t *testing.T) {
		sl1 := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)
		sl2 := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

		// Add items only to first skiplist
		item := &TestItem{ID: 1, Name: "one"}
		sl1.Insert(item)

		originalLength := sl1.Length()

		err := sl1.Merge(sl2, MergeTheirs)
		if err != nil {
			t.Fatalf("Merge failed: %v", err)
		}

		if sl1.Length() != originalLength {
			t.Errorf("Length should not change when merging from empty, expected %d, got %d", originalLength, sl1.Length())
		}
	})

	// Test merging with incompatible maxLevel
	t.Run("MergeIncompatibleMaxLevel", func(t *testing.T) {
		sl1 := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)
		sl2 := makeZeroCopySkiplist(8, getIntKey, getIntItemSize, cmpInt) // Different maxLevel

		err := sl1.Merge(sl2, MergeTheirs)
		if err == nil {
			t.Error("Expected merge to fail with incompatible maxLevel")
		}
	})

	// Test invalid merge strategy
	t.Run("InvalidMergeStrategy", func(t *testing.T) {
		sl1 := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)
		sl2 := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

		// Add an item to trigger strategy handling
		item := &TestItem{ID: 1, Name: "test"}
		sl1.Insert(item)
		sl2.Insert(item) // Create conflict

		// Use invalid strategy value
		invalidStrategy := MergeStrategy(999)
		err := sl1.Merge(sl2, invalidStrategy)
		if err == nil {
			t.Error("Expected merge to fail with invalid strategy")
		}
	})
}

// TestMergeOrdering tests that merge preserves correct ordering
func TestMergeOrdering(t *testing.T) {
	sl1 := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)
	sl2 := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

	// Add items to first skiplist (odd numbers)
	for i := 1; i <= 9; i += 2 {
		item := &TestItem{ID: i, Name: fmt.Sprintf("item-%d", i)}
		sl1.Insert(item)
	}

	// Add items to second skiplist (even numbers)
	for i := 2; i <= 10; i += 2 {
		item := &TestItem{ID: i, Name: fmt.Sprintf("item-%d", i)}
		sl2.Insert(item)
	}

	err := sl1.Merge(sl2, MergeTheirs)
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	// Verify ordering
	expectedOrder := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	i := 0
	for current := sl1.First(); current != nil; current = current.Next() {
		if i >= len(expectedOrder) {
			t.Error("Too many items in merged skiplist")
			break
		}
		if current.Key() != expectedOrder[i] {
			t.Errorf("Expected key %d at position %d, got %d", expectedOrder[i], i, current.Key())
		}
		i++
	}

	if i != len(expectedOrder) {
		t.Errorf("Expected %d items, got %d", len(expectedOrder), i)
	}
}

// TestDelete tests the Delete functionality
func TestDelete(t *testing.T) {
	sl := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

	// Test deleting from empty skiplist
	if sl.Delete(5) {
		t.Error("Expected Delete to return false for empty skiplist")
	}

	// Insert test items
	items := []*TestItem{
		{ID: 1, Name: "one"},
		{ID: 3, Name: "three"},
		{ID: 5, Name: "five"},
		{ID: 7, Name: "seven"},
		{ID: 9, Name: "nine"},
	}

	for _, item := range items {
		sl.Insert(item)
	}

	initialLength := sl.Length()
	if initialLength != len(items) {
		t.Fatalf("Expected initial length %d, got %d", len(items), initialLength)
	}

	// Test deleting non-existing key
	if sl.Delete(6) {
		t.Error("Expected Delete to return false for non-existing key")
	}
	if sl.Length() != initialLength {
		t.Error("Length should not change when deleting non-existing key")
	}

	// Test deleting existing key (middle item)
	if !sl.Delete(5) {
		t.Error("Expected Delete to return true for existing key")
	}
	if sl.Length() != initialLength-1 {
		t.Errorf("Expected length %d after deletion, got %d", initialLength-1, sl.Length())
	}

	// Verify item is actually deleted
	if found := sl.Find(5); found != nil {
		t.Error("Expected deleted item to not be found")
	}

	// Verify remaining items are still in order and accessible
	expectedRemaining := []int{1, 3, 7, 9}
	i := 0
	for current := sl.First(); current != nil; current = current.Next() {
		if i >= len(expectedRemaining) {
			t.Error("Too many items remaining")
			break
		}
		if current.Key() != expectedRemaining[i] {
			t.Errorf("Expected key %d at position %d, got %d", expectedRemaining[i], i, current.Key())
		}
		i++
	}

	if i != len(expectedRemaining) {
		t.Errorf("Expected %d remaining items, got %d", len(expectedRemaining), i)
	}
}

// TestDeleteFirstLast tests deleting first and last items
func TestDeleteFirstLast(t *testing.T) {
	sl := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

	// Insert items
	keys := []int{2, 4, 6, 8, 10}
	for _, key := range keys {
		item := &TestItem{ID: key, Name: fmt.Sprintf("item-%d", key)}
		sl.Insert(item)
	}

	// Delete first item
	if !sl.Delete(2) {
		t.Error("Failed to delete first item")
	}

	first := sl.First()
	if first == nil || first.Key() != 4 {
		t.Errorf("Expected new first item to have key 4, got %v", first)
	}

	// Delete last item
	if !sl.Delete(10) {
		t.Error("Failed to delete last item")
	}

	last := sl.Last()
	if last == nil || last.Key() != 8 {
		t.Errorf("Expected new last item to have key 8, got %v", last)
	}

	// Verify remaining items and navigation
	expectedKeys := []int{4, 6, 8}
	i := 0
	for current := sl.First(); current != nil; current = current.Next() {
		if i >= len(expectedKeys) {
			t.Error("Too many items")
			break
		}
		if current.Key() != expectedKeys[i] {
			t.Errorf("Expected key %d, got %d", expectedKeys[i], current.Key())
		}
		i++
	}

	// Test backward navigation
	i = len(expectedKeys) - 1
	for current := sl.Last(); current != nil; current = current.Prev() {
		if i < 0 {
			t.Error("Too many items in backward traversal")
			break
		}
		if current.Key() != expectedKeys[i] {
			t.Errorf("Backward: expected key %d, got %d", expectedKeys[i], current.Key())
		}
		i--
	}
}

// TestDeleteSingleItem tests deleting the only item in skiplist
func TestDeleteSingleItem(t *testing.T) {
	sl := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

	item := &TestItem{ID: 42, Name: "answer"}
	sl.Insert(item)

	if sl.Length() != 1 {
		t.Errorf("Expected length 1, got %d", sl.Length())
	}

	// Delete the only item
	if !sl.Delete(42) {
		t.Error("Failed to delete single item")
	}

	// Verify skiplist is empty
	if !sl.IsEmpty() {
		t.Error("Expected skiplist to be empty after deleting single item")
	}

	if sl.First() != nil {
		t.Error("Expected First() to return nil for empty skiplist")
	}

	if sl.Last() != nil {
		t.Error("Expected Last() to return nil for empty skiplist")
	}

	if sl.Length() != 0 {
		t.Errorf("Expected length 0, got %d", sl.Length())
	}
}

// TestDeleteAllItems tests deleting all items one by one
func TestDeleteAllItems(t *testing.T) {
	sl := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

	// Insert items
	keys := []int{1, 3, 5, 7, 9}
	for _, key := range keys {
		item := &TestItem{ID: key, Name: fmt.Sprintf("item-%d", key)}
		sl.Insert(item)
	}

	initialLength := len(keys)

	// Delete items in random order
	deleteOrder := []int{5, 1, 9, 3, 7}

	for i, key := range deleteOrder {
		if !sl.Delete(key) {
			t.Errorf("Failed to delete key %d", key)
		}

		expectedLength := initialLength - (i + 1)
		if sl.Length() != expectedLength {
			t.Errorf("After deleting %d items, expected length %d, got %d", i+1, expectedLength, sl.Length())
		}

		// Verify deleted item is not found
		if found := sl.Find(key); found != nil {
			t.Errorf("Deleted key %d should not be found", key)
		}
	}

	// Verify skiplist is empty
	if !sl.IsEmpty() {
		t.Error("Expected skiplist to be empty after deleting all items")
	}
}

// TestDeleteWithDuplicateAttempts tests multiple delete attempts on same key
func TestDeleteWithDuplicateAttempts(t *testing.T) {
	sl := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

	item := &TestItem{ID: 10, Name: "ten"}
	sl.Insert(item)

	// First delete should succeed
	if !sl.Delete(10) {
		t.Error("First delete should succeed")
	}

	// Second delete should fail
	if sl.Delete(10) {
		t.Error("Second delete should fail")
	}

	// Third delete should also fail
	if sl.Delete(10) {
		t.Error("Third delete should also fail")
	}

	if !sl.IsEmpty() {
		t.Error("Skiplist should be empty")
	}
}

// TestToPwritevSliceRaw tests the ToPwritevSliceRaw functionality
func TestToPwritevSliceRaw(t *testing.T) {
	sl := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

	// Test empty skiplist
	emptyBuffers := sl.ToPwritevSliceRaw()
	if len(emptyBuffers) != 0 {
		t.Errorf("Expected 0 buffers for empty skiplist, got %d", len(emptyBuffers))
	}

	// Insert test items in random order
	items := []*TestItem{
		{ID: 5, Name: "five"},
		{ID: 2, Name: "two"},
		{ID: 8, Name: "eight"},
		{ID: 1, Name: "one"},
		{ID: 9, Name: "nine"},
	}

	for _, item := range items {
		sl.Insert(item)
	}

	// Get buffer list using raw method
	buffers := sl.ToPwritevSliceRaw()

	// Verify we got the right number of buffers
	if len(buffers) != len(items) {
		t.Errorf("Expected %d buffers, got %d", len(items), len(buffers))
	}

	// Verify buffers are in sorted order by checking the underlying data
	expectedOrder := []int{1, 2, 5, 8, 9}
	for i, buffer := range buffers {
		// Convert buffer back to TestItem pointer to verify ordering
		itemPtr := (*TestItem)(unsafe.Pointer(&buffer[0]))
		if itemPtr.ID != expectedOrder[i] {
			t.Errorf("Buffer %d: expected ID %d, got %d", i, expectedOrder[i], itemPtr.ID)
		}

		// Verify buffer length matches getItemSize result
		expectedSize := getIntItemSize(itemPtr)
		if len(buffer) != expectedSize {
			t.Errorf("Buffer %d: expected length %d, got %d", i, expectedSize, len(buffer))
		}
	}
}

// TestToPwritevSliceRawWithPwritev tests ToPwritevSliceRaw with actual Pwritev system call
func TestToPwritevSliceRawWithPwritev(t *testing.T) {
	sl := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)

	// Insert test items in random order
	items := []*TestItem{
		{ID: 3, Name: "three"},
		{ID: 1, Name: "one"},
		{ID: 4, Name: "four"},
		{ID: 2, Name: "two"},
	}

	for _, item := range items {
		sl.Insert(item)
	}

	// Get temporary directory
	tmpDir := os.Getenv("TMPDIR")
	if tmpDir == "" {
		tmpDir = "/tmp"
	}

	// Create temporary file
	tempFile := filepath.Join(tmpDir, fmt.Sprintf("zerocopyskiplist_test_%d.dat", time.Now().UnixNano()))
	fd, err := unix.Open(tempFile, unix.O_RDWR|unix.O_CREAT|unix.O_TRUNC, 0644)
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}

	// Ensure cleanup
	defer func() {
		unix.Close(fd)
		os.Remove(tempFile)
	}()

	// Get byte slices using ToPwritevSliceRaw
	buffers := sl.ToPwritevSliceRaw()

	// Write data using Pwritev
	n, err := unix.Pwritev(fd, buffers, 0)
	if err != nil {
		t.Fatalf("Pwritev failed: %v", err)
	}

	// Calculate expected total size
	expectedSize := len(items) * int(unsafe.Sizeof(TestItem{}))
	if n != expectedSize {
		t.Errorf("Expected to write %d bytes, wrote %d", expectedSize, n)
	}

	// Read the file back to verify contents
	fileData := make([]byte, expectedSize)
	readBytes, err := unix.Pread(fd, fileData, 0)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	if readBytes != expectedSize {
		t.Errorf("Expected to read %d bytes, read %d", expectedSize, readBytes)
	}

	// Verify the file contents match expected order (sorted by ID)
	expectedOrder := []TestItem{
		{ID: 1, Name: "one"},
		{ID: 2, Name: "two"},
		{ID: 3, Name: "three"},
		{ID: 4, Name: "four"},
	}

	itemSize := int(unsafe.Sizeof(TestItem{}))
	for i, expectedItem := range expectedOrder {
		offset := i * itemSize

		// Extract the TestItem from the file data
		fileItem := (*TestItem)(unsafe.Pointer(&fileData[offset]))

		// Compare ID (the key field)
		if fileItem.ID != expectedItem.ID {
			t.Errorf("Item %d: expected ID %d, got %d", i, expectedItem.ID, fileItem.ID)
		}

		// Compare Name field (accounting for potential padding/string representation differences)
		// Note: We need to be careful with string comparison due to how Go stores strings in memory
		if fileItem.Name != expectedItem.Name {
			t.Errorf("Item %d: expected Name %s, got %s", i, expectedItem.Name, fileItem.Name)
		}
	}

	// Additional verification: check that the written data matches the original skiplist order
	bufferOffset := 0
	for i, buffer := range buffers {
		// Compare the buffer data with the corresponding section in the file
		start := bufferOffset
		end := bufferOffset + len(buffer)

		if end > len(fileData) {
			t.Errorf("Buffer %d extends beyond file data", i)
			continue
		}

		fileSection := fileData[start:end]
		for j, b := range buffer {
			if fileSection[j] != b {
				t.Errorf("Buffer %d, byte %d: expected 0x%02x, got 0x%02x", i, j, b, fileSection[j])
				break // Only report first mismatch per buffer
			}
		}

		bufferOffset += len(buffer)
	}
}

// TestItemPtrMethods tests the ItemPtr accessor methods
func TestItemPtrMethods(t *testing.T) {
	// Test with nil ItemPtr
	var nilPtr *ItemPtr[TestItem, int]
	if nilPtr.Item() != nil {
		t.Error("Expected Item() to return nil for nil ItemPtr")
	}
	if nilPtr.Next() != nil {
		t.Error("Expected Next() to return nil for nil ItemPtr")
	}
	if nilPtr.Prev() != nil {
		t.Error("Expected Prev() to return nil for nil ItemPtr")
	}

	// Test with valid ItemPtr
	sl := makeZeroCopySkiplist(16, getIntKey, getIntItemSize, cmpInt)
	item := &TestItem{ID: 5, Name: "test"}
	sl.Insert(item)

	ptr := sl.Find(5)
	if ptr == nil {
		t.Fatal("Expected to find inserted item")
	}

	if ptr.Item() != item {
		t.Error("Expected Item() to return original item pointer")
	}
	if ptr.Key() != 5 {
		t.Errorf("Expected Key() to return 5, got %d", ptr.Key())
	}
}
