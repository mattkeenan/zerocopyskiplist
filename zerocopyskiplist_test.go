package zerocopyskiplist

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
	"unsafe"
)

// Test data structures
type TestItem struct {
	ID    int
	Value string
	Data  []byte
}

type TestContext struct {
	Timestamp   int64
	AccessCount int
	IsCached    bool
	Metadata    map[string]interface{}
}

// Helper functions
func getKeyFromTestItem(item *TestItem) int {
	return item.ID
}

func getTestItemSize(item *TestItem) int {
	return int(unsafe.Sizeof(*item))
}

func compareInt(a, b int) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func createTestItems(count int) []*TestItem {
	items := make([]*TestItem, count)
	for i := 0; i < count; i++ {
		items[i] = &TestItem{
			ID:    i + 1,
			Value: fmt.Sprintf("value_%d", i+1),
			Data:  []byte(fmt.Sprintf("data_%d", i+1)),
		}
	}
	return items
}

func createTestContexts(count int) []*TestContext {
	contexts := make([]*TestContext, count)
	for i := 0; i < count; i++ {
		contexts[i] = &TestContext{
			Timestamp:   time.Now().Unix() + int64(i),
			AccessCount: i,
			IsCached:    i%2 == 0,
			Metadata:    map[string]interface{}{"index": i},
		}
	}
	return contexts
}

func TestBasicOperations(t *testing.T) {
	skiplist := MakeZeroCopySkiplist[TestItem, int, TestContext](
		16,
		getKeyFromTestItem,
		getTestItemSize,
		compareInt,
	)

	if !skiplist.IsEmpty() {
		t.Error("New skiplist should be empty")
	}

	if skiplist.Length() != 0 {
		t.Error("New skiplist should have length 0")
	}

	items := createTestItems(5)
	contexts := createTestContexts(5)

	// Test insert with context
	for i := 0; i < len(items); i++ {
		if !skiplist.Insert(items[i], contexts[i]) {
			t.Errorf("Insert should return true for new item %d", items[i].ID)
		}
	}

	if skiplist.Length() != 5 {
		t.Errorf("Expected length 5, got %d", skiplist.Length())
	}

	if skiplist.IsEmpty() {
		t.Error("Skiplist should not be empty after inserts")
	}
}

func TestInsertWithoutContext(t *testing.T) {
	skiplist := MakeZeroCopySkiplist[TestItem, int, TestContext](
		16,
		getKeyFromTestItem,
		getTestItemSize,
		compareInt,
	)

	items := createTestItems(3)

	// Test insert without context
	for i := 0; i < len(items); i++ {
		if !skiplist.Insert(items[i], nil) {
			t.Errorf("Insert without context should return true for new item %d", items[i].ID)
		}
	}

	// Test InsertWithoutContext method
	extraItem := &TestItem{ID: 10, Value: "extra", Data: []byte("extra")}
	if !skiplist.InsertWithoutContext(extraItem) {
		t.Error("InsertWithoutContext should return true for new item")
	}

	if skiplist.Length() != 4 {
		t.Errorf("Expected length 4, got %d", skiplist.Length())
	}
}

func TestUpdateExistingItem(t *testing.T) {
	skiplist := MakeZeroCopySkiplist[TestItem, int, TestContext](
		16,
		getKeyFromTestItem,
		getTestItemSize,
		compareInt,
	)

	originalItem := &TestItem{ID: 1, Value: "original", Data: []byte("original")}
	originalContext := &TestContext{AccessCount: 5}

	// Insert original
	if !skiplist.Insert(originalItem, originalContext) {
		t.Error("First insert should return true")
	}

	updatedItem := &TestItem{ID: 1, Value: "updated", Data: []byte("updated")}
	updatedContext := &TestContext{AccessCount: 10}

	// Update existing (should return false)
	if skiplist.Insert(updatedItem, updatedContext) {
		t.Error("Insert of existing key should return false")
	}

	// Verify the item and context were updated
	found, ctx := skiplist.Find(1)
	if found == nil {
		t.Fatal("Should find the item")
	}

	if found.Item() != updatedItem {
		t.Error("Item should be updated")
	}

	if ctx != updatedContext {
		t.Error("Context should be updated")
	}

	if skiplist.Length() != 1 {
		t.Error("Length should remain 1 after update")
	}
}

func TestDelete(t *testing.T) {
	skiplist := MakeZeroCopySkiplist[TestItem, int, TestContext](
		16,
		getKeyFromTestItem,
		getTestItemSize,
		compareInt,
	)

	items := createTestItems(5)
	contexts := createTestContexts(5)

	// Insert items
	for i := 0; i < len(items); i++ {
		skiplist.Insert(items[i], contexts[i])
	}

	// Delete existing item
	if !skiplist.Delete(3) {
		t.Error("Delete should return true for existing item")
	}

	if skiplist.Length() != 4 {
		t.Errorf("Expected length 4 after delete, got %d", skiplist.Length())
	}

	// Verify item is gone
	found, ctx := skiplist.Find(3)
	if found != nil || ctx != nil {
		t.Error("Deleted item should not be found")
	}

	// Delete non-existent item
	if skiplist.Delete(999) {
		t.Error("Delete should return false for non-existent item")
	}

	if skiplist.Length() != 4 {
		t.Error("Length should not change when deleting non-existent item")
	}
}

func TestFindOperations(t *testing.T) {
	skiplist := MakeZeroCopySkiplist[TestItem, int, TestContext](
		16,
		getKeyFromTestItem,
		getTestItemSize,
		compareInt,
	)

	items := createTestItems(5)
	contexts := createTestContexts(5)

	// Insert items with contexts
	for i := 0; i < len(items); i++ {
		skiplist.Insert(items[i], contexts[i])
	}

	// Test Find method (returns both item and context)
	for i := 0; i < len(items); i++ {
		found, ctx := skiplist.Find(items[i].ID)
		if found == nil {
			t.Errorf("Should find item with ID %d", items[i].ID)
			continue
		}
		if found.Item() != items[i] {
			t.Errorf("Found wrong item for ID %d", items[i].ID)
		}
		if ctx != contexts[i] {
			t.Errorf("Found wrong context for ID %d", items[i].ID)
		}
	}

	// Test FindItem method (backward compatibility)
	for i := 0; i < len(items); i++ {
		found := skiplist.FindItem(items[i].ID)
		if found == nil {
			t.Errorf("FindItem should find item with ID %d", items[i].ID)
			continue
		}
		if found.Item() != items[i] {
			t.Errorf("FindItem found wrong item for ID %d", items[i].ID)
		}
		if found.Context() != contexts[i] {
			t.Errorf("FindItem found wrong context for ID %d", items[i].ID)
		}
	}

	// Test finding non-existent item
	found, ctx := skiplist.Find(999)
	if found != nil || ctx != nil {
		t.Error("Should not find non-existent item")
	}

	foundItem := skiplist.FindItem(999)
	if foundItem != nil {
		t.Error("FindItem should not find non-existent item")
	}
}

func TestContextOperations(t *testing.T) {
	skiplist := MakeZeroCopySkiplist[TestItem, int, TestContext](
		16,
		getKeyFromTestItem,
		getTestItemSize,
		compareInt,
	)

	item := &TestItem{ID: 1, Value: "test", Data: []byte("test")}
	context := &TestContext{
		Timestamp:   123456789,
		AccessCount: 5,
		IsCached:    true,
	}

	// Insert with context
	skiplist.Insert(item, context)

	// Test Context() method on ItemPtr
	found := skiplist.FindItem(1)
	if found == nil {
		t.Fatal("Should find inserted item")
	}

	if found.Context() != context {
		t.Error("Context() should return the inserted context")
	}

	// Test SetContext() method
	newContext := &TestContext{
		Timestamp:   987654321,
		AccessCount: 10,
		IsCached:    false,
	}
	found.SetContext(newContext)

	if found.Context() != newContext {
		t.Error("SetContext() should update the context")
	}

	// Test UpdateContext() method
	anotherContext := &TestContext{
		Timestamp:   555555555,
		AccessCount: 15,
		IsCached:    true,
	}
	if !skiplist.UpdateContext(1, anotherContext) {
		t.Error("UpdateContext should return true for existing key")
	}

	found, ctx := skiplist.Find(1)
	if ctx != anotherContext {
		t.Error("UpdateContext should update the context")
	}

	// Test UpdateContext for non-existent key
	if skiplist.UpdateContext(999, anotherContext) {
		t.Error("UpdateContext should return false for non-existent key")
	}
}

func TestNavigation(t *testing.T) {
	skiplist := MakeZeroCopySkiplist[TestItem, int, TestContext](
		16,
		getKeyFromTestItem,
		getTestItemSize,
		compareInt,
	)

	items := createTestItems(5)
	contexts := createTestContexts(5)

	// Insert in random order
	indices := []int{2, 0, 4, 1, 3}
	for _, i := range indices {
		skiplist.Insert(items[i], contexts[i])
	}

	// Test First()
	first := skiplist.First()
	if first == nil || first.Key() != 1 {
		t.Error("First() should return item with key 1")
	}

	// Test Last()
	last := skiplist.Last()
	if last == nil || last.Key() != 5 {
		t.Error("Last() should return item with key 5")
	}

	// Test forward navigation
	current := skiplist.First()
	expectedKeys := []int{1, 2, 3, 4, 5}
	for i, expectedKey := range expectedKeys {
		if current == nil {
			t.Fatalf("Navigation failed at position %d", i)
		}
		if current.Key() != expectedKey {
			t.Errorf("Expected key %d at position %d, got %d", expectedKey, i, current.Key())
		}
		// Verify context access during navigation
		if current.Context() != contexts[expectedKey-1] {
			t.Errorf("Wrong context at position %d", i)
		}
		current = current.Next()
	}

	if current != nil {
		t.Error("Next() after last item should return nil")
	}

	// Test backward navigation
	current = skiplist.Last()
	for i := len(expectedKeys) - 1; i >= 0; i-- {
		if current == nil {
			t.Fatalf("Backward navigation failed at position %d", i)
		}
		if current.Key() != expectedKeys[i] {
			t.Errorf("Expected key %d at position %d, got %d", expectedKeys[i], i, current.Key())
		}
		current = current.Prev()
	}

	if current != nil {
		t.Error("Prev() before first item should return nil")
	}
}

func TestCopy(t *testing.T) {
	original := MakeZeroCopySkiplist[TestItem, int, TestContext](
		16,
		getKeyFromTestItem,
		getTestItemSize,
		compareInt,
	)

	items := createTestItems(3)
	contexts := createTestContexts(3)

	for i := 0; i < len(items); i++ {
		original.Insert(items[i], contexts[i])
	}

	// Create copy
	copy := original.Copy()

	// Verify copy has same items and contexts
	if copy.Length() != original.Length() {
		t.Error("Copy should have same length as original")
	}

	for i := 0; i < len(items); i++ {
		foundOrig, ctxOrig := original.Find(items[i].ID)
		foundCopy, ctxCopy := copy.Find(items[i].ID)

		if foundOrig == nil || foundCopy == nil {
			t.Errorf("Both original and copy should contain item %d", items[i].ID)
			continue
		}

		if foundOrig.Item() != foundCopy.Item() {
			t.Errorf("Copy should reference same item pointers for item %d", items[i].ID)
		}

		if ctxOrig != ctxCopy {
			t.Errorf("Copy should reference same context pointers for item %d", items[i].ID)
		}
	}

	// Verify independence - modify copy shouldn't affect original
	newItem := &TestItem{ID: 10, Value: "new", Data: []byte("new")}
	copy.Insert(newItem, nil)

	if original.Length() == copy.Length() {
		t.Error("Copy should be independent of original")
	}
}

func TestToPwritevSlice(t *testing.T) {
	skiplist := MakeZeroCopySkiplist[TestItem, int, TestContext](
		16,
		getKeyFromTestItem,
		getTestItemSize,
		compareInt,
	)

	items := createTestItems(3)
	contexts := createTestContexts(3)

	for i := 0; i < len(items); i++ {
		skiplist.Insert(items[i], contexts[i])
	}

	// Test custom serialization
	buffers := skiplist.ToPwritevSlice(func(item *TestItem) []byte {
		return []byte(fmt.Sprintf("ID:%d,Value:%s", item.ID, item.Value))
	})

	if len(buffers) != 3 {
		t.Errorf("Expected 3 buffers, got %d", len(buffers))
	}

	expectedData := []string{
		"ID:1,Value:value_1",
		"ID:2,Value:value_2",
		"ID:3,Value:value_3",
	}

	for i, buffer := range buffers {
		if string(buffer) != expectedData[i] {
			t.Errorf("Buffer %d: expected %s, got %s", i, expectedData[i], string(buffer))
		}
	}

	// Test raw serialization
	rawBuffers := skiplist.ToPwritevSliceRaw()
	if len(rawBuffers) != 3 {
		t.Errorf("Expected 3 raw buffers, got %d", len(rawBuffers))
	}

	for i, buffer := range rawBuffers {
		expectedSize := getTestItemSize(items[i])
		if len(buffer) != expectedSize {
			t.Errorf("Raw buffer %d: expected size %d, got %d", i, expectedSize, len(buffer))
		}
	}
}

func TestMerge(t *testing.T) {
	skiplist1 := MakeZeroCopySkiplist[TestItem, int, TestContext](
		16,
		getKeyFromTestItem,
		getTestItemSize,
		compareInt,
	)

	skiplist2 := MakeZeroCopySkiplist[TestItem, int, TestContext](
		16,
		getKeyFromTestItem,
		getTestItemSize,
		compareInt,
	)

	// Add items to first skiplist
	items1 := []*TestItem{
		{ID: 1, Value: "value1_sl1"},
		{ID: 3, Value: "value3_sl1"},
	}
	contexts1 := []*TestContext{
		{AccessCount: 1},
		{AccessCount: 3},
	}

	for i, item := range items1 {
		skiplist1.Insert(item, contexts1[i])
	}

	// Add items to second skiplist (with overlap)
	items2 := []*TestItem{
		{ID: 2, Value: "value2_sl2"},
		{ID: 3, Value: "value3_sl2"}, // Conflicting key
	}
	contexts2 := []*TestContext{
		{AccessCount: 2},
		{AccessCount: 33}, // Different context for same key
	}

	for i, item := range items2 {
		skiplist2.Insert(item, contexts2[i])
	}

	// Test MergeTheirs strategy
	copyForTheirs := skiplist1.Copy()
	err := copyForTheirs.Merge(skiplist2, MergeTheirs)
	if err != nil {
		t.Errorf("MergeTheirs should not return error: %v", err)
	}

	if copyForTheirs.Length() != 3 {
		t.Errorf("After merge, expected length 3, got %d", copyForTheirs.Length())
	}

	// Verify conflict resolution - should use "theirs" (skiplist2) values
	found, ctx := copyForTheirs.Find(3)
	if found == nil {
		t.Fatal("Should find merged item")
	}
	if found.Item().Value != "value3_sl2" {
		t.Error("MergeTheirs should use value from second skiplist")
	}
	if ctx.AccessCount != 33 {
		t.Error("MergeTheirs should use context from second skiplist")
	}

	// Test MergeOurs strategy
	copyForOurs := skiplist1.Copy()
	err = copyForOurs.Merge(skiplist2, MergeOurs)
	if err != nil {
		t.Errorf("MergeOurs should not return error: %v", err)
	}

	found, ctx = copyForOurs.Find(3)
	if found == nil {
		t.Fatal("Should find merged item")
	}
	if found.Item().Value != "value3_sl1" {
		t.Error("MergeOurs should keep value from first skiplist")
	}
	if ctx.AccessCount != 3 {
		t.Error("MergeOurs should keep context from first skiplist")
	}

	// Test MergeError strategy
	copyForError := skiplist1.Copy()
	err = copyForError.Merge(skiplist2, MergeError)
	if err == nil {
		t.Error("MergeError should return error for conflicting keys")
	}
}

func TestLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test in short mode")
	}

	skiplist := MakeZeroCopySkiplist[TestItem, int, TestContext](
		20,
		getKeyFromTestItem,
		getTestItemSize,
		compareInt,
	)

	const numItems = 10000
	items := make([]*TestItem, numItems)
	contexts := make([]*TestContext, numItems)

	// Create items in random order
	for i := 0; i < numItems; i++ {
		items[i] = &TestItem{
			ID:    i + 1,
			Value: fmt.Sprintf("large_value_%d", i+1),
			Data:  make([]byte, 100), // Larger data
		}
		contexts[i] = &TestContext{
			Timestamp:   time.Now().Unix() + int64(i),
			AccessCount: rand.Intn(1000),
			IsCached:    rand.Float32() < 0.5,
		}
	}

	// Shuffle for random insertion order
	rand.Seed(time.Now().UnixNano())
	for i := range items {
		j := rand.Intn(i + 1)
		items[i], items[j] = items[j], items[i]
		contexts[i], contexts[j] = contexts[j], contexts[i]
	}

	// Insert all items
	start := time.Now()
	for i := 0; i < numItems; i++ {
		skiplist.Insert(items[i], contexts[i])
	}
	insertTime := time.Since(start)

	if skiplist.Length() != numItems {
		t.Errorf("Expected length %d, got %d", numItems, skiplist.Length())
	}

	// Test search performance
	start = time.Now()
	searchKeys := make([]int, 1000)
	for i := range searchKeys {
		searchKeys[i] = rand.Intn(numItems) + 1
	}

	for _, key := range searchKeys {
		found, ctx := skiplist.Find(key)
		if found == nil || ctx == nil {
			t.Errorf("Should find item and context for key %d", key)
		}
	}
	searchTime := time.Since(start)

	t.Logf("Large dataset test completed:")
	t.Logf("  Items: %d", numItems)
	t.Logf("  Insert time: %v", insertTime)
	t.Logf("  Search time for 1000 queries: %v", searchTime)
	t.Logf("  Average search time: %v", searchTime/1000)
}

func TestConcurrency(t *testing.T) {
	skiplist := MakeZeroCopySkiplist[TestItem, int, TestContext](
		16,
		getKeyFromTestItem,
		getTestItemSize,
		compareInt,
	)

	const numGoroutines = 10
	const itemsPerGoroutine = 100

	var wg sync.WaitGroup

	// Concurrent insertions
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < itemsPerGoroutine; i++ {
				id := goroutineID*itemsPerGoroutine + i
				item := &TestItem{
					ID:    id,
					Value: fmt.Sprintf("value_%d", id),
					Data:  []byte(fmt.Sprintf("data_%d", id)),
				}
				context := &TestContext{
					AccessCount: i,
					IsCached:    i%2 == 0,
				}
				skiplist.Insert(item, context)
			}
		}(g)
	}

	wg.Wait()

	expectedLength := numGoroutines * itemsPerGoroutine
	if skiplist.Length() != expectedLength {
		t.Errorf("Expected length %d after concurrent inserts, got %d", expectedLength, skiplist.Length())
	}

	// Concurrent reads
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < itemsPerGoroutine; i++ {
				id := goroutineID*itemsPerGoroutine + i
				found, ctx := skiplist.Find(id)
				if found == nil {
					t.Errorf("Should find item with ID %d", id)
					continue
				}
				if found.Key() != id {
					t.Errorf("Found wrong item for ID %d", id)
				}
				if ctx == nil {
					t.Errorf("Should find context for ID %d", id)
				}
			}
		}(g)
	}

	wg.Wait()
}

func TestEdgeCases(t *testing.T) {
	skiplist := MakeZeroCopySkiplist[TestItem, int, TestContext](
		16,
		getKeyFromTestItem,
		getTestItemSize,
		compareInt,
	)

	// Test operations on empty skiplist
	if skiplist.First() != nil {
		t.Error("First() on empty skiplist should return nil")
	}

	if skiplist.Last() != nil {
		t.Error("Last() on empty skiplist should return nil")
	}

	found, ctx := skiplist.Find(1)
	if found != nil || ctx != nil {
		t.Error("Find() on empty skiplist should return nil")
	}

	if skiplist.Delete(1) {
		t.Error("Delete() on empty skiplist should return false")
	}

	// Insert single item
	item := &TestItem{ID: 1, Value: "single", Data: []byte("single")}
	context := &TestContext{AccessCount: 1}
	skiplist.Insert(item, context)

	// Test single item operations
	if skiplist.First() != skiplist.Last() {
		t.Error("In single-item skiplist, First() should equal Last()")
	}

	first := skiplist.First()
	if first.Next() != nil {
		t.Error("Single item's Next() should be nil")
	}

	if first.Prev() != nil {
		t.Error("Single item's Prev() should be nil")
	}

	// Test with nil context
	itemWithoutContext := &TestItem{ID: 2, Value: "no_context", Data: []byte("no_context")}
	skiplist.Insert(itemWithoutContext, nil)

	found, ctx = skiplist.Find(2)
	if found == nil {
		t.Error("Should find item inserted with nil context")
	}
	if ctx != nil {
		t.Error("Context should be nil for item inserted with nil context")
	}
}

// Benchmark tests
func BenchmarkInsert(b *testing.B) {
	skiplist := MakeZeroCopySkiplist[TestItem, int, TestContext](
		16,
		getKeyFromTestItem,
		getTestItemSize,
		compareInt,
	)

	items := make([]*TestItem, b.N)
	contexts := make([]*TestContext, b.N)
	for i := 0; i < b.N; i++ {
		items[i] = &TestItem{ID: i, Value: fmt.Sprintf("value_%d", i)}
		contexts[i] = &TestContext{AccessCount: i}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		skiplist.Insert(items[i], contexts[i])
	}
}

func BenchmarkFind(b *testing.B) {
	skiplist := MakeZeroCopySkiplist[TestItem, int, TestContext](
		16,
		getKeyFromTestItem,
		getTestItemSize,
		compareInt,
	)

	// Pre-populate
	for i := 0; i < 10000; i++ {
		item := &TestItem{ID: i, Value: fmt.Sprintf("value_%d", i)}
		context := &TestContext{AccessCount: i}
		skiplist.Insert(item, context)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := i % 10000
		_, _ = skiplist.Find(key)
	}
}

func BenchmarkFindItem(b *testing.B) {
	skiplist := MakeZeroCopySkiplist[TestItem, int, TestContext](
		16,
		getKeyFromTestItem,
		getTestItemSize,
		compareInt,
	)

	// Pre-populate
	for i := 0; i < 10000; i++ {
		item := &TestItem{ID: i, Value: fmt.Sprintf("value_%d", i)}
		context := &TestContext{AccessCount: i}
		skiplist.Insert(item, context)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := i % 10000
		_ = skiplist.FindItem(key)
	}
}

func BenchmarkTraversal(b *testing.B) {
	skiplist := MakeZeroCopySkiplist[TestItem, int, TestContext](
		16,
		getKeyFromTestItem,
		getTestItemSize,
		compareInt,
	)

	// Pre-populate
	for i := 0; i < 1000; i++ {
		item := &TestItem{ID: i, Value: fmt.Sprintf("value_%d", i)}
		context := &TestContext{AccessCount: i}
		skiplist.Insert(item, context)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		current := skiplist.First()
		for current != nil {
			_ = current.Item()
			_ = current.Context()
			current = current.Next()
		}
	}
}
