package zerocopyskiplist

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/google/vectorio"
)

// writevChunked writes iovecs in chunks to avoid IOV_MAX limits
func writevChunked(fd uintptr, iovecs []syscall.Iovec, chunkSize int) (int, error) {
	totalWritten := 0
	for i := 0; i < len(iovecs); i += chunkSize {
		end := i + chunkSize
		if end > len(iovecs) {
			end = len(iovecs)
		}

		chunk := iovecs[i:end]
		written, err := vectorio.WritevRaw(fd, chunk)
		if err != nil {
			return totalWritten, err
		}
		totalWritten += written
	}
	return totalWritten, nil
}

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
	MetadataKey string // Changed from map to simple string to make comparable
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
			MetadataKey: fmt.Sprintf("key_%d", i), // Changed from map to string
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

	// Test insert with explicit nil context
	for i := 0; i < len(items); i++ {
		if !skiplist.Insert(items[i], nil) {
			t.Errorf("Insert with nil context should return true for new item %d", items[i].ID)
		}
	}

	// Test another insert with nil context
	extraItem := &TestItem{ID: 10, Value: "extra", Data: []byte("extra")}
	if !skiplist.Insert(extraItem, nil) {
		t.Error("Insert with nil context should return true for new item")
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

// NEW TESTS FOR IOVEC FUNCTIONALITY

func TestToIovecSlice(t *testing.T) {
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

	iovecs := skiplist.ToIovecSlice()

	if len(iovecs) != 3 {
		t.Errorf("Expected 3 iovecs, got %d", len(iovecs))
	}

	// Verify iovecs point to correct data
	current := skiplist.First()
	for i, iovec := range iovecs {
		if current == nil {
			t.Fatalf("Not enough items in skiplist for iovec %d", i)
		}

		expectedBase := (*byte)(unsafe.Pointer(current.Item()))
		if iovec.Base != expectedBase {
			t.Errorf("Iovec %d has wrong base pointer", i)
		}

		expectedLen := uint64(getTestItemSize(current.Item()))
		if iovec.Len != expectedLen {
			t.Errorf("Iovec %d has wrong length: expected %d, got %d", i, expectedLen, iovec.Len)
		}

		current = current.Next()
	}
}

func TestToContextIovecSlice(t *testing.T) {
	skiplist := MakeZeroCopySkiplist[TestItem, int, TestContext](
		16,
		getKeyFromTestItem,
		getTestItemSize,
		compareInt,
	)

	items := createTestItems(4)
	contexts := createTestContexts(4)

	// Create a specific context to search for
	targetContext := &TestContext{
		Timestamp:   999999,
		AccessCount: 100,
		IsCached:    true,
	}

	// Insert items - some with target context, some without
	skiplist.Insert(items[0], contexts[0])   // different context
	skiplist.Insert(items[1], targetContext) // target context
	skiplist.Insert(items[2], contexts[2])   // different context
	skiplist.Insert(items[3], targetContext) // target context

	iovecs := skiplist.ToContextIovecSlice(*targetContext)

	// Should find 2 items with target context
	expectedCount := 2
	if len(iovecs) != expectedCount {
		t.Errorf("Expected %d iovecs with target context, got %d", expectedCount, len(iovecs))
	}

	// Verify the iovecs correspond to items with target context
	current := skiplist.First()
	iovecIndex := 0
	for current != nil {
		if current.Context() != nil && *current.Context() == *targetContext {
			if iovecIndex >= len(iovecs) {
				t.Error("Not enough iovecs for matching contexts")
				break
			}

			expectedBase := (*byte)(unsafe.Pointer(current.Item()))
			if iovecs[iovecIndex].Base != expectedBase {
				t.Errorf("Context iovec %d has wrong base pointer", iovecIndex)
			}

			iovecIndex++
		}
		current = current.Next()
	}
}

func TestToNotContextIovecSlice(t *testing.T) {
	skiplist := MakeZeroCopySkiplist[TestItem, int, TestContext](
		16,
		getKeyFromTestItem,
		getTestItemSize,
		compareInt,
	)

	items := createTestItems(4)
	contexts := createTestContexts(4)

	// Create a specific context to exclude
	excludeContext := &TestContext{
		Timestamp:   999999,
		AccessCount: 100,
		IsCached:    true,
	}

	// Insert items - some with exclude context, some without
	skiplist.Insert(items[0], contexts[0])    // different context
	skiplist.Insert(items[1], excludeContext) // exclude context
	skiplist.Insert(items[2], contexts[2])    // different context
	skiplist.Insert(items[3], excludeContext) // exclude context

	iovecs := skiplist.ToNotContextIovecSlice(*excludeContext)

	// Should find 2 items not matching exclude context
	expectedCount := 2
	if len(iovecs) != expectedCount {
		t.Errorf("Expected %d iovecs not matching exclude context, got %d", expectedCount, len(iovecs))
	}

	// Verify the iovecs correspond to items not matching exclude context
	current := skiplist.First()
	iovecIndex := 0
	for current != nil {
		contextMatches := current.Context() != nil && *current.Context() == *excludeContext
		if !contextMatches {
			if iovecIndex >= len(iovecs) {
				t.Error("Not enough iovecs for non-matching contexts")
				break
			}

			expectedBase := (*byte)(unsafe.Pointer(current.Item()))
			if iovecs[iovecIndex].Base != expectedBase {
				t.Errorf("Not-context iovec %d has wrong base pointer", iovecIndex)
			}

			iovecIndex++
		}
		current = current.Next()
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

	// Determine temp directory - use TMPDIR env var if set, otherwise /tmp
	tempDir := os.Getenv("TMPDIR")
	if tempDir == "" {
		tempDir = "/tmp"
	}
	t.Logf("Using temp directory: %s", tempDir)

	// Use string contexts instead of TestContext struct
	skiplist := MakeZeroCopySkiplist[TestItem, int, string](
		24, // Increased for larger dataset
		getKeyFromTestItem,
		getTestItemSize,
		compareInt,
	)

	const numItems = 1000000
	const numDeletes = 300000

	// Define 5 string contexts
	stringContexts := []string{
		"cache_hot",
		"cache_warm",
		"cache_cold",
		"no_cache",
		"cache_dirty",
	}

	items := make([]*TestItem, numItems)
	contexts := make([]*string, numItems)

	// Create deterministic dataset
	for i := 0; i < numItems; i++ {
		items[i] = &TestItem{
			ID:    i + 1,
			Value: fmt.Sprintf("large_value_%d", i+1),
			Data:  make([]byte, 64), // Fixed size for consistency
		}
		// Fill data with deterministic pattern
		for j := range items[i].Data {
			items[i].Data[j] = byte((i + j) % 256)
		}

		// Assign context deterministically - cycle through the 5 contexts
		contextIndex := i % len(stringContexts)
		contexts[i] = &stringContexts[contextIndex]
	}

	// Create deterministic insertion order (reverse order for worst-case scenario)
	insertOrder := make([]*TestItem, numItems)
	insertContexts := make([]*string, numItems)
	for i := 0; i < numItems; i++ {
		insertOrder[i] = items[numItems-1-i] // Reverse order
		insertContexts[i] = contexts[numItems-1-i]
	}

	// Insert all items
	t.Logf("Inserting %d items in reverse order...", numItems)
	start := time.Now()
	for i := 0; i < numItems; i++ {
		skiplist.Insert(insertOrder[i], insertContexts[i])

		// Progress indicator for long operation
		if (i+1)%100000 == 0 {
			elapsed := time.Since(start)
			rate := float64(i+1) / elapsed.Seconds()
			t.Logf("  Inserted %d items (%.0f items/sec)", i+1, rate)
		}
	}
	insertTime := time.Since(start)

	if skiplist.Length() != numItems {
		t.Errorf("Expected length %d, got %d", numItems, skiplist.Length())
	}

	// Test search performance with deterministic keys
	t.Logf("Testing search performance...")
	searchKeys := make([]int, 10000)
	for i := range searchKeys {
		// Deterministic search pattern: every 100th item
		searchKeys[i] = (i * 100) + 1
		if searchKeys[i] > numItems {
			searchKeys[i] = (i % numItems) + 1
		}
	}

	start = time.Now()
	for _, key := range searchKeys {
		found, ctx := skiplist.Find(key)
		if found == nil || ctx == nil {
			t.Errorf("Should find item and context for key %d", key)
		}
	}
	searchTime := time.Since(start)

	// Test all three iovec functions and write to disk using WritevRaw
	t.Logf("Testing iovec generation and disk writes...")

	// 1. Test ToIovecSlice (all items)
	start = time.Now()
	allIovecs := skiplist.ToIovecSlice()
	allIovecTime := time.Since(start)

	if len(allIovecs) != numItems {
		t.Errorf("Expected %d iovecs, got %d", numItems, len(allIovecs))
	}

	// Write all items to disk using WritevRaw
	allFile, err := os.CreateTemp(tempDir, "skiplist_all_*.bin")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(allFile.Name())
	defer allFile.Close()

	start = time.Now()
	allBytesWritten, err := writevChunked(allFile.Fd(), allIovecs, 1024)
	if err != nil {
		t.Fatalf("Failed to write all items: %v", err)
	}
	allWriteTime := time.Since(start)

	// 2. Test ToContextIovecSlice (items matching "cache_hot")
	targetContext := "cache_hot"
	start = time.Now()
	contextIovecs := skiplist.ToContextIovecSlice(targetContext)
	contextIovecTime := time.Since(start)

	expectedContextItems := numItems / len(stringContexts) // Should be 1/5 of items
	if len(contextIovecs) < expectedContextItems-1000 || len(contextIovecs) > expectedContextItems+1000 {
		t.Errorf("Expected ~%d context iovecs, got %d", expectedContextItems, len(contextIovecs))
	}

	// Write context-matching items to disk using WritevRaw
	contextFile, err := os.CreateTemp(tempDir, "skiplist_context_*.bin")
	if err != nil {
		t.Fatalf("Failed to create context temp file: %v", err)
	}
	defer os.Remove(contextFile.Name())
	defer contextFile.Close()

	start = time.Now()
	contextBytesWritten, err := writevChunked(contextFile.Fd(), contextIovecs, 1024)
	if err != nil {
		t.Fatalf("Failed to write context items: %v", err)
	}
	contextWriteTime := time.Since(start)

	// 3. Test ToNotContextIovecSlice (items not matching "cache_hot")
	start = time.Now()
	notContextIovecs := skiplist.ToNotContextIovecSlice(targetContext)
	notContextIovecTime := time.Since(start)

	expectedNotContextItems := numItems - expectedContextItems // Should be 4/5 of items
	if len(notContextIovecs) < expectedNotContextItems-1000 || len(notContextIovecs) > expectedNotContextItems+1000 {
		t.Errorf("Expected ~%d not-context iovecs, got %d", expectedNotContextItems, len(notContextIovecs))
	}

	// Write non-context-matching items to disk using WritevRaw
	notContextFile, err := os.CreateTemp(tempDir, "skiplist_not_context_*.bin")
	if err != nil {
		t.Fatalf("Failed to create not-context temp file: %v", err)
	}
	defer os.Remove(notContextFile.Name())
	defer notContextFile.Close()

	start = time.Now()
	notContextBytesWritten, err := writevChunked(notContextFile.Fd(), notContextIovecs, 1024)
	if err != nil {
		t.Fatalf("Failed to write not-context items: %v", err)
	}
	notContextWriteTime := time.Since(start)

	// Verify the counts add up
	if len(contextIovecs)+len(notContextIovecs) != numItems {
		t.Errorf("Context + not-context items (%d + %d) should equal total items %d",
			len(contextIovecs), len(notContextIovecs), numItems)
	}

	// Delete deterministic set of items (every 3rd item starting from index 2)
	t.Logf("Deleting %d items...", numDeletes)
	deleteKeys := make([]int, numDeletes)
	for i := 0; i < numDeletes; i++ {
		deleteKeys[i] = (i * 3) + 3 // Every 3rd item: 3, 6, 9, 12, ...
	}

	start = time.Now()
	deletedCount := 0
	for i, key := range deleteKeys {
		if skiplist.Delete(key) {
			deletedCount++
		}

		// Progress indicator for long operation
		if (i+1)%50000 == 0 {
			elapsed := time.Since(start)
			rate := float64(i+1) / elapsed.Seconds()
			t.Logf("  Processed %d deletions (%.0f ops/sec)", i+1, rate)
		}
	}
	deleteTime := time.Since(start)

	expectedFinalLength := numItems - deletedCount
	if skiplist.Length() != expectedFinalLength {
		t.Errorf("Expected final length %d, got %d", expectedFinalLength, skiplist.Length())
	}

	// Verify deletions worked by searching for deleted items
	t.Logf("Verifying deletions...")
	start = time.Now()
	for i := 0; i < 1000; i++ { // Check first 1000 deleted items
		key := deleteKeys[i]
		found, _ := skiplist.Find(key)
		if found != nil {
			t.Errorf("Item %d should have been deleted but was found", key)
		}
	}
	verifyTime := time.Since(start)

	// Test navigation after deletions
	t.Logf("Testing navigation after deletions...")
	start = time.Now()
	current := skiplist.First()
	navigationCount := 0
	for current != nil && navigationCount < 10000 { // Navigate first 10k items
		_ = current.Item()
		_ = current.Context()
		current = current.Next()
		navigationCount++
	}
	navigationTime := time.Since(start)

	// Final iovec test after deletions - write remaining items to disk
	start = time.Now()
	finalIovecs := skiplist.ToIovecSlice()
	finalIovecTime := time.Since(start)

	if len(finalIovecs) != expectedFinalLength {
		t.Errorf("Expected %d final iovecs, got %d", expectedFinalLength, len(finalIovecs))
	}

	// Write final state to disk using WritevRaw
	finalFile, err := os.CreateTemp(tempDir, "skiplist_final_*.bin")
	if err != nil {
		t.Fatalf("Failed to create final temp file: %v", err)
	}
	defer os.Remove(finalFile.Name())
	defer finalFile.Close()

	start = time.Now()
	finalBytesWritten, err := writevChunked(finalFile.Fd(), finalIovecs, 1024)
	if err != nil {
		t.Fatalf("Failed to write final items: %v", err)
	}
	finalWriteTime := time.Since(start)

	// Performance summary
	t.Logf("\n=== Large Dataset Test Results ===")
	t.Logf("Dataset: %d items, %d deletions", numItems, numDeletes)
	t.Logf("String contexts: %v", stringContexts)
	t.Logf("Final size: %d items", skiplist.Length())

	t.Logf("\nPerformance Metrics:")
	t.Logf("  Insert time: %v (%.0f items/sec)", insertTime, float64(numItems)/insertTime.Seconds())
	t.Logf("  Search time (10k queries): %v (%.0f searches/sec)", searchTime, float64(len(searchKeys))/searchTime.Seconds())
	t.Logf("  Delete time: %v (%.0f deletions/sec)", deleteTime, float64(len(deleteKeys))/deleteTime.Seconds())
	t.Logf("  Verify time (1k checks): %v", verifyTime)
	t.Logf("  Navigation time (10k items): %v", navigationTime)

	t.Logf("\nIovec Generation Times:")
	t.Logf("  All items iovec: %v (%d items)", allIovecTime, len(allIovecs))
	t.Logf("  Context='%s' iovec: %v (%d items)", targetContext, contextIovecTime, len(contextIovecs))
	t.Logf("  Not-context='%s' iovec: %v (%d items)", targetContext, notContextIovecTime, len(notContextIovecs))
	t.Logf("  Final state iovec: %v (%d items)", finalIovecTime, len(finalIovecs))

	t.Logf("\nDisk Write Performance:")
	t.Logf("  All items write: %v (%d bytes, %.1f MB/s)",
		allWriteTime, allBytesWritten, float64(allBytesWritten)/(1024*1024)/allWriteTime.Seconds())
	t.Logf("  Context items write: %v (%d bytes, %.1f MB/s)",
		contextWriteTime, contextBytesWritten, float64(contextBytesWritten)/(1024*1024)/contextWriteTime.Seconds())
	t.Logf("  Not-context items write: %v (%d bytes, %.1f MB/s)",
		notContextWriteTime, notContextBytesWritten, float64(notContextBytesWritten)/(1024*1024)/notContextWriteTime.Seconds())
	t.Logf("  Final state write: %v (%d bytes, %.1f MB/s)",
		finalWriteTime, finalBytesWritten, float64(finalBytesWritten)/(1024*1024)/finalWriteTime.Seconds())

	avgSearchTime := searchTime / time.Duration(len(searchKeys))
	avgDeleteTime := deleteTime / time.Duration(len(deleteKeys))
	t.Logf("\nAverage Operation Times:")
	t.Logf("  Average search: %v", avgSearchTime)
	t.Logf("  Average delete: %v", avgDeleteTime)

	// Calculate total data written
	totalBytesWritten := allBytesWritten + contextBytesWritten + notContextBytesWritten + finalBytesWritten
	totalWriteTime := allWriteTime + contextWriteTime + notContextWriteTime + finalWriteTime
	t.Logf("\nTotal Disk I/O:")
	t.Logf("  Total bytes written: %d (%.1f MB)", totalBytesWritten, float64(totalBytesWritten)/(1024*1024))
	t.Logf("  Total write time: %v", totalWriteTime)
	t.Logf("  Overall write throughput: %.1f MB/s", float64(totalBytesWritten)/(1024*1024)/totalWriteTime.Seconds())
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

	// Test iovec operations on empty skiplist
	iovecs := skiplist.ToIovecSlice()
	if len(iovecs) != 0 {
		t.Error("ToIovecSlice() on empty skiplist should return empty slice")
	}

	testContext := TestContext{AccessCount: 1}
	contextIovecs := skiplist.ToContextIovecSlice(testContext)
	if len(contextIovecs) != 0 {
		t.Error("ToContextIovecSlice() on empty skiplist should return empty slice")
	}

	notContextIovecs := skiplist.ToNotContextIovecSlice(testContext)
	if len(notContextIovecs) != 0 {
		t.Error("ToNotContextIovecSlice() on empty skiplist should return empty slice")
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

func BenchmarkToIovecSlice(b *testing.B) {
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
		_ = skiplist.ToIovecSlice()
	}
}
