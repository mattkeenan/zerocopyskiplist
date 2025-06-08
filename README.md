# ZeroCopySkiplist

A high-performance skiplist implementation in Go using generics, optimized for accessing streamed or memory-mapped arrays of homogeneous data structures.

## Origin

This library was originally developed for the `dircachefilehash` program, where it's used to manipulate the dircache index (which behaves similarly to the git index file). The skiplist provides fast ordered access to large datasets without copying the underlying data structures.

## Features

- **Generic implementation** supporting any comparable key type
- **Memory efficient** - stores only pointers to existing data structures
- **Double-linked navigation** with forward/backward traversal
- **Configurable maximum levels** for optimal performance tuning
- **O(log n) search, insertion, and navigation**
- **Zero-copy design** ideal for mmap'd data arrays

## Use Cases

- Fast ordered access to large mmap'd file indexes
- Directory cache manipulation and searching
- Efficient batch disk writes using Pwritev() with ordered data
- Handling variable-size data structures with known size functions
- Any scenario requiring ordered access to existing data arrays without copying

## Installation

```bash
go get github.com/mattkeenan/zerocopyskiplist
```

## Quick Start

```go
package main

import (
    "fmt"
    "unsafe"
    "github.com/mattkeenan/zerocopyskiplist"
)

type FileEntry struct {
    ID   int
    Path string
    Hash []byte
}

func main() {
    // Your existing data array (could be mmap'd)
    var fileEntries []FileEntry

    // Create skiplist with configurable max levels
    skiplist := zerocopyskiplist.makeZeroCopySkiplist(
        16, // maxLevel
        func(entry *FileEntry) int { return entry.ID }, // key extractor
        func(entry *FileEntry) int { // size function
            return int(unsafe.Sizeof(*entry))
        },
        func(a, b int) int { // key comparator
            if a < b { return -1 }
            if a > b { return 1 }
            return 0
        },
    )

    // Insert pointers to existing data (no copying)
    for i := range fileEntries {
        skiplist.Insert(&fileEntries[i])
    }

    // Fast ordered access
    if found := skiplist.Find(42); found != nil {
        fmt.Printf("Found: %s\n", found.Item().Path)
    }

    // Traverse in order
    for current := skiplist.First(); current != nil; current = current.Next() {
        fmt.Printf("ID: %d, Path: %s\n", current.Key(), current.Item().Path)
    }

    // Generate byte slices for efficient disk writes (custom serialization)
    buffers := skiplist.ToPwritevSlice(func(entry *FileEntry) []byte {
        // Convert struct to byte slice (zero-copy)
        size := int(unsafe.Sizeof(*entry))
        return (*[1024]byte)(unsafe.Pointer(entry))[:size:size]
    })

    // Or use the built-in size function for raw binary output
    rawBuffers := skiplist.ToPwritevSliceRaw()

    // Use buffers with unix.Pwritev() for efficient batch writes
    // n, err := unix.Pwritev(fd, buffers, offset)
    // n, err := unix.Pwritev(fd, rawBuffers, offset)

    // Merge another skiplist
    otherSkiplist := zerocopyskiplist.makeZeroCopySkiplist(16, getKeyFromItem, getItemSize, cmpInt)
    // ... populate otherSkiplist ...

    // Merge with conflict resolution
    err = skiplist.Merge(otherSkiplist, zerocopyskiplist.MergeTheirs)
    if err != nil {
        fmt.Printf("Merge failed: %v\n", err)
    }
}
```

## API Overview

### Core Types

- `ZeroCopySkiplist[T, K]` - Main skiplist structure
- `ItemPtr[T, K]` - Node pointing to your data with navigation methods
- `MergeStrategy` - Enum for handling key conflicts during merge operations (`MergeTheirs`, `MergeOurs`, `MergeError`)

### Main Functions

- `makeZeroCopySkiplist(maxLevel, getKeyFromItem, getItemSize, cmpKey)` - Constructor
- `Insert(item *T) bool` - Add item to skiplist
- `Delete(key K) bool` - Remove item with given key from skiplist
- `Merge(other *ZeroCopySkiplist[T, K], strategy MergeStrategy) error` - Merge another skiplist into this one
- `Find(key K) *ItemPtr[T, K]` - Search for item by key
- `Copy() *ZeroCopySkiplist[T, K]` - Create deep copy of skiplist structure (zero-copy for items)
- `ToPwritevSlice(getItemBytes func(*T) []byte) [][]byte` - Generate byte slices for Pwritev() with custom serialization
- `ToPwritevSliceRaw() [][]byte` - Generate byte slices for Pwritev() using built-in item size function
- `First()`, `Last()` - Access boundary items
- `Length()`, `IsEmpty()` - Size information

### Navigation

- `Next()`, `Prev()` - Move through skiplist order
- `Item()` - Get pointer to original data structure
- `Key()` - Get cached key value

## Performance

The skiplist provides O(log n) performance for search and insertion operations. Maximum levels can be tuned based on expected dataset size:

- Small datasets (< 1K items): maxLevel = 8-12
- Medium datasets (1K-100K items): maxLevel = 16
- Large datasets (> 100K items): maxLevel = 20-24

## Thread Safety

All ZeroCopySkiplist operations are thread-safe:

- **Write operations** (`Insert()`, `Delete()`, `Merge()`) use write locks for exclusive access during structural modifications
- **Read operations** (`Find()`, `First()`, `Last()`, `Length()`, `IsEmpty()`, `Copy()`) use read locks for concurrent safe access
- **Multiple readers** can access the skiplist simultaneously
- **Writers are exclusive** and block all other operations during modification

This makes the skiplist safe for concurrent use across multiple goroutines without requiring external synchronization.

## Testing

```bash
go test                    # Run all tests
go test -v                 # Verbose output
go test -bench=.           # Include benchmarks
go test -short             # Skip long-running tests
```

## License

This project is dual licensed under your choice of:

- **BSD 3-Clause License** - see [LICENSE-BSD](LICENSE-BSD) file
- **GNU Lesser General Public License v2.1 or later** - see [LICENSE-LGPL](LICENSE-LGPL) file

You may use this software under the terms of either license.

## Contributing

Contributions are welcome! Please note that by contributing to this project, you agree to license your contributions under both the BSD 3-Clause License and the GNU LGPL v2.1 or later. This dual licensing approach ensures maximum compatibility and adoption of the library.

### Contribution Process

1. Fork the repository
2. Create a feature branch
3. Make your changes with appropriate tests
4. Ensure all tests pass (`go test`)
5. Submit a pull request

All contributions must:
- Include appropriate test coverage
- Follow Go conventions and formatting (`go fmt`)
- Be licensed under both the BSD 3-Clause and GNU LGPL licenses
