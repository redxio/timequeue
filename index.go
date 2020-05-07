package timequeue

// Indexer implements general interface for indexing data
type Indexer interface {
	// Set sets key and associated data to index
	Set(key int64, n *Node) error
	// Get finds data corresponding to key
	Get(key int64) (*Node, error)
	// Delete deletes index specified by key
	Delete(key int64) error
	// range returns a channel that can only be received, it can be used to retrieve data within the specified index range
	Range(after, before int64, boundary bool) <-chan interface{}
}
