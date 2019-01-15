// connpool.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-07-05
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2019-01-15

// A concurrency-safe connection pool. It can be used to manage and reuse connections
// based on the destination address of which. This design makes a pool work better with
// some name servers.
package connpool

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// This type defines how to connect to the address. The purpose of designing this
// type is to serve the Pool struct. It's not as common as net.Dial that you can
// specify a network type, which means the type of connections in one pool should
// be same. In fact, I don't think caching different kinds of connections in the
// same pool is a good idea, although they all satisfy the net.Conn interface. All
// network types defined in https://golang.org/pkg/net/#Dial are not completely
// independent, like tcp4, tcp6 and tcp, the latter includes the former two. There
// will be some extra works to detect network types passed by callers when users
// get connections from a pool, which sacrifices performance. Dividing various kinds
// of connections into different pools won't be harder for users and more efficient.
type Dial func(address string) (net.Conn, error)

// Pool is a connection pool. It will cache some connections for each address.
// If a connection is never be used for a long time (mark it as idle), it will
// be cleaned.
type Pool struct {
	rwlock   sync.RWMutex
	dial     Dial
	capacity int
	period   time.Duration
	bs       map[string]*bucket
	exit     chan chan struct{}
}

// Create a connection pool. The dial parameter defines how to create a new
// connection. You can't directly use the raw net.Dial function, but I think
// wrapping it on the named network (tcp, udp and unix etc) is easy, as follows:
//
//  func(address string) (net.Conn, error) {
//      return net.Dial("tcp", address)
//  }
//
// The capacity parameter controls the maximum idle connections to keep per-host
// (not all hosts). The period parameter specifies period between two cleanup ops,
// which closes some idle connections not used for a long time (about 1~2 period).
// It can't be less than 1 min in this version; otherwise, many CPU cycles are
// occupied by the cleanup task. I usually set it to 3 ~ 5 min, but if there exist
// too many resident connections in your program, this value should be larger.
func New(dial Dial, capacity int, period time.Duration) (*Pool, error) {
	if dial == nil {
		return nil, fmt.Errorf("dial can't be nil")
	}

	if capacity < 0 {
		return nil, fmt.Errorf("capacity (%d) can't be less than zero", capacity)
	}

	if period < time.Minute {
		return nil, fmt.Errorf("cleanup period (%s) can't be less than 1 min", period)
	}

	pool := &Pool{
		dial:     dial,
		capacity: capacity,
		period:   period,
		bs:       make(map[string]*bucket),
		exit:     make(chan chan struct{}),
	}
	// The cleanup method runs in a new goroutine. In fact, the primary objective of
	// designing the Close method is stopping it to prevent resource leak.
	go pool.cleanup()

	return pool, nil
}

// Get a connection from the pool, the destination address of which is
// equal to the address parameter.
func (pool *Pool) Get(address string) (net.Conn, error) {
	// First, get a connection bucket.
	b := pool.selectBucket(address)

	// Second, get a connection from the bucket.
	conn := b.pop()
	if conn == nil {
		// If there is no idle connection in this bucket, we need to invoke the
		// dial function to create a new connection and bind it to the bucket.
		if c, err := pool.dial(address); err == nil {
			conn = b.bind(c)
		} else {
			return nil, err
		}
	}

	return conn, nil
}

// Create a new connection by using the underlying dial field, and bind it to a bucket.
func (pool *Pool) New(address string) (net.Conn, error) {
	b := pool.selectBucket(address)
	if c, err := pool.dial(address); err == nil {
		return b.bind(c), nil
	} else {
		return nil, err
	}
}

// Close the connection pool. It will release all idle connections in the pool. You
// shouldn't use this pool anymore after this method has been called.
func (pool *Pool) Close() error {
	exitDone := make(chan struct{})
	pool.exit <- exitDone
	<-exitDone
	return nil
}

// Pool's statistical data. You can get it from Pool.Stats method.
type Stats struct {
	// Timestamp identifies when this statistical was generated. It equals
	// to the number of seconds elapsed since January 1, 1970 UTC.
	Timestamp int64 `json:"timestamp"`

	// Destinations collects all destination statistical data related to the pool.
	Destinations []DestinationStats `json:"destinations"`
}

// Destination's statistical data.
type DestinationStats struct {
	// Address identifies a destination, the format of which usually likes
	// ip:port. In fact, you can not be confined to this format and specify
	// any only and meaningful value to it.
	Address string `json:"address"`

	// The total number of connections related to this destination, which
	// contains the number of active connections and idle connections.
	Total int64 `json:"total"`

	// The number of idle connections related to this destination.
	Idle int64 `json:"idle"`
}

// Get a statistical data slice of the Pool.
func (pool *Pool) Stats() *Stats {
	stats := &Stats{
		Timestamp: time.Now().Unix(),
	}

	pool.rwlock.RLock()
	defer pool.rwlock.RUnlock()

	stats.Destinations = make([]DestinationStats, 0, len(pool.bs))
	for address, b := range pool.bs {
		// We needn't add the lock to protect 'total' and 'idle' field of a
		// bucket; any operation on them is atomic.
		stats.Destinations = append(stats.Destinations, DestinationStats{
			Address: address,
			Total:   atomic.LoadInt64(&b.total),
			Idle:    atomic.LoadInt64(&b.idle),
		})
	}

	return stats
}

// Select a bucket for the address. If it doesn't exist, create a new one.
func (pool *Pool) selectBucket(address string) (b *bucket) {
	// Get a bucket from the bucket map.
	pool.rwlock.RLock()
	b = pool.bs[address]
	pool.rwlock.RUnlock()

	// This condition statement can save much time in most cases. Because
	// the bucket for this address has already existed in normal case.
	// Otherwise, we have to add write-lock every time.
	if b == nil {
		pool.rwlock.Lock()
		// If the bucket for this address doesn't exist, we need to create a
		// new one and add it to the bucket map.
		//
		// NOTE: We need to check whether there has already existed a bucket for
		// this address again. The outer statement 'if b == nil' can't guarantee
		// the bucket doesn't exist at this point.
		if b = pool.bs[address]; b == nil {
			b = &bucket{capacity: pool.capacity, top: bottom}
			pool.bs[address] = b
		}
		pool.rwlock.Unlock()
	}
	return
}

// Clean idle connections periodically.
func (pool *Pool) cleanup() {
	var (
		ticker = time.NewTicker(pool.period)
		bs     []*bucket
	)

	for {
		select {
		case <-ticker.C:
			pool.rwlock.RLock()
			for _, b := range pool.bs {
				// If we invoke bucket's cleanup method in this for loop it will cause the
				// Get or the New method waiting for too long when creates a new bucket.
				bs = append(bs, b)
			}
			pool.rwlock.RUnlock()

			for _, b := range pool.bs {
				b.cleanup(false)
			}
		case exitDone := <-pool.exit:
			ticker.Stop()
			for _, b := range pool.bs {
				b.cleanup(true)
			}
			close(exitDone)
		}
	}
}

// bucket is a collection of connections, the internal structure of which is
// a linked list which implements some operations related to the stack.
type bucket struct {
	sync.Mutex
	size     int
	capacity int
	closed   bool
	top      *element

	// The cut field records the successor of the element popped, which has
	// the max depth between the two adjacent cleanup task; the depth field
	// records the number of elements before (closer to the stack top) the
	// element referenced by the cut field (it's 0 when the cut field is nil).
	cut   *element
	depth int

	// The following fields are related to statistics, and the sync.Mutex doesn't
	// protect them. So any operation on them should be atomic.
	total int64 // The total number of connections related to this bucket.
	idle  int64 // The number of idle connections in the bucket.

	// The 'interrupt' field is only used in test mode; it will be nil
	// in normal cases.
	interrupt chan chan struct{}
}

// pop a connection from the bucket. If the bucket is empty or closed, returns nil.
func (b *bucket) pop() (conn *Conn) {
	b.Lock()
	if b.size > 0 && !b.closed {
		// There two cases we need to adjust the cut field to reference the
		// next element of the top one:
		//
		// 1. The cut field is nil. Which means the pop method has never been called
		//    since the last cleanup opeartion, so we need to initialize it.
		// 2. The top element is equal to the element that the cut field references
		//    to. Cause the top element will be returned immeidatelly, so the cut
		//    field must move to the next one of which.
		if b.top == b.cut || b.cut == nil {
			b.cut = b.top.next
		}

		conn, b.top = b.top.conn, b.top.next
		b.size--

		// Although this statement is in the critical region, the 'idle' field is not
		// protected by the sync.Mutex and can be accessed by outer caller directly.
		// So we need to use the following atomic operation to handle it.
		atomic.AddInt64(&b.idle, -1)
	}
	b.Unlock()
	return
}

// push a connection to the bucket. If success, returns ture; otherwise,
// returns false (bucket is full or closed).
func (b *bucket) push(conn *Conn) (ok bool) {
	b.Lock()
	if !b.closed && b.size < b.capacity {
		b.top = &element{conn: conn, next: b.top}
		// Not only the size of the entrie bucket will increase, but also the number
		// of elements before the element referenced by the cut field will increase.
		b.size++
		b.depth++
		atomic.AddInt64(&b.idle, 1)
		ok = true
	}
	b.Unlock()
	return
}

// When a connection is created, we should bind it to the specific bucket.
func (b *bucket) bind(c net.Conn) *Conn {
	atomic.AddInt64(&b.total, 1)
	return &Conn{c, b}
}

func (b *bucket) cleanup(shutdown bool) int {
	var (
		cut    element
		unused int
	)

	b.Lock()
	if !shutdown && b.cut != nil {
		cut, b.cut = *b.cut, nil
	} else {
		cut, b.top = *b.top, bottom
	}
	b.size, b.depth = b.depth, 0
	atomic.StoreInt64(&b.idle, int64(b.size))
	b.Unlock()

	for e := &cut; e.conn != nil; e = e.next {
		e.conn.Release()
		unused++
	}

	return unused
}

// Set the bucket to the closed state; it's only used in test now.
func (b *bucket) _close() {
	b.Lock()
	b.closed = true
	defer b.Unlock()
}

// Iterating all elements in the bucket to compute its size; it's only
// used in test now.
func (b *bucket) _size() int {
	size := 0
	for top := b.top; top != bottom; top = top.next {
		size++
	}
	return size
}

var bottom = &element{}

// The basic element of the bucket.
type element struct {
	conn *Conn
	next *element
}

// Conn is an implementation of the net.Conn interface. It just wraps the raw
// connection directly, which rewrites the original Close method.
type Conn struct {
	// The raw connection created by the dial function which you register.
	net.Conn

	// The bucket to which this connection binds.
	b *bucket
}

// If there is enough room in the connection pool to place this connection,
// push it to the pool. Otherwise, release this connection directly.
func (conn *Conn) Close() error {
	if !conn.b.push(conn) {
		return conn.Release()
	}
	return nil
}

// Release the underlying connection directly.
func (conn *Conn) Release() error {
	atomic.AddInt64(&conn.b.total, -1)
	return conn.Conn.Close()
}
