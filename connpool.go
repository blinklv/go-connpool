// connpool.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-07-05
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2019-12-31

// Package connpool implements a concurrency-safe connection pool. It can be used to
// manage and reuse connections based on the destination address of which. This design
// makes a pool work better with some name servers.
package connpool

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Control whether the package runs in testing mode. The name of all variables, fields,
// functions, and methods related to testing will have an underscore ('_') prefix.
var _test = false

// Dial defines how to connect to the address. The purpose of designing this
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
// If a connection has never been used for a long time (mark it as idle), it
// will be released.
type Pool struct {
	rw       sync.RWMutex
	dial     Dial
	capacity int
	period   time.Duration
	buckets  map[string]*bucket
	exit     chan chan struct{}
	closed   bool

	// _interrupt channel is used to notify users the connection pool has
	// been cleaned once. (**only used in testing mode**)
	_interrupt chan chan struct{}
}

// New creates a connection pool. The dial parameter defines how to create a new
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

	// NOTE: period can be less than 1 min in testing mode :)
	if !_test && period < time.Minute {
		return nil, fmt.Errorf("cleanup period (%s) can't be less than 1 min", period)
	}

	pool := &Pool{
		dial:     dial,
		capacity: capacity,
		period:   period,
		buckets:  make(map[string]*bucket),
		exit:     make(chan chan struct{}),
	}

	if _test {
		pool._interrupt = make(chan chan struct{})
	}

	// The autoCleanup method runs in a new goroutine. In fact, the primary reason
	// of designing the Closed method is stopping it to prevent resource leak.
	go pool.autoCleanup()

	return pool, nil
}

// Get a connection from the pool, the destination address of which is equal to
// the address parameter. If an error happens, the connection returned is nil.
func (pool *Pool) Get(address string) (net.Conn, error) {
	// First, get a connection bucket.
	b := pool.selectBucket(address)

	// Second, get a connection from the bucket.
	conn := b.pop()
	if conn == nil {
		// If there is no idle connection in the bucket, we need to invoke the
		// dial function to create a new connection and bind it to the bucket.
		if c, err := pool.dial(address); err == nil {
			conn = b.bind(c)
		} else {
			return nil, err
		}
	}

	return conn, nil
}

// New creates a new connection by using the underlying dial function you register
// and bind it to the specific bucket.
func (pool *Pool) New(address string) (net.Conn, error) {
	c, err := pool.dial(address)
	if err != nil {
		return nil, err
	}
	return pool.selectBucket(address).bind(c), nil
}

// Close the connection pool. It will release all idle connections in the pool.
// You shouldn't use this pool anymore after this method has been called.
func (pool *Pool) Close() (err error) {
	pool.rw.Lock()
	if !pool.closed {
		pool.closed = true
	} else {
		err = fmt.Errorf("connection pool is already closed")
	}

	// Although there is no code to acquire the lock explicitly after the following
	// statement, the cleanup task will be triggered to do it implicitly. So the
	// lock must be released at this point to avoid deadlock.
	pool.rw.Unlock()

	if err == nil {
		// I must guarantee the following statements are only executed once; otherwise,
		// it will be blocked forever at the second time (3re, 4th, ...) cause there is
		// no reader of the pool.exit channel.
		done := make(chan struct{})
		pool.exit <- done
		<-done
	}
	return
}

// Stats represents the statistical data of Pool. You can get it from Pool.Stats method.
type Stats struct {
	// Timestamp identifies when this statistical was generated. It equals
	// to the number of seconds elapsed since January 1, 1970 UTC.
	Timestamp int64 `json:"timestamp"`

	// Destinations collects all destination statistical data related to the pool.
	Destinations []DestinationStats `json:"destinations"`
}

// DestinationStats represents the statistical data of each backend server.
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

// Stats returns the current statistical data of the calling Pool.
func (pool *Pool) Stats() *Stats {
	stats := &Stats{
		Timestamp: time.Now().Unix(),
	}

	pool.rw.RLock()
	defer pool.rw.RUnlock()

	stats.Destinations = make([]DestinationStats, 0, len(pool.buckets))
	for address, b := range pool.buckets {
		// We needn't add the lock to protect the total and the idle field of a
		// bucket; any operation on them is atomic.
		stats.Destinations = append(stats.Destinations, DestinationStats{
			Address: address,
			Total:   atomic.LoadInt64(&b.total),
			Idle:    atomic.LoadInt64(&b.idle),
		})
	}
	return stats
}

// select a bucket for the address. If it doesn't exist, create a new one; which
// means the return value of this function can't be nil.
func (pool *Pool) selectBucket(address string) (b *bucket) {
	// At first, get a bucket from the buckets.
	pool.rw.RLock()
	b = pool.buckets[address]
	pool.rw.RUnlock()

	// This conditional statement can save much time in most cases. Because the bucket
	// for the address has already existed in normal case; otherwise, we have to add
	// write-lock every time.
	if b == nil {
		pool.rw.Lock()
		// If the bucket for this address doesn't exist, we need to create a
		// new one and add it to the bucket map.
		//
		// NOTE: We need to check whether there has already existed a bucket for
		// this address again. The outer statement 'if b == nil' can't guarantee
		// the bucket doesn't exist at this point.
		if b = pool.buckets[address]; b == nil {
			b = &bucket{capacity: pool.capacity, closed: pool.closed, top: &element{}}
			pool.buckets[address] = b
		}
		pool.rw.Unlock()
	}
	return
}

// Cleans up idle connections periodically.
func (pool *Pool) autoCleanup() {
	timer := time.NewTimer(pool.period)
	for {
		select {
		case <-timer.C:
			pool.cleanup(false)

			if _test {
				pool._wait()
			}

			timer.Reset(pool.period)
		case done := <-pool.exit:
			timer.Stop()
			pool.cleanup(true)
			close(done)
			return
		}
	}
}

// Cleans up idle connections once.
func (pool *Pool) cleanup(shutdown bool) {
	pool.rw.RLock()
	var buckets = make([]*bucket, 0, len(pool.buckets))
	// If we invokes the bucket.cleanup method in this for-loop, which
	// will cause the Get or the New method waiting for too long when
	// creates a new bucket.
	for _, b := range pool.buckets {
		buckets = append(buckets, b)
	}
	pool.rw.RUnlock()

	for _, b := range buckets {
		b.cleanup(shutdown)
	}
}

// _wait method sends a signal to users that the pool has been cleaned once,
// and waits users finish their works. (**only used in testing mode**)
func (pool *Pool) _wait() {
	back := make(chan struct{})
	pool._interrupt <- back
	<-back
}

// Returns the number of idle connections of the pool. (**only used in testing mode**)
func (pool *Pool) _size() (size int) {
	for _, b := range pool.buckets {
		size += b.size
	}
	return
}

// bucket is a collection of connections, the internal structure of which is
// a linked list which implements some operations related to the stack.
type bucket struct {
	sync.Mutex
	size     int
	capacity int
	closed   bool

	// The head pointer of the linked list. I name it to 'top' cause I operate
	// the linked list as a stack. It will reference an empty element when
	// initialize.
	top *element

	// The cut field records the successor of the popped element which has
	// the max depth between the two adjacent cleanup task; the depth field
	// records the number of elements above the element referenced by the
	// cut field (it's 0 when the cut field is nil).
	cut   *element
	depth int

	// The following fields are related to statistics, and the sync.Mutex doesn't
	// protect them. So any operation on them should be atomic.
	total int64 // The total number of connections related to this bucket.
	idle  int64 // The number of idle connections in the bucket.
}

// Puts a connection to the top of the bucket. If success, returns true; otherwise,
// returns false when bucket is full or closed.
func (b *bucket) push(conn *Conn) (ok bool) {
	b.Lock()
	if !b.closed && b.size < b.capacity {
		if b.cut == nil {
			b.cut = b.top
		}
		// The cut field has already been initialized (no matter by the bucket.push
		// method or the bucket.pop method) at this point; the number of elements
		// above the element referenced by which will increase.
		b.depth++

		b.top = &element{conn: conn, next: b.top}
		b.size++
		atomic.AddInt64(&b.idle, 1)
		ok = true
	}
	b.Unlock()
	return
}

// Removes the top connection of the bucket and returns it to users. If the bucket is
// empty or closed, returns nil.
func (b *bucket) pop() (conn *Conn) {
	b.Lock()
	if !b.closed && b.size > 0 {
		// There are two cases we need to adjust the cut field to reference the
		// successor of the top one:
		//
		// 1. The cut field is nil. Which means the pop and the push method have
		//    never been called since the last cleanup operation.
		// 2. The top element is equal to the element referenced by the cut field.
		//    Cause the top element will be returned immediately, so the cut field
		//    must move to the successor of which.
		if b.top == b.cut || b.cut == nil {
			b.cut = b.top.next
		}

		if b.depth > 0 {
			b.depth--
		}

		conn, b.top = b.top.conn, b.top.next
		b.size--
		atomic.AddInt64(&b.idle, -1)
	}
	b.Unlock()
	return
}

// A connection should be bound to the specific bucket when it's created.
func (b *bucket) bind(conn net.Conn) *Conn {
	atomic.AddInt64(&b.total, 1)
	return &Conn{conn, b}
}

// Cleans up the idle connections of the bucket and returns the number of closed
// connections. If the shutdown parameter is false, only releases connections not
// used rencently; otherwise, releases all.
func (b *bucket) cleanup(shutdown bool) (unused int) {
	var cut element

	b.Lock()
	b.closed = shutdown

	// I use an empty element to represent the end of a linked list; it's a mark
	// node which tells you that you reach the end. The primary reason I don't use
	// the nil to represent the end is distinguishing it from the beginning.
	if !shutdown && b.cut != nil {
		cut, *b.cut = *b.cut, element{}
	} else {
		cut, b.top, b.depth = *b.top, &element{}, 0
	}

	// The element referenced by the cut field and elements below it will be
	// released, so the number of remaining connections is equal to the depth
	// of the last cycle. We also need to reset the cut field and the depth
	// field to nil and zero respectively to prepare for the next cycle.
	b.size, b.cut, b.depth = b.depth, nil, 0
	atomic.StoreInt64(&b.idle, int64(b.size))
	b.Unlock()

	for e := &cut; e.conn != nil; e = e.next {
		e.conn.Release()
		unused++
	}

	return
}

// Set the bucket to the closed state. (**only used in testing mode**)
func (b *bucket) _close() {
	b.Lock()
	b.closed = true
	b.Unlock()
}

// Iterating all elements to compute its size. (**only used in testing mode**)
func (b *bucket) _size() int {
	size := 0
	for e := b.top; e.conn != nil; e = e.next {
		size++
	}
	return size
}

// Computing the size of all elements above the element referenced by the
// cut field. (**only used in testing mode**)
func (b *bucket) _depth() int {
	depth := 0
	for e := b.top; b.cut != nil && e != b.cut; e = e.next {
		depth++
	}
	return depth
}

// The basic element of the bucket type. Multiple elements are organized
// in linked list form.
type element struct {
	conn *Conn
	next *element
}

// Conn is an implementation of the net.Conn interface. It wraps the raw connection
// created by the dial function you register to rewrite the original Close method,
// which can make you put a connection to the pool implicitly by using the Close
// method instead of calling an pool.Put method explicitly to do this. The former
// is more natural than the latter for users.
type Conn struct {
	net.Conn         // The raw connection created by the dial function you register.
	b        *bucket // The bucket to which this connection binds.
}

// Close the connection. If the pool which this connection binds to isn't
// closed and has enough room, puts the connection to the pool; otherwise,
// release the underlying connection directly (call the raw Close method).
func (conn *Conn) Close() error {
	if !conn.b.push(conn) {
		return conn.Release()
	}
	return nil
}

// Release the underlying connection. It will call the Close method of the
// net.Conn field. Although you can invoke the Close method of the net.Conn
// field (exported) by yourself, I don't recommend you do this, cause the
// Release method will do some extra works.
func (conn *Conn) Release() error {
	atomic.AddInt64(&conn.b.total, -1)
	return conn.Conn.Close()
}
