// connpool.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-07-05
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2019-01-17

// A concurrency-safe connection pool. It can be used to manage and reuse connections
// based on the destination address of which. This design makes a pool work better with
// some name servers.
package connpool

import (
	"net"
	"sync"
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

// push a connection to the bucket. If success, returns; otherwise,
// returns false when bucket is full or closed.
func (b *bucket) push(conn *Conn) (ok bool) {
	b.Lock()
	if !b.closed && b.size < b.capacity {
		b.top = &element{conn: conn, next: b.top}
		b.size++
		atomic.AddInt64(&b.idle, 1)

		// Not only the size of the entrie bucket will increase, but also the number
		// of elements above the element referenced by the cut field will increase.
		b.depth++
		ok = true
	}
	b.Unlock()
	return
}

// pop a connection from the bucket. If the bucket is empty or closed, returns nil.
func (b *bucket) pop() (conn *Conn) {
	b.Lock()
	if !b.closed && b.size > 0 {
		// There two cases we need to adjust the cut field to reference the
		// successor of the top one:
		//
		// 1. The cut field is nil. Which means the pop method has never been called
		//    since the last cleanup operation.
		// 2. The top element is equal to the element referenced by the cut field.
		//    Cause the top element will be returned immediately, so the cut field
		//    must move to the successor of which.
		if b.top == b.cut || b.cut == nil {
			b.cut = b.top.next
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
// connections. If the shutdown parameter is false, only releases rencently not
// used connections; otherwise, releases all.
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
		cut, b.top = *b.top, &element{}
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

// Set the bucket to the closed state; it's only used in test now.
func (b *bucket) _close() {
	b.Lock()
	b.closed = true
	b.Unlock()
}

// Iterating all elements in the bucket to compute its size; it's only
// used in test now.
func (b *bucket) _size() int {
	size := 0
	for e := b.top; e.conn != nil; e = e.next {
		size++
	}
	return size
}

// The basic element of the bucket type. Multiple elements are organized
// in linked list form.
type element struct {
	conn *Conn
	next *element
}

// An implementation of the net.Conn interface. It wraps the raw connection
// created by the dial function you register to rewrite the original Close
// method, which can make you put a connection to the pool implicitly by using
// the Close method instead of calling an pool.Put method explicitly to do
// this. The former is more natural than the latter for users.
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
