// connpool.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-07-05
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2018-07-26

// A concurrent safe connection pool. It can be used to manage and reuse connections
// based on the destination address of which. This design make a pool work better with
// some load balancers.
package connpool

import (
	"errors"
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
	timeout  time.Duration
	bs       map[string]*bucket
	exit     chan chan struct{}
}

// Create a connection pool. The 'dial' parameter defines how to create a new
// connection. You can't directly use the raw net.Dial function, but I think
// wrapping it on the named network (tcp, udp and unix etc) is easy. The 'capacity'
// parameter controls the maximum idle connections to keep per-host (not all hosts).
// The 'timeout' parameter is the maximum amount of time an idle connection will
// remain idle before closing itself. It can't be less than 1 min in this version;
// Otherwise, many CPU cycles are occupied by the 'clean' task. I usually set it
// to 3 ~ 5 min, but if there exist too many resident connections in your program,
// this value should be larger.
func New(dial Dial, capacity int, timeout time.Duration) (*Pool, error) {
	if dial == nil {
		return nil, fmt.Errorf("dial can't be nil")
	}

	if capacity < 0 {
		return nil, fmt.Errorf("capacity (%d) can't be less than zero", capacity)
	}

	if timeout < time.Minute {
		return nil, fmt.Errorf("timeout (%s) can't be less than 1 min", timeout)
	}

	pool := &Pool{
		dial:     dial,
		capacity: capacity,
		timeout:  timeout,
		bs:       make(map[string]*bucket),
		exit:     make(chan chan struct{}),
	}

	go pool.clean()

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
			atomic.AddInt64(&b.total, 1)
			conn = &Conn{c, b, 0}
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
		atomic.AddInt64(&b.total, 1)
		return &Conn{c, b, 0}, nil
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
			b = &bucket{capacity: pool.capacity}
			pool.bs[address] = b
		}
		pool.rwlock.Unlock()
	}
	return
}

// Clean idle connections periodically.
func (pool *Pool) clean() {
	var (
		ticker = time.NewTicker(pool.timeout)
		bs     []*bucket
	)

	for {
		select {
		case <-ticker.C:
			pool.rwlock.RLock()
			for _, b := range pool.bs {
				// If we invoke bucket's clean method in this for loop it will cause the
				// Get or the New method waiting for too long when creates a new bucket.
				bs = append(bs, b)
			}
			pool.rwlock.RUnlock()

			for _, b := range pool.bs {
				b.clean(true)
			}
		case exitDone := <-pool.exit:
			ticker.Stop()
			for _, b := range pool.bs {
				b.clean(true)
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
	top      *element
	closed   bool

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

var (
	bucketIsFull   = errors.New("bucket is full")
	bucketIsClosed = errors.New("bucket is closed")
)

// push a connection to the bucket. If the bucket is full, returns bucketIsFull.
// If the bucket is closed, returns bucketIsClosed.
func (b *bucket) push(conn *Conn) (err error) {
	b.Lock()
	if !b.closed && b.size < b.capacity {
		// When a connection is pushed to the bucket, we think it's used recently.
		// So we set the state to 1 (active). This operation can't done in the pop
		// method, because some connections are created directly instead of poping
		// from the bucket.
		conn.state = 1

		// Adjust the top and the size.
		b.top = &element{conn: conn, next: b.top}
		b.size++
		atomic.AddInt64(&b.idle, 1)
	} else if b.closed {
		// In fact, the bucket is full and closed can happen simultaneously, but
		// we think the closed state has higher priority.
		err = bucketIsClosed
	} else {
		err = bucketIsFull
	}
	b.Unlock()
	return
}

// Clean all idle connections in the stack. This operation is more expensive than
// the pop and the push method. The main problem is this operation will make other
// methods (push, pop) temporary unavailability when the list is too long. The current
// strategy is dividing this task into a number of small parts; other methods have
// a chance to get the lock between two subtasks.
func (b *bucket) clean(shutdown bool) {
	for backup := (*element)(nil); ; {
		if backup = b.iterate(backup, shutdown); backup == nil {
			break
		}

		// NOTE: The interrupt field isn't empty only in test mode.
		if b.interrupt != nil {
			done := make(chan struct{})
			b.interrupt <- done
			<-done
		}
	}

	if b.interrupt != nil {
		// NOTE: The following statements only run in test mode. So no matter
		// how complicated it is, it won't affect performance in normal cases.
		close(b.interrupt)
		b.interrupt = nil
	}
}

// Wrap the _iterate method; lock it and initialize the backup parameter.
func (b *bucket) iterate(backup *element, shutdown bool) *element {
	b.Lock()
	defer b.Unlock()

	// The 'closed' field of a bucket will be set to true only when the Close
	// method of the pool is invoked. The following assignment is placed in the
	// bucket.clean method may be better from the view of semantics, but if we
	// do that, we need some additional works (add lock) to protect it. However,
	// these works can be omitted in this method.
	b.closed = shutdown

	if backup == nil {
		if b.top == nil {
			return nil
		}
		backup = b.top
	}

	return b._iterate(backup, shutdown)
}

// Iterate each connection in the bucket. If the connection's state is active,
// reset it to idle and push it to the temporary bucket. Otherwise, skip it (of
// course also release it) and descrease the original bucket's size.
func (b *bucket) _iterate(backup *element, shutdown bool) *element {
	// Invariant: the backup parameter isn't nil.
	var (
		top, tail *element
		i         int
	)

	// We only handle the first 16 connection at once; this will prevent this
	// loop costs so much time when the list is too long.
	for tail, i = backup, 0; backup != nil && i < 16; backup, i = backup.next, i+1 {
		if !shutdown && backup.conn.state == 1 {
			backup.conn.state = 0
			// NOTE: Because we push the active connection to the temporary bucket.
			// which will reverse the elements order in the original bucket..
			top = &element{conn: backup.conn, next: top}
		} else {
			b.size--
			atomic.AddInt64(&b.idle, -1)
			backup.conn.Release()
		}
	}
	b.top, tail.next = top, b.top
	return backup
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
	for top := b.top; top != nil; top = top.next {
		size++
	}
	return size
}

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

	// Represents whether the current connection is active or idle. 0 means
	// idle and 1 means active. This field is only meaningful to the correspond
	// bucket. The connection itself doesn't access it.
	state int32
}

// If there is enough room in the connection pool to place this connection,
// push it to the pool. Otherwise, release this connection directly.
func (conn *Conn) Close() error {
	if err := conn.b.push(conn); err != nil {
		return conn.Release()
	}
	return nil
}

// Release the underlying connection directly.
func (conn *Conn) Release() error {
	atomic.AddInt64(&conn.b.total, -1)
	return conn.Conn.Close()
}
