// connpool.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-07-05
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2018-07-09

package connpool

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

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
		exit:     make(chan struct{}),
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
		return Conn{c, b, 0}, nil
	} else {
		return nil, err
	}
}

func (pool *Pool) Close() error {
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
			b = &bucket{capacity: pool.capacity, pool: pool}
			pool.bs[address] = b
		}
		pool.rwlock.Unlock()
	}
	return
}

func (pool *Pool) clean() {
}

// bucket is a collection of connections, the internal structure of which is
// a linked list which implements some operations related to the stack.
type bucket struct {
	sync.Mutex
	size     int
	capacity int
	top      *element
	pool     *Pool

	// The following fields are related to statistics, and the sync.Mutex doesn't
	// protect them. So any operation on them should be atomic.
	total int64 // The total number of connections related to this bucket.
	idle  int64 // The number of idle connections in the bucket.
}

// pop a connection from the bucket. If the bucket is empty, returns nil.
func (b *bucket) pop() (conn *Conn) {
	b.Lock()
	if stack.size > 0 {
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

var bucketIsFull = errors.New("bucket is full")

// push a connection to the bucket. If the bucket is full, returns bucketIsFull.
func (b *bucket) push(conn *Conn) (err error) {
	b.Lock()
	if b.size < b.capacity {
		// When a connection is pushed to the bucket, we think it's used recently.
		// So we set the state to 1 (active). This operation can't done in the pop
		// method, because some connections are created directly instead of poping
		// from the bucket.
		conn.state = 1

		// Adjust the top and the size.
		b.top = &element{conn: conn, next: b.top}
		b.size++
		atomic.AddInt64(&b.idle, 1)
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
			return
		}
	}
}

// Wrap the _iterate method; lock it and initialize the backup parameter.
func (b *bucket) iterate(backup *element, shutdown bool) *element {
	b.Lock()
	defer b.Unlock()

	if backup == nil {
		if b.top != nil {
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
			conn.Release()
		}
	}
	b.top, tail.next = top, b.top
	return backup
}

// The basic element of the bucket.
type element struct {
	conn *connection
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
