// connpool.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-07-05
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2018-07-05

package connpool

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Pool is a connection pool. It will cache some connections for each address.
// If a connection is never be used for a long time (mark it as idle), it will
// be cleaned.
type Pool struct {
	rwlock sync.RWMutex
}

func (pool *Pool) Get(address string) (net.Conn, error) {
}

func (pool *Pool) New(address string) (net.Conn, error) {
}

func (pool *Pool) Close() error {
}

// bucket is a collection of connections, the internal structure of which is
// a linked list which implements some operations related to the stack.
type bucket struct {
	sync.Mutex
	size     int
	capacity int
	top      *element
	pool     *Pool
}

func (b *bucket) pop() (conn *Conn) {
}

func (b *bucket) push(conn *Conn) {
}

func (b *bucket) clean(shutdown bool) {
}

// The basic element of the bucket.
type element struct {
	conn *connection
	next *element
}

// Conn is an implementation of the net.Conn interface. It just wraps the raw
// connection directly, which rewrites the original Close method.
type Conn struct {
	// The raw connection created by the dial function which you
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
	return conn.Conn.Close()
}
