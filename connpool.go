// connpool.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-07-05
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2019-01-16

package connpool

import (
	"net"
)

// The bottom variable represents the end of a linked list; it's a mark node
// which tells you that you reach the end. The primary reason I don't use the
// nil to represent the end is distinguishing it from the beginning.
var bottom = &element{}

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
