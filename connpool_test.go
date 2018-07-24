// connpool_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-07-11
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2018-07-24

package connpool

import (
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// This is an auxiliary connection type to print some operation information.
// It satisfies net.Conn interface (But it doesn't satisfy all requirements
// in comments of each method).
type connection struct {
	d      *dialer
	local  net.Addr
	remote net.Addr
}

func (c *connection) Read(b []byte) (int, error) {
	return len(b), nil
}

func (c *connection) Write(b []byte) (int, error) {
	return len(b), nil
}

func (c *connection) Close() error {
	c.d.t.Logf("close connection (%s -> %s)", c.local, c.remote)
	return nil
}

func (c *connection) LocalAddr() net.Addr {
	return c.local
}

func (c *connection) RemoteAddr() net.Addr {
	return c.remote
}

func (c *connection) SetDeadline(t time.Time) error {
	return nil
}

func (c *connection) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *connection) SetWriteDeadline(t time.Time) error {
	return nil
}

type dialer struct {
	t         *testing.T
	localPort int32
	count     int64
}

func (d *dialer) Dial(address string) (net.Conn, error) {
	c := &connection{d: d}
	c.local, _ = net.ResolveTCPAddr("tcp",
		fmt.Sprintf("127.0.0.1:%d", atomic.AddInt32(&d.localPort, 1)))
	c.remote, _ = net.ResolveTCPAddr("tcp", address)
	atomic.AddInt64(&d.count, 1)
	return c, nil
}
