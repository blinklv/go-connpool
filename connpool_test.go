// connpool_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2020-01-02
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2020-01-03

package connpool

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync/atomic"
	"time"
)

// mockDialer contains options and metrics for connecting to an address.
type mockDialer struct {
	port      int64      // Local port.
	failProb  float64    // Failture probability.
	rsource   *rand.Rand // Source of random numbers.
	dialNum   int64      // The number of invoking the dial method.
	totalConn int64      // The number of current total connections used by callers.
}

var (
	errInvalidAddr    = errors.New("invalid address")
	errConnectTimeout = errors.New("connect timeout")
)

// dial connects to the address. The underlying type of the returned net.Conn is MockConn.
func (d *mockDialer) dial(address string) (net.Conn, error) {
	remote, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, errInvalidAddr
	}

	if d.rsource.Float64() < d.failProb {
		return nil, errConnectTimeout
	}

	local, _ := net.ResolveTCPAddr("tcp", sprintf("127.0.0.1:%d", inc(&d.port)))

	inc(&d.dialNum)
	inc(&d.totalConn)

	return &mockConn{
		d:      d,
		local:  local,
		remote: remote,
	}, nil
}

// mockConn is an implementation of the net.Conn interface, which can record
// some quantity information to verify the correctness of the connection pool.
type mockConn struct {
	d             *mockDialer
	local, remote net.Addr
}

func (c *mockConn) Read(b []byte) (int, error) {
	return len(b), nil
}

func (c *mockConn) Write(b []byte) (int, error) {
	return len(b), nil
}

func (c *mockConn) Close() error {
	dec(&c.d.totalConn)
	return nil
}

func (c *mockConn) LocalAddr() net.Addr {
	return c.local
}

func (c *mockConn) RemoteAddr() net.Addr {
	return c.remote
}

func (c *mockConn) SetDeadline(t time.Time) error {
	return nil
}

func (c *mockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}

var sprintf = fmt.Sprintf

// inc atomically adds 1 to *addr and returns the new value.
func inc(addr *int64) int64 {
	return atomic.AddInt64(addr, 1)
}

// dec atomically subtracts 1 to *addr and returns the new value.
func dec(addr *int64) int64 {
	return atomic.AddInt64(addr, -1)
}
