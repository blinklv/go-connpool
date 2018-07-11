// connpool_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-07-11
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2018-07-11

package connpool

import (
	"fmt"
	"github.com/bmizerany/assert"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// This is an auxiliary connection type to print some operation information.
// It satisfies net.Conn interface (But it doesn't satisfy all requirements
// in comments of each method).
type dummyConn struct {
	remoteAddr string
	localAddr  string
}

func (dc *dummyConn) Read(b []byte) (n int, err error) {
	return len(b), nil
}

func (dc *dummyConn) Write(b []byte) (n int, err error) {
	return len(b), nil
}

func (dc *dummyConn) Close() error {
	fmt.Printf("close connection (%s -> %s)\n", dc.localAddr, dc.remoteAddr)
	return nil
}

func (dc *dummyConn) LocalAddr() net.Addr {
	addr, _ := net.ResolveTCPAddr("tcp", dc.localAddr)
	return addr
}

func (dc *dummyConn) RemoteAddr() net.Addr {
	addr, _ := net.ResolveTCPAddr("tcp", dc.remoteAddr)
	return addr
}

func (dc *dummyConn) SetDeadline(t time.Time) error      { return nil }
func (dc *dummyConn) SetReadDeadline(t time.Time) error  { return nil }
func (dc *dummyConn) SetWriteDeadline(t time.Time) error { return nil }

var dummyPort int32

// A trivial implementation of the Dial type.
func dummyDial(address string) (net.Conn, error) {
	return &dummyConn{address, fmt.Sprintf("127.0.0.1:%d", atomic.AddInt32(&dummyPort, 1))}, nil
}

func TestNew(t *testing.T) {
	arguments := []struct {
		dial     Dial
		capacity int
		timeout  time.Duration
		ok       bool
	}{
		{nil, 32, 5 * time.Minute, false},
		{dummyDial, -10, 5 * time.Minute, false},
		{func(address string) (net.Conn, error) { return net.Dial("tcp", address) }, 32, time.Second, false},
		{dummyDial, 128, 5 * time.Minute, true},
	}

	for _, argument := range arguments {
		pool, err := New(argument.dial, argument.capacity, argument.timeout)
		if argument.ok {
			assert.NotEqual(t, nil, pool)
			assert.Equal(t, nil, err)
			assert.Equal(t, nil, pool.Close())
		} else {
			assert.Equal(t, (*Pool)(nil), pool)
			assert.NotEqual(t, nil, err)
		}
	}
}
