// connpool_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2020-01-02
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2020-01-06

package connpool

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"net"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestConnRelease(t *testing.T) {
	for _, suit := range []struct {
		Parallel int     `json:"parallel"`
		FailProb float64 `json:"fail_prob"`
		DialNum  int     `json:"dial_num"`
	}{
		{1, 0, 100},
		{1, 0.1, 100},
		{16, 0.1, 100},
		{1, 0, 10000},
		{1, 0.1, 10000},
		{1, 0.5, 10000},
		{16, 0.1, 10000},
		{16, 0.2, 10000},
	} {

		var (
			d                     = &mockDialer{failProb: suit.FailProb, rsource: defaultRSource}
			b                     = &bucket{}
			conns                 = make(chan net.Conn, 64)
			dialSucc, releaseSucc int64
		)

		t.Run(encodeSuit(suit), func(t *testing.T) {
			t.Run("dial", func(t *testing.T) {
				t.Parallel()
				for i := 0; i < suit.DialNum; i++ {

					c, err := d.dial("127.0.0.1:80")
					if err != nil {
						assert.Equal(t, err, errConnectTimeout)
						continue
					}
					inc(&dialSucc)
					conns <- b.bind(c)
				}
				close(conns)
			})

			for p := 0; p < suit.Parallel; p++ {
				t.Run(sprintf("conn.release-%d", p), func(t *testing.T) {
					t.Parallel()
					for c := range conns {
						if err := c.(*Conn).Release(); err != nil {
							assert.Equal(t, err, errCloseConnFailed)
							continue
						}
						inc(&releaseSucc)
					}
				})
			}
		})

		assert.Equal(t, dialSucc-releaseSucc, b.total)
		assert.Equal(t, d.totalConn, b.total)
	}
}

var defaultRSource = rand.New(rand.NewSource(0))

// mockDialer contains options and metrics for connecting to an address.
type mockDialer struct {
	port      int64      // Local port.
	failProb  float64    // Failture probability.
	rsource   *rand.Rand // Source of random numbers.
	dialNum   int64      // The number of invoking the dial method.
	totalConn int64      // The number of current total connections used by callers.
}

var (
	errInvalidAddr     = errors.New("invalid address")
	errConnectTimeout  = errors.New("connect timeout")
	errCloseConnFailed = errors.New("close connection failed")
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
	if c.d.rsource.Float64() < c.d.failProb {
		return errCloseConnFailed
	}
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

func encodeSuit(suit interface{}) string {
	var (
		strs []string
		v    = reflect.ValueOf(suit)
		t    = v.Type()
	)

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		strs = append(strs, sprintf("%s=%v", f.Tag.Get("json"), v.Field(i).Interface()))
	}

	return strings.Join(strs, ";")
}
