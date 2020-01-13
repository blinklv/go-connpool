// connpool_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2020-01-02
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2020-01-13

package connpool

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBucketPush(t *testing.T) {
	for _, cs := range []struct {
		Parallel     int  `json:"parallel"`
		BucketCap    int  `json:"bucket_cap"`
		BucketClosed bool `json:"bucket_closed"`
		DialNum      int  `json:"dial_num"`
	}{
		{1, 0, false, 10000},
		{1, 0, true, 10000},
		{1, 256, false, 10000},
		{1, 256, true, 10000},
		{16, 0, false, 10000},
		{16, 0, true, 10000},
		{16, 256, false, 10000},
		{16, 256, true, 10000},
		{16, 1, false, 10000},
	} {
		var (
			d        = &mockDialer{}
			b        = &bucket{capacity: cs.BucketCap, closed: cs.BucketClosed, top: &element{}}
			n        = int64(cs.DialNum)
			pushSucc int64
		)

		t.Run(encodeCase(cs), func(t *testing.T) {
			for p := 0; p < cs.Parallel; p++ {
				t.Run(sprintf("bucket.push-%d", p), func(t *testing.T) {
					t.Parallel()
					for dec(&n) >= 0 {
						c, err := d.dial("127.0.0.1:80")
						assert.Equal(t, err, nil)
						bc := b.bind(c)

						if b.push(bc) {
							inc(&pushSucc)
						} else {
							bc.Release()
						}
					}
				})
			}
		})

		assert.Equal(t, b._size(), b.size)
		assert.Equal(t, b._size(), int(b.idle))
		assert.Equal(t, b._depth(), int(b.depth))
		assert.Equal(t, b.size, int(b.depth))
		assert.Equal(t, pushSucc, b.idle)
		assert.Equal(t, pushSucc, b.total)
		assert.Equal(t, d.totalConn, b.total)
		if cs.BucketClosed {
			assert.Equal(t, int64(0), b.idle)
		} else {
			assert.LessOrEqual(t, b.idle, int64(cs.BucketCap))
		}
	}
}

func TestConnClose(t *testing.T) {
	for _, cs := range []struct {
		Parallel     int     `json:"parallel"`
		FailProb     float64 `json:"fail_prob"`
		BucketCap    int     `json:"bucket_cap"`
		BucketClosed bool    `json:"bucket_closed"`
		DialNum      int     `json:"dial_num"`
	}{
		{1, 0, 0, false, 10000},
		{1, 0, 0, true, 10000},
		{1, 0, 256, false, 10000},
		{1, 0, 256, true, 10000},
		{1, 0.2, 0, false, 10000},
		{1, 0.2, 0, true, 10000},
		{1, 0.2, 256, false, 10000},
		{1, 0.2, 256, true, 10000},
		{16, 0, 0, false, 10000},
		{16, 0, 0, true, 10000},
		{16, 0, 256, false, 10000},
		{16, 0, 256, true, 10000},
		{16, 0.2, 0, false, 10000},
		{16, 0.2, 0, true, 10000},
		{16, 0.2, 256, false, 10000},
		{16, 0.2, 256, true, 10000},
		{16, 0, 256, false, 100},
		{16, 0.2, 256, false, 100},
	} {
		var (
			d                   = &mockDialer{failProb: cs.FailProb}
			b                   = &bucket{capacity: cs.BucketCap, closed: cs.BucketClosed, top: &element{}}
			conns               = make(chan net.Conn, 64)
			dialSucc, closeSucc int64
		)

		t.Run(encodeCase(cs), func(t *testing.T) {
			t.Run("dial", func(t *testing.T) {
				t.Parallel()
				for i := 0; i < cs.DialNum; i++ {

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

			for p := 0; p < cs.Parallel; p++ {
				t.Run(sprintf("conn.close-%d", p), func(t *testing.T) {
					t.Parallel()
					for c := range conns {
						if err := c.Close(); err != nil {
							assert.Equal(t, err, errCloseConnFailed)
							continue
						}
						inc(&closeSucc)
					}
				})
			}
		})

		assert.Equal(t, (dialSucc-closeSucc)+b.idle, b.total)
		assert.Equal(t, d.totalConn, b.total)
		if cs.BucketClosed {
			assert.Equal(t, int64(0), b.idle)
		} else {
			assert.LessOrEqual(t, b.idle, int64(cs.BucketCap))
		}
	}
}

func TestConnRelease(t *testing.T) {
	for _, cs := range []struct {
		Parallel int     `json:"parallel"`
		FailProb float64 `json:"fail_prob"`
		DialNum  int     `json:"dial_num"`
	}{
		{1, 0, 100},
		{1, 0.2, 100},
		{16, 0, 100},
		{16, 0.2, 100},
		{1, 0, 10000},
		{1, 0.2, 10000},
		{16, 0, 10000},
		{16, 0.2, 10000},
	} {

		var (
			d                     = &mockDialer{failProb: cs.FailProb}
			b                     = &bucket{}
			conns                 = make(chan net.Conn, 64)
			dialSucc, releaseSucc int64
		)

		t.Run(encodeCase(cs), func(t *testing.T) {
			t.Run("dial", func(t *testing.T) {
				t.Parallel()
				for i := 0; i < cs.DialNum; i++ {

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

			for p := 0; p < cs.Parallel; p++ {
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

// mockDialer contains options and metrics for connecting to an address.
type mockDialer struct {
	port      int64   // Local port.
	failProb  float64 // Failture probability.
	dialNum   int64   // The number of invoking the dial method.
	totalConn int64   // The number of current total connections used by callers.
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

	if rand.Float64() < d.failProb {
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
	if rand.Float64() < c.d.failProb {
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

func encodeCase(cs interface{}) string {
	var (
		strs []string
		v    = reflect.ValueOf(cs)
		t    = v.Type()
	)

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		strs = append(strs, sprintf("%s=%v", f.Tag.Get("json"), v.Field(i).Interface()))
	}

	return strings.Join(strs, ";")
}
