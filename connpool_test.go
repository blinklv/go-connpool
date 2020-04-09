// connpool_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2020-01-02
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2020-04-09

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

func TestNew(t *testing.T) {
	for _, cs := range []struct {
		Dial     Dial          `json:"dial"`
		Capacity int           `json:"capacity"`
		Period   time.Duration `json:"period"`
		Error    string        `json:"error"`
	}{
		{nil, 32, 5 * time.Minute, "dial can't be nil"},
		{(&mockDialer{}).dial, -10, 5 * time.Minute, "capacity (-10) can't be less than zero"},
		{(&mockDialer{}).dial, 32, time.Second, "cleanup period (1s) can't be less than 1 min"},
		{(&mockDialer{}).dial, 32, 5 * time.Minute, "nil"},
	} {
		t.Run(encodeCase(cs), func(t *testing.T) {
			pool, err := New(cs.Dial, cs.Capacity, cs.Period)
			if cs.Error == "nil" {
				assert.NotNil(t, pool)
				assert.Nil(t, err)
				assert.NotNil(t, pool.dial)
				assert.GreaterOrEqual(t, pool.capacity, 0)
				assert.NotNil(t, pool.buckets)
				assert.NotNil(t, pool.exit)
				assert.False(t, pool.closed)
			} else {
				assert.Nil(t, pool)
				assert.EqualError(t, err, cs.Error)
			}
		})
	}
}

func TestPoolNew(t *testing.T) {
	for _, cs := range []struct {
		Parallel int `json:"parallel"`
		NewNum   int `json:"new_num"`
	}{
		{1, 0},
		{1, 1000},
		{1, 100000},
		{32, 10000},
		{32, 10000},
	} {
		t.Run(encodeCase(cs), func(t *testing.T) {
			var (
				dialer = &mockDialer{}
				n      = int64(cs.NewNum)
			)

			pool, err := New(dialer.dial, 32, 5*time.Minute)
			assert.Nil(t, err)
			assert.NotNil(t, pool)

			conns := make([]net.Conn, int(n))

			t.Run("group", func(t *testing.T) {
				for p := 0; p < cs.Parallel; p++ {
					t.Run(sprintf("pool.New-%d", p), func(t *testing.T) {
						t.Parallel()
						for i := dec(&n); i >= 0; i = dec(&n) {
							k := rand.Intn(16)
							address := sprintf("192.168.0.%d:80", k)
							c, err := pool.New(address)
							assert.Nil(t, err)
							assert.NotNil(t, c)
							conns[i] = c
						}
					})
				}
			})

			assert.Equal(t, int64(cs.NewNum), dialer.totalConn)
			assert.Equal(t, 0, pool._size())

			for _, c := range conns {
				if c != nil {
					assert.Nil(t, c.Close())
				}
			}
			assert.Equal(t, dialer.totalConn, int64(pool._size()))
			assert.LessOrEqual(t, pool._size(), 32*16)
		})
	}
}

func TestPoolSelectBucket(t *testing.T) {
	for _, cs := range []struct {
		Parallel  int `json:"parallel"`
		SelectNum int `json:"select_num"`
	}{
		{1, 1000},
		{1, 1000000},
		{32, 1000},
		{32, 1000000},
	} {
		t.Run(encodeCase(cs), func(t *testing.T) {
			pool, err := New((&mockDialer{}).dial, 32, 5*time.Minute)
			assert.Nil(t, err)

			var (
				buckets = make([]*bucket, 16)
				n       = int64(cs.SelectNum)
			)

			for p := 0; p < cs.Parallel; p++ {
				t.Run(sprintf("pool.selectBucket-%d", p), func(t *testing.T) {
					t.Parallel()
					for dec(&n) >= 0 {
						i := rand.Intn(16)
						address := sprintf("192.168.0.%d:80", i)
						b := pool.selectBucket(address)
						assert.NotNil(t, b)

						// Accessing to buckets concurrently doesn't matter.
						if buckets[i] == nil {
							buckets[i] = b
						}
						assert.True(t, buckets[i] == b)
					}
				})
			}
		})
	}
}

func TestPoolClose(t *testing.T) {
	pool, err := New((&mockDialer{}).dial, 32, 5*time.Minute)
	assert.NotNil(t, pool)
	assert.Nil(t, err)

	err = pool.Close()
	assert.Nil(t, err)

	err = pool.Close()
	assert.EqualError(t, err, "connection pool is already closed")
}

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
						assert.Equal(t, nil, err)
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
			assert.Equal(t, int64(0), pushSucc)
			assert.Equal(t, int64(0), b.idle)
		} else {
			assert.LessOrEqual(t, int64(cs.BucketCap), b.idle)
		}
	}
}

func TestBucketPop(t *testing.T) {
	for _, cs := range []struct {
		Parallel     int  `json:"parallel"`
		BucketCap    int  `json:"bucket_cap"`
		BucketClosed bool `json:"bucket_closed"`
		PopNum       int  `json:"pop_num"`
	}{
		{1, 0, false, 10000},
		{1, 0, true, 10000},
		{1, 256, false, 10000},
		{1, 256, true, 10000},
		{1, 10000, false, 10000},
		{1, 20000, false, 10000},
		{16, 0, false, 10000},
		{16, 0, true, 10000},
		{16, 256, false, 10000},
		{16, 256, true, 10000},
		{16, 10000, false, 10000},
		{16, 20000, false, 10000},
	} {
		var (
			d       = &mockDialer{}
			b       = &bucket{capacity: cs.BucketCap, top: &element{}}
			n       = int64(cs.PopNum)
			popSucc int64
		)

		// Fill the bucket.
		for i := 0; i < cs.BucketCap; i++ {
			c, err := d.dial("127.0.0.1:80")
			assert.Equal(t, nil, err)
			b.push(b.bind(c))
		}

		if cs.BucketClosed {
			b._close()
		}

		t.Run(encodeCase(cs), func(t *testing.T) {
			for p := 0; p < cs.Parallel; p++ {
				t.Run(sprintf("bucket.pop-%d", p), func(t *testing.T) {
					t.Parallel()
					for dec(&n) >= 0 {
						if c := b.pop(); c != nil {
							inc(&popSucc)
							c.Release()
						}
					}
				})
			}
		})

		assert.Equal(t, b._size(), b.size)
		assert.Equal(t, b._size(), int(b.idle))
		assert.Equal(t, b._depth(), int(b.depth))
		assert.Equal(t, b.size, int(b.depth))
		assert.Equal(t, int64(cs.BucketCap)-b.idle, popSucc)
		assert.Equal(t, d.totalConn, b.total)
		if cs.BucketClosed {
			assert.Equal(t, int64(0), popSucc)
			assert.Equal(t, int64(cs.BucketCap), b.idle)
		}
	}
}

func TestBucketCleanup(t *testing.T) {
	for _, cs := range []struct {
		Shutdown    bool `json:"shutdown"`
		BucketCap   int  `json:"bucket_cap"`
		BucketDepth int  `json:"bucket_depth"`
	}{
		{false, 0, 0},
		{false, 10000, 0},
		{false, 10000, 1},
		{false, 10000, 5000},
		{false, 10000, 10000},
		{true, 0, 0},
		{true, 10000, 0},
		{true, 10000, 1},
		{true, 10000, 5000},
		{true, 10000, 10000},
	} {
		var (
			d = &mockDialer{}
			b = &bucket{capacity: cs.BucketCap, top: &element{}}
		)

		// Fill the bucket.
		for i := 0; i < cs.BucketCap; i++ {
			c, err := d.dial("127.0.0.1:80")
			assert.Equal(t, nil, err)
			b.push(b.bind(c))
		}

		// Adjust the depth of the bucket.
		assert.Equal(t, 0, b.cleanup(false))

		conns := make([]*Conn, 0, cs.BucketDepth)
		for i := 0; i < cs.BucketDepth; i++ {
			conns = append(conns, b.pop())
		}
		for _, conn := range conns {
			b.push(conn)
		}
		assert.Equal(t, cs.BucketDepth, b.depth)

		t.Run(encodeCase(cs), func(t *testing.T) {
			newsize := int(b.depth)
			unused := b.cleanup(cs.Shutdown)

			assert.Equal(t, b._size(), b.size)
			assert.Equal(t, b._size(), int(b.idle))
			assert.Equal(t, b._depth(), int(b.depth))
			assert.Equal(t, d.totalConn, b.total)
			assert.Equal(t, 0, int(b.depth))
			assert.Equal(t, (*element)(nil), b.cut)
			if cs.Shutdown {
				assert.Equal(t, cs.BucketCap, unused)
				assert.Equal(t, 0, b.size)
			} else {
				assert.Equal(t, cs.BucketCap-cs.BucketDepth, unused)
				assert.Equal(t, newsize, b.size)
			}
		})
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
						assert.Equal(t, errConnectTimeout, err)
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
							assert.Equal(t, errCloseConnFailed, err)
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
						assert.Equal(t, errConnectTimeout, err)
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
							assert.Equal(t, errCloseConnFailed, err)
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
