// connpool_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2019-01-18
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2019-01-18

package connpool

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBucket(t *testing.T) {
	t.Run("bucket.push", testBucketPush)
	t.Run("(closed) bucket.push", testClosedBucketPush)
}

func testBucketPush(t *testing.T) {
	for _, e := range []struct {
		b *bucket
		n int
	}{
		{b: &bucket{capacity: 0, top: &element{}}, n: 4096},
		{b: &bucket{capacity: 1, top: &element{}}, n: 4096},
		{b: &bucket{capacity: 2048, top: &element{}}, n: 4096},
		{b: &bucket{capacity: 4096, top: &element{}}, n: 4096},
		{b: &bucket{capacity: 8192, top: &element{}}, n: 4096},
	} {
		var (
			d          = &dialer{}
			succ, fail int64
		)

		execute(16, e.n, func() {
			conn, _ := d.dial("192.168.1.1:80")
			c := e.b.bind(conn)
			if e.b.push(c) {
				atomic.AddInt64(&succ, 1)
			} else {
				atomic.AddInt64(&fail, 1)
				c.Close()
			}
		})

		assert, env := assert.New(t), sprintf("[capacity:%d number:%d]", e.b.capacity, e.n)
		assert.Equalf(e.n, int(succ+fail), "%s total:%d != succ:%d + fail:%d", env, e.n, succ, fail)
		assert.Equalf(min(e.b.capacity, e.n), int(succ), "%s min(capacity:%d, number:%d) != succ:%d", env, e.b.capacity, e.n, succ)
		assert.Equalf(max(e.n-e.b.capacity, 0), int(fail), "%s max(number:%d - capacity:%d, 0) != fail:%d", env, e.n, e.b.capacity, fail)
		assert.Equalf(e.b.size, int(succ), "%s bucket.size:%d != succ:%d", env, e.b.size, succ)
		assert.Equalf(e.b.size, e.b._size(), "%s bucket.size:%d != bucket._size:%d", env, e.b.size, e.b._size())
		assert.Equalf(e.b.total, succ, "%s bucket.total:%d != succ:%d", env, e.b.total, succ)
		assert.Equalf(e.b.idle, succ, "%s bucket.idle:%d != succ:%d", env, e.b.idle, succ)
		assert.Equalf(e.b.depth, 0, "%s bucket.depth:%d != 0", env, e.b.depth)
		assert.Equalf((*element)(nil), e.b.cut, "%s bucket.cut:%v != nil", env, e.b.cut)
	}
}

func testClosedBucketPush(t *testing.T) {
	for _, e := range []struct {
		b *bucket
		n int
	}{
		{b: &bucket{capacity: 0, top: &element{}, closed: true}, n: 10000},
		{b: &bucket{capacity: 1, top: &element{}, closed: true}, n: 10000},
		{b: &bucket{capacity: 2048, top: &element{}, closed: true}, n: 10000},
		{b: &bucket{capacity: 4096, top: &element{}, closed: true}, n: 10000},
		{b: &bucket{capacity: 8192, top: &element{}, closed: true}, n: 10000},
	} {
		var (
			d          = &dialer{}
			succ, fail int64
		)

		execute(16, e.n, func() {
			conn, _ := d.dial("192.168.1.1:80")
			c := e.b.bind(conn)
			if e.b.push(c) {
				atomic.AddInt64(&succ, 1)
			} else {
				atomic.AddInt64(&fail, 1)
				c.Close()
			}
		})

		assert, env := assert.New(t), sprintf("[capacity:%d number:%d closed:true]", e.b.capacity, e.n)
		assert.Equalf(e.n, int(succ+fail), "%s total:%d != succ:%d + fail:%d", env, e.n, succ, fail)
		assert.Equalf(0, int(succ), "%s 0 != succ:%d", env, succ)
		assert.Equalf(e.n, int(fail), "%s number:%d != fail:%d", env, e.n, fail)
		assert.Equalf(0, e.b.size, "%s bucket.size:%d != 0", env, e.b.size)
		assert.Equalf(0, e.b._size(), "%s bucket._size:%d != 0", env, e.b._size())
		assert.Equalf(0, int(e.b.total), "%s bucket.total:%d != 0", env, e.b.total)
		assert.Equalf(0, int(e.b.idle), "%s bucket.idle:%d != 0", env, e.b.idle)
		assert.Equalf(0, e.b.depth, "%s bucket.depth:%d != 0", env, e.b.depth)
		assert.Equalf((*element)(nil), e.b.cut, "%s bucket.cut:%v != nil", env, e.b.cut)
	}
}

// Executing a callback function n times in multiple goroutines simultaneously.
func execute(parallel, n int, cb func()) {
	var wg = &sync.WaitGroup{}
	for pn := n / parallel; n > 0; n -= pn {
		m := min(pn, n)
		wg.Add(1)
		go func(m int) {
			for i := 0; i < m; i++ {
				cb()
			}
			wg.Done()
		}(m)
	}
	wg.Wait()
}

type dialer struct {
	port  int32
	count int64
}

func (d *dialer) dial(address string) (net.Conn, error) {
	c := &connection{
		d:      d,
		local:  resolveTCPAddr(sprintf("127.0.0.1:%d", atomic.AddInt32(&d.port, 1))),
		remote: resolveTCPAddr(address),
	}
	atomic.AddInt64(&d.count, 1)
	return c, nil
}

var sprintf = fmt.Sprintf

func resolveTCPAddr(s string) net.Addr {
	addr, _ := net.ResolveTCPAddr("tcp", s)
	return addr
}

// connection is an auxiliary struct which satisfies the net.Conn interface.
type connection struct {
	d             *dialer
	local, remote net.Addr
}

func (c *connection) Read(b []byte) (int, error) {
	return len(b), nil
}

func (c *connection) Write(b []byte) (int, error) {
	return len(b), nil
}

func (c *connection) Close() error {
	atomic.AddInt64(&c.d.count, -1)
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
