// connpool_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2019-01-18
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2019-01-18

package connpool

import (
	"github.com/stretchr/testify/assert"
	"net"
	"sync/atomic"
	"testing"
)

func testBucketPush(t *testing.T) {
	for _, e := range []struct {
		b *bucket
		n int
	}{
		{b: &bucket{capacity: 0, top: &element{}}, n: 64},
		{b: &bucket{capacity: 1, top: &element{}}, n: 64},
		{b: &bucket{capacity: 32, top: &element{}}, n: 64},
		{b: &bucket{capacity: 64, top: &element{}}, n: 64},
		{b: &bucket{capacity: 128, top: &element{}}, n: 64},
	} {
		d, succ, fail := &dialer{}, 0, 0
		for i := 0; i < e.n; i++ {
			conn, _ := d.dial("192.168.1.1:80")
			if e.b.push(conn) {
				succ++
			} else {
				fail++
			}
		}
	}
}

// Executing a callback function n times in multiple goroutines simultaneously.
func execute(parallel, n int, cb func()) {
	for wg, pn := (&sync.WaitGroup{}), n/parallel; n > (1 - pn); n -= pn {
		m := pn
		if n < 0 {
			m = pn + n
		}

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
	count int32
}

func (d *dailer) dial(address string) (net.Conn, error) {
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
	d             *dailer
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
