// connpool_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2019-04-01
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2019-12-31

package connpool

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBucketPush(t *testing.T) {
	for _, env := range []struct {
		b      *bucket
		n      int
		closed bool
	}{
		{b: &bucket{capacity: 0, top: &element{}}, n: 4096, closed: false},
		{b: &bucket{capacity: 1, top: &element{}}, n: 4096, closed: false},
		{b: &bucket{capacity: 2048, top: &element{}}, n: 4096, closed: false},
		{b: &bucket{capacity: 4096, top: &element{}}, n: 4096, closed: false},
		{b: &bucket{capacity: 8192, top: &element{}}, n: 4096, closed: false},
		{b: &bucket{capacity: 0, top: &element{}}, n: 4096, closed: true},
		{b: &bucket{capacity: 1, top: &element{}}, n: 4096, closed: true},
		{b: &bucket{capacity: 2048, top: &element{}}, n: 4096, closed: true},
		{b: &bucket{capacity: 4096, top: &element{}}, n: 4096, closed: true},
		{b: &bucket{capacity: 8192, top: &element{}}, n: 4096, closed: true},
	} {
		var (
			d = &dialer{}
			a = &assertions{assert.New(t), env}
			s = &stats{}
		)

		if env.closed {
			env.b._close()
		}

		execute(64, env.n, func() {
			conn, _ := d.dial("192.168.1.1:80")
			c := env.b.bind(conn)
			if env.b.push(c) {
				add(&s.succ, 1)
			} else {
				add(&s.fail, 1)
				c.Release()
			}
		})

		t.Logf("%s %s %s", s.str(), d.str(), env.b.str())

		a.equalf(env.n, s.succ+s.fail, "number:%d != s.succ:%d + s.fail:%d", env.n, s.succ, s.fail)
		a.equalf(s.succ, env.b.size, "s.succ:%d != size:%d", s.succ, env.b.size)
		a.equalf(env.b._size(), env.b.size, "_size:%d != size:%d", env.b._size(), env.b.size)
		a.equalf(s.succ, env.b.total, "s.succ:%d != total:%d", s.succ, env.b.total)
		a.equalf(s.succ, env.b.idle, "s.succ:%d != idle:%d", s.succ, env.b.idle)
		a.equalf(s.succ, env.b.depth, "s.succ:%d != depth:%d", s.succ, env.b.depth)
		a.equalf(env.b.depth, env.b._depth(), "depth:%d != _depth:%d", env.b.depth, env.b._depth())

		if !env.closed && env.b.capacity > 0 {
			a.equalf(min(env.b.capacity, env.n), s.succ, "min{capacity:%d, number:%d} != s.succ:%d", env.b.capacity, env.n, s.succ)
			a.equalf(max(env.n-env.b.capacity, 0), s.fail, "max{number:%d - capacity:%d, 0} != s.fail:%d", env.n, env.b.capacity, s.fail)
			a.equalf((*Conn)(nil), env.b.cut.conn, "nil != cut.conn:%v", env.b.cut.conn)
		} else {
			a.equalf(0, s.succ, "0 != s.succ:%d", s.succ)
			a.equalf((*element)(nil), env.b.cut, "nil != cut:%v", env.b.cut)
		}
	}
}

func TestBucketPop(t *testing.T) {
	for _, env := range []struct {
		b      *bucket
		n      int
		closed bool
	}{
		{b: &bucket{capacity: 0, top: &element{}}, n: 4096, closed: false},
		{b: &bucket{capacity: 1, top: &element{}}, n: 4096, closed: false},
		{b: &bucket{capacity: 2048, top: &element{}}, n: 4096, closed: false},
		{b: &bucket{capacity: 4096, top: &element{}}, n: 4096, closed: false},
		{b: &bucket{capacity: 8192, top: &element{}}, n: 4096, closed: false},
		{b: &bucket{capacity: 0, top: &element{}}, n: 4096, closed: true},
		{b: &bucket{capacity: 1, top: &element{}}, n: 4096, closed: true},
		{b: &bucket{capacity: 2048, top: &element{}}, n: 4096, closed: true},
		{b: &bucket{capacity: 4096, top: &element{}}, n: 4096, closed: true},
		{b: &bucket{capacity: 8192, top: &element{}}, n: 4096, closed: true},
	} {
		var (
			d = &dialer{}
			a = &assertions{assert.New(t), env}
			s = &stats{}
		)

		for i := 0; i < env.b.capacity; i++ {
			conn, _ := d.dial("192.168.1.1:80")
			env.b.push(env.b.bind(conn))
		}
		unused := env.b.cleanup(false) // Reset the bucket to the initial state.
		a.equalf(0, unused, "0 != unused:%d", unused)

		if env.closed {
			env.b._close()
		}

		execute(64, env.n, func() {
			if c := env.b.pop(); c != nil {
				add(&s.succ, 1)
				c.Release()
			} else {
				add(&s.fail, 1)
			}
		})

		t.Logf("%s %s %s", s.str(), d.str(), env.b.str())

		a.equalf(env.n, s.succ+s.fail, "number:%d != s.succ:%d + s.fail:%d", env.n, s.succ, s.fail)
		a.equalf(max(env.b.capacity-int(s.succ), 0), env.b.size, "min{cap:%d - succ:%d, 0} != size:%d", env.b.capacity, s.succ, env.b.size)
		a.equalf(env.b._size(), env.b.size, "_size:%d != size:%d", env.b._size(), env.b.size)
		a.equalf(max(env.b.capacity-int(s.succ), 0), env.b.total, "min{cap:%d - succ:%d, 0} != total:%d", env.b.capacity, s.succ, env.b.total)
		a.equalf(max(env.b.capacity-int(s.succ), 0), env.b.idle, "min{cap:%d - succ:%d, 0} != idle:%d", env.b.capacity, s.succ, env.b.idle)
		a.equalf(0, env.b.depth, "0 != depth:%d", env.b.depth)
		a.equalf(env.b.depth, env.b._depth(), "depth:%d != _depth:%d", env.b.depth, env.b._depth())

		if !env.closed && env.b.capacity > 0 {
			a.equalf(min(env.b.capacity, env.n), s.succ, "min{capacity:%d, number:%d} != s.succ:%d", env.b.capacity, env.n, s.succ)
			a.equalf(max(env.n-env.b.capacity, 0), s.fail, "max{number:%d - capacity:%d, 0} != s.fail:%d", env.n, env.b.capacity, s.fail)

			if env.b.size > 0 {
				a.notEqualf((*Conn)(nil), env.b.cut.conn, "nil == cut.conn:%v", env.b.cut.conn)
			} else {
				a.equalf((*Conn)(nil), env.b.cut.conn, "nil != cut.conn:%v", env.b.cut.conn)
			}
		} else {
			a.equalf(0, s.succ, "0 != s.succ:%d", s.succ)
			a.equalf((*element)(nil), env.b.cut, "nil != cut:%v", env.b.cut)
		}
	}
}

func TestBucketCleanup(t *testing.T) {
	for _, env := range []struct {
		b       *bucket
		initNum int
		pushNum int
		popNum  int
		closed  bool
	}{
		{b: &bucket{capacity: 100000, top: &element{}}, initNum: 0, pushNum: 256, popNum: 128, closed: false},
		{b: &bucket{capacity: 100000, top: &element{}}, initNum: 0, pushNum: 256, popNum: 256, closed: false},
		{b: &bucket{capacity: 100000, top: &element{}}, initNum: 0, pushNum: 256, popNum: 512, closed: false},
		{b: &bucket{capacity: 100000, top: &element{}}, initNum: 1024, pushNum: 256, popNum: 128, closed: false},
		{b: &bucket{capacity: 100000, top: &element{}}, initNum: 1024, pushNum: 256, popNum: 256, closed: false},
		{b: &bucket{capacity: 100000, top: &element{}}, initNum: 1024, pushNum: 256, popNum: 512, closed: false},
		{b: &bucket{capacity: 100000, top: &element{}}, initNum: 1024, pushNum: 256, popNum: 2048, closed: false},
		{b: &bucket{capacity: 100000, top: &element{}}, initNum: 0, pushNum: 256, popNum: 128, closed: true},
		{b: &bucket{capacity: 100000, top: &element{}}, initNum: 0, pushNum: 256, popNum: 256, closed: true},
		{b: &bucket{capacity: 100000, top: &element{}}, initNum: 0, pushNum: 256, popNum: 512, closed: true},
		{b: &bucket{capacity: 100000, top: &element{}}, initNum: 1024, pushNum: 256, popNum: 128, closed: true},
		{b: &bucket{capacity: 100000, top: &element{}}, initNum: 1024, pushNum: 256, popNum: 256, closed: true},
		{b: &bucket{capacity: 100000, top: &element{}}, initNum: 1024, pushNum: 256, popNum: 512, closed: true},
		{b: &bucket{capacity: 100000, top: &element{}}, initNum: 1024, pushNum: 256, popNum: 2048, closed: true},
	} {
		var (
			d = &dialer{}
			a = &assertions{assert.New(t), env}
		)

		// Assume that the capacity of the bucket is big enough :)
		execute(64, env.initNum, func() {
			conn, _ := d.dial("192.168.1.1:80")
			env.b.push(env.b.bind(conn))
		})
		unused := env.b.cleanup(false)
		a.equalf(0, unused, "0 != unused:%d", unused)

		execute(64, env.pushNum, func() {
			conn, _ := d.dial("192.168.1.1:80")
			env.b.push(env.b.bind(conn))
		})
		maxSize := env.b.size

		backup := make(chan *Conn, env.popNum)
		execute(64, env.popNum, func() {
			backup <- env.b.pop()
		})

		execute(64, env.popNum, func() {
			if c := <-backup; c != nil {
				env.b.push(c)
			}
		})

		unused = env.b.cleanup(env.closed)
		used := max(env.pushNum, min(env.popNum, maxSize))

		t.Logf("%s %s maxSize:%-6d used:%-6d unused:%-6d", d.str(), env.b.str(), maxSize, used, unused)
		a.equalf(0, env.b.depth, "0 != depth:%d", env.b.depth)
		a.equalf(env.b._depth(), env.b.depth, "_depth:%d != depth:%d", env.b._depth(), env.b.depth)
		a.equalf((*element)(nil), env.b.cut, "nil != cut:%d", env.b.cut)
		a.equalf(env.b._size(), env.b.size, "_size:%d != size:%d", env.b._size(), env.b.size)
		a.equalf(env.b.size, env.b.total, "size:%d != total:%d", env.b.size, env.b.total)
		a.equalf(env.b.size, env.b.idle, "size:%d != idle:%d", env.b.size, env.b.idle)

		if !env.closed {
			a.equalf(maxSize-used, unused, "(maxSize:%d - used:%d) != unused:%d", maxSize, used, unused)
			a.equalf(d.totalConn, used, "totalConn:%d != used:%d", d.totalConn, used)
			a.equalf(used, env.b.size, "used:%d != size:%d", used, env.b.size)
		} else {
			a.equalf(maxSize, unused, "maxSize:%d != unused:%d", maxSize, unused)
			a.equalf(0, d.totalConn, "0 != totalConn:%d", d.totalConn)
		}
	}
}

func TestCreateAndClosePool(t *testing.T) {
	for _, env := range []struct {
		dial              Dial
		capacity          int
		period            time.Duration
		createOk, closeOk bool
	}{
		{nil, 32, 2 * time.Minute, false, true},
		{(&dialer{}).dial, -10, 2 * time.Minute, false, true},
		{(&dialer{}).dial, 64, 5 * time.Second, false, true},
		{(&dialer{}).dial, 32, 5 * time.Minute, true, true},
		{(&dialer{}).dial, 0, 5 * time.Minute, true, true},
		{(&dialer{}).dial, 32, 1 * time.Minute, true, true},
		{(&dialer{}).dial, 0, 1 * time.Minute, true, true},
	} {
		a := &assertions{assert.New(t), env}
		pool, err := New(env.dial, env.capacity, env.period)

		a.equalf(env.createOk, err == nil, "createOk:%v not match error:%s", env.createOk, err)
		a.equalf(env.createOk, pool != nil, "createOk:%v not match pool:%v", env.createOk, pool)
		if env.createOk {
			a.equalf(sprintf("%v", env.dial), sprintf("%v", pool.dial), "env.dial:%v != pool.dail:%v", env.dial, pool.dial)
			a.equalf(env.capacity, pool.capacity, "env.cap:%v != pool.cap:%v", env.capacity, pool.capacity)
			a.equalf(env.period, pool.period, "env.period:%s != pool.period", env.period, pool.period)
		} else {
			t.Logf("create pool failed: %s", err)
			continue
		}

		err = pool.Close()
		a.equalf(env.closeOk, err == nil, "closeOk:%v not match error:%s", env.closeOk, err)

		// Test duplicate shutdown.
		err = pool.Close()
		a.notEqualf(nil, err, "closing the closed pool should be failed")
		t.Logf("closing the closed failed: %s", err)
	}
}

func TestPool(t *testing.T) {
	_test = true // Runs package in testing mode.
	defer func() {
		// Recover the package to normal mode when this function has done.
		_test = false
	}()

	var (
		d       = &dialer{}
		pool, _ = New(d.dial, 64, time.Second)
		a       = &assertions{assert.New(t), "test-pool"}
		addr    int64
		addrs   = []string{
			"192.168.1.1:80",
			"192.168.1.2:80",
			"192.168.1.3:80",
			"192.168.1.4:80",
			"192.168.1.5:80",
			"192.168.1.6:80",
			"192.168.1.7:80",
			"192.168.1.8:80",
			"192.168.1.9:80",
			"192.168.1.10:80",
			"192.168.1.11:80",
			"192.168.1.12:80",
		}
	)

	for w, n, i, inc := 2, 100, 0, true; i < 100; i++ {
		var s = &stats{}
		execute(w, n, func() {
			conn, err := pool.Get(addrs[int(add(&addr, 1))%len(addrs)])
			if err == nil {
				add(&s.succ, 1)
			} else {
				add(&s.fail, 1)
			}
			conn.Close()
		})

		back := <-pool._interrupt

		logf("%s %s %s", s.str(), d.str(), sprintf(strings.Repeat("%-6s:%6d ", 3),
			"worker", w,
			"num", n,
			"size", pool._size()))

		a.equalf(n, s.succ, "number:%d != succ:%d", n, s.succ)
		a.equalf(0, s.fail, "0 != fail:%d", s.fail)
		a.equalf(d.totalConn, pool._size(), "totalConn:%d != pool.size:%d", d.totalConn, pool._size())
		for _, b := range pool.buckets {
			a.equalf(b._size(), b.size, "bucket._size:%d != bucket.size:%d", b._size(), b.size)
			a.equalf(b.size, b.idle, "bucket.size:%d != bucket.idle:%d", b.size, b.idle)
			a.equalf(b._depth(), b.depth, "bucket._depth:%d != bucket.depth:%d", b._depth(), b.depth)
		}

		close(back)

		if w <= 2 || n <= 100 {
			inc = true
		} else if w >= 2048 || n >= 100000 {
			inc = false
		}

		if inc {
			w, n = w*2, n*2
		} else {
			w, n = w/2, n/2
		}
	}

	pool.Close()
	a.equalf(0, pool._size(), "pool.size:%d is not zero after closing", pool._size())
	a.equalf(0, d.totalConn, "totalConn:%d is not zero after closing", d.totalConn)
}

/* Auxiliary Structs and Their Methods */

type dialer struct {
	port      int64 // Local port.
	dialNum   int64 // Number of invoking the dial method.
	totalConn int64 // Number of current total connections.
}

func (d *dialer) dial(address string) (net.Conn, error) {
	c := &connection{
		d:      d,
		local:  resolveTCPAddr(sprintf("127.0.0.1:%d", add(&d.port, 1))),
		remote: resolveTCPAddr(address),
	}
	add(&d.dialNum, 1)
	add(&d.totalConn, 1)
	return c, nil
}

func (d *dialer) str() string {
	return sprintf(strings.Repeat("%-10s:%6d ", 2),
		"dialNum", d.dialNum,
		"totalConn", d.totalConn,
	)
}

type stats struct {
	succ int64
	fail int64
}

func (s *stats) str() string {
	return sprintf(strings.Repeat("%-4s:%6d ", 2),
		"succ", s.succ,
		"fail", s.fail,
	)
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
	add(&c.d.totalConn, -1)
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

// Wraps assert.Assertions to output environment information.
type assertions struct {
	*assert.Assertions
	env interface{}
}

func (a *assertions) equalf(expected, actual interface{}, msg string, args ...interface{}) bool {
	return a.Equalf(int2int64(expected), int2int64(actual), sprintf("%#v %s", a.env, msg), args...)
}

func (a *assertions) notEqualf(expected, actual interface{}, msg string, args ...interface{}) bool {
	return a.NotEqualf(int2int64(expected), int2int64(actual), sprintf("%#v %s", a.env, msg), args...)
}

// Satisfy sort.Interface to sort multiple DestinationsStats structs.
type destinations []DestinationStats

func (ds destinations) Len() int {
	return len(ds)
}

func (ds destinations) Less(i, j int) bool {
	return strings.Compare(ds[i].Address, ds[j].Address) < 0
}

func (ds destinations) Swap(i, j int) {
	ds[i], ds[j] = ds[j], ds[i]
}

func (b *bucket) str() string {
	return sprintf(strings.Repeat("%-6s:%6d ", 7),
		"cap", b.capacity,
		"size", b.size,
		"_size", b._size(),
		"depth", b.depth,
		"_depth", b._depth(),
		"total", b.total,
		"idle", b.idle,
	)
}

/* Auxiliary Functions */

// I rename the following functions to simplify my codes because they're
// very commonly used in this testing.
var (
	sprintf = fmt.Sprintf
	errorf  = fmt.Errorf
	add     = atomic.AddInt64
	load    = atomic.LoadInt64
)

// Executing a callback function n times in multiple goroutines simultaneously.
func execute(parallel, n int, cb func()) {
	var wg = &sync.WaitGroup{}
	for pn := max(n/parallel, 1); n > 0; n -= pn {
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

// Output log information in verbose mode.
func logf(format string, args ...interface{}) {
	if testing.Verbose() {
		log.Printf(format, args...)
	}
}

func resolveTCPAddr(s string) net.Addr {
	addr, _ := net.ResolveTCPAddr("tcp", s)
	return addr
}

func int2int64(i interface{}) interface{} {
	if v, ok := i.(int); ok {
		return int64(v)
	}
	return i
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
