// connpool_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2019-04-01
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2019-04-04

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
				a.not_equalf((*Conn)(nil), env.b.cut.conn, "nil == cut.conn:%v", env.b.cut.conn)
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
		b        *bucket
		init_num int
		push_num int
		pop_num  int
		closed   bool
	}{
		{b: &bucket{capacity: 100000, top: &element{}}, init_num: 0, push_num: 256, pop_num: 128, closed: false},
		{b: &bucket{capacity: 100000, top: &element{}}, init_num: 0, push_num: 256, pop_num: 256, closed: false},
		{b: &bucket{capacity: 100000, top: &element{}}, init_num: 0, push_num: 256, pop_num: 512, closed: false},
		{b: &bucket{capacity: 100000, top: &element{}}, init_num: 1024, push_num: 256, pop_num: 128, closed: false},
		{b: &bucket{capacity: 100000, top: &element{}}, init_num: 1024, push_num: 256, pop_num: 256, closed: false},
		{b: &bucket{capacity: 100000, top: &element{}}, init_num: 1024, push_num: 256, pop_num: 512, closed: false},
		{b: &bucket{capacity: 100000, top: &element{}}, init_num: 1024, push_num: 256, pop_num: 2048, closed: false},
		{b: &bucket{capacity: 100000, top: &element{}}, init_num: 0, push_num: 256, pop_num: 128, closed: true},
		{b: &bucket{capacity: 100000, top: &element{}}, init_num: 0, push_num: 256, pop_num: 256, closed: true},
		{b: &bucket{capacity: 100000, top: &element{}}, init_num: 0, push_num: 256, pop_num: 512, closed: true},
		{b: &bucket{capacity: 100000, top: &element{}}, init_num: 1024, push_num: 256, pop_num: 128, closed: true},
		{b: &bucket{capacity: 100000, top: &element{}}, init_num: 1024, push_num: 256, pop_num: 256, closed: true},
		{b: &bucket{capacity: 100000, top: &element{}}, init_num: 1024, push_num: 256, pop_num: 512, closed: true},
		{b: &bucket{capacity: 100000, top: &element{}}, init_num: 1024, push_num: 256, pop_num: 2048, closed: true},
	} {
		var (
			d = &dialer{}
			a = &assertions{assert.New(t), env}
		)

		// Assume that the capacity of the bucket is big enough :)
		execute(64, env.init_num, func() {
			conn, _ := d.dial("192.168.1.1:80")
			env.b.push(env.b.bind(conn))
		})
		unused := env.b.cleanup(false)
		a.equalf(0, unused, "0 != unused:%d", unused)

		execute(64, env.push_num, func() {
			conn, _ := d.dial("192.168.1.1:80")
			env.b.push(env.b.bind(conn))
		})
		max_size := env.b.size

		backup := make(chan *Conn, env.pop_num)
		execute(64, env.pop_num, func() {
			backup <- env.b.pop()
		})

		execute(64, env.pop_num, func() {
			if c := <-backup; c != nil {
				env.b.push(c)
			}
		})

		unused = env.b.cleanup(env.closed)
		used := max(env.push_num, min(env.pop_num, max_size))

		t.Logf("%s %s max_size:%-6d used:%-6d unused:%-6d", d.str(), env.b.str(), max_size, used, unused)
		a.equalf(0, env.b.depth, "0 != depth:%d", env.b.depth)
		a.equalf(env.b._depth(), env.b.depth, "_depth:%d != depth:%d", env.b._depth(), env.b.depth)
		a.equalf((*element)(nil), env.b.cut, "nil != cut:%d", env.b.cut)
		a.equalf(env.b._size(), env.b.size, "_size:%d != size:%d", env.b._size(), env.b.size)
		a.equalf(env.b.size, env.b.total, "size:%d != total:%d", env.b.size, env.b.total)
		a.equalf(env.b.size, env.b.idle, "size:%d != idle:%d", env.b.size, env.b.idle)

		if !env.closed {
			a.equalf(max_size-used, unused, "(max_size:%d - used:%d) != unused:%d", max_size, used, unused)
			a.equalf(d.total_conn, used, "total_conn:%d != used:%d", d.total_conn, used)
			a.equalf(used, env.b.size, "used:%d != size:%d", used, env.b.size)
		} else {
			a.equalf(max_size, unused, "max_size:%d != unused:%d", max_size, unused)
			a.equalf(0, d.total_conn, "0 != total_conn:%d", d.total_conn)
		}
	}
}

func TestCreateAndClosePool(t *testing.T) {
	for _, env := range []struct {
		dial                Dial
		capacity            int
		period              time.Duration
		create_ok, close_ok bool
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

		a.equalf(env.create_ok, err == nil, "create_ok:%v not match error:%s", env.create_ok, err)
		a.equalf(env.create_ok, pool != nil, "create_ok:%v not match pool:%v", env.create_ok, pool)
		if env.create_ok {
			a.equalf(sprintf("%v", env.dial), sprintf("%v", pool.dial), "env.dial:%v != pool.dail:%v", env.dial, pool.dial)
			a.equalf(env.capacity, pool.capacity, "env.cap:%v != pool.cap:%v", env.capacity, pool.capacity)
			a.equalf(env.period, pool.period, "env.period:%s != pool.period", env.period, pool.period)
		} else {
			t.Logf("create pool failed: %s", err)
			continue
		}

		err = pool.Close()
		a.equalf(env.close_ok, err == nil, "close_ok:%v not match error:%s", env.close_ok, err)

		// Test duplicate shutdown.
		err = pool.Close()
		a.not_equalf(nil, err, "closing the closed pool should be failed")
		t.Logf("closing the closed failed: %s", err)
	}
}

/* Auxiliary Structs and Their Methods */

type dialer struct {
	port       int64 // Local port.
	dial_num   int64 // Number of invoking the dial method.
	total_conn int64 // Number of current total connections.
}

func (d *dialer) dial(address string) (net.Conn, error) {
	c := &connection{
		d:      d,
		local:  resolveTCPAddr(sprintf("127.0.0.1:%d", add(&d.port, 1))),
		remote: resolveTCPAddr(address),
	}
	add(&d.dial_num, 1)
	add(&d.total_conn, 1)
	return c, nil
}

func (d *dialer) str() string {
	return sprintf(strings.Repeat("%-10s:%6d ", 2),
		"dial_num", d.dial_num,
		"total_conn", d.total_conn,
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
	add(&c.d.total_conn, -1)
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

func (a *assertions) not_equalf(expected, actual interface{}, msg string, args ...interface{}) bool {
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
