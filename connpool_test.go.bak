// connpool_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2019-01-18
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2019-03-22

package connpool

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPool(t *testing.T) {
	t.Run("create/close pool", testCreateAndClosePool)
	t.Run("pool.new", testPoolNew)
	if !testing.Short() {
		t.Run("pool.get", testPoolGet)
	}
}

func testCreateAndClosePool(t *testing.T) {
	dial := func(string) (net.Conn, error) {
		return nil, nil
	}

	for _, e := range []struct {
		dial              Dial
		capacity          int
		period            time.Duration
		createOK, closeOK bool
	}{
		{nil, 32, 2 * time.Minute, false, true},
		{dial, -10, 2 * time.Minute, false, true},
		{dial, 64, 5 * time.Second, false, true},
		{dial, 32, 5 * time.Minute, true, true},
		{dial, 0, 5 * time.Minute, true, true},
		{dial, 32, 1 * time.Minute, true, true},
		{dial, 0, 1 * time.Minute, true, true},
	} {
		assert, env := assert.New(t), sprintf("[dial:%v capacity:%d period:%s]", e.dial, e.capacity, e.period)
		pool, err := New(e.dial, e.capacity, e.period)
		if !e.createOK {
			assert.NotEqualf(nil, err, "%s creating a pool should be failed", env)
			t.Logf("%s create a pool failed: %s", env, err)
			continue
		}
		assert.Equalf(nil, err, "%s creating a pool failed: %s", env, err)
		assert.Equalf(sprintf("%v", e.dial), sprintf("%v", pool.dial), "pool.dial (%v) != dial (%v)", pool.dial, e.dial)
		assert.Equalf(e.capacity, pool.capacity, "pool.capacity (%d) != capacity (%d)", pool.capacity, e.capacity)
		assert.Equalf(e.period, pool.period, "pool.period (%v) != period (%v)", pool.period, e.period)

		err = pool.Close()
		if !e.closeOK {
			assert.NotEqualf(nil, err, "%s closing the pool should be failed: %s", env, err)
			t.Logf("%s closing the pool failed: %s", env, err)
		}
		assert.Equalf(nil, err, "%s closing the pool failed: %s", env, err)

		// test duplicate shutdown
		err = pool.Close()
		assert.NotEqualf(nil, err, "%s closing the pool should be failed: %s", env, err)
		t.Logf("%s closing the pool failed: %s", env, err)
	}
}

func testPoolGet(t *testing.T) {
	_test = true // Runs package in testing mode.
	defer func() {
		// Recover the package to normal mode when this function has done.
		_test = false
	}()

	addresses := []string{
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

	var (
		d       = &dialer{}
		pool, _ = New(d.dial, 64, time.Second)
		addr    int64
	)

	assert := assert.New(t)

	for w, n, i, inc := 2, 100, 0, true; i < 100; i++ {
		var succ, fail int64

		execute(w, n, func() {
			conn, err := pool.Get(addresses[int(atomic.AddInt64(&addr, 1))%len(addresses)])
			if err == nil {
				atomic.AddInt64(&succ, 1)
			} else {
				atomic.AddInt64(&fail, 1)
			}
			conn.Close()
		})

		back := <-pool._interrupt
		env := sprintf("%d dial:%d worker:%d number:%d rest:%d pool-size:%d", i, d.total, w, n, d.count, pool._size())

		logf("%s\n%s\n", env, stats2str(pool.Stats()))
		assert.Equalf(n, int(succ), "%s success:%d != number:%d", env, succ, n)
		assert.Equalf(0, int(fail), "%s fail:%d is not zero", env, fail)
		assert.Equalf(int(d.count), pool._size(), "%s rest:%d != pool-size:%d", env, d.count, pool._size())
		for _, b := range pool.buckets {
			assert.Equalf(b.size, b._size(), "%s bucket.size:%d != bucket._size:%d", env, b.size, b._size())
			assert.Equalf(b.size, int(b.idle), "%s bucket.size:%d != bucket.idle:%d", env, b.size, b.idle)
			assert.Equalf(b._depth(), b.depth, "%s bucket._depth:%d != bucket.depth:%d", env, b._depth(), b.depth)
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
	assert.Equalf(0, pool._size(), "pool-size:%d is not zero after closing", pool._size())
	assert.Equalf(0, int(d.count), "rest:%d is not zero after closing", d.count)
}

func testPoolNew(t *testing.T) {
	addresses := []string{
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

	for _, e := range []struct {
		capacity int
		n        int
	}{
		{8, 1000},
		{16, 1000},
		{32, 2000},
		{64, 10000},
		{128, 10000},
	} {

		var (
			d                = &dialer{}
			pool, _          = New(d.dial, e.capacity, time.Hour)
			addr, succ, fail int64
			env              = sprintf("[capacity:%d number:%d]", e.capacity, e.n)
			assert           = assert.New(t)
		)

		execute(64, e.n, func() {
			conn, err := pool.New(addresses[int(atomic.AddInt64(&addr, 1))%len(addresses)])
			if err == nil {
				atomic.AddInt64(&succ, 1)
			} else {
				atomic.AddInt64(&fail, 1)
			}
			conn.Close()
		})

		t.Logf("%s dial:%d rest:%d pool-size:%d\n%s\n", env, d.total, d.count, pool._size(), stats2str(pool.Stats()))
		assert.Equalf(e.n, int(succ), "%s success:%d != number:%d", env, succ, e.n)
		assert.Equalf(0, int(fail), "%s fail:%d is not zero", env, fail)
		assert.Equalf(int(d.count), pool._size(), "%s rest:%d != pool-size:%d", env, d.count, pool._size())
		assert.Equalf(len(addresses)*e.capacity, pool._size(), "pool._size:%d is not correct", env, pool._size())
		assert.Equalf(e.n, int(d.total), "%s number:%d != dial-total:%d", env, e.n, d.total)
		for _, b := range pool.buckets {
			assert.Equalf(0, b.depth, "%s bucket.depth:%d is not zero", env, b.depth)
			assert.Equalf(b._depth(), b.depth, "%s bucket._depth:%d != bucket.depth:%d", env, b._depth(), b.depth)
		}

		pool.Close()
		assert.Equalf(0, pool._size(), "pool-size:%d is not zero after closing", pool._size())
		assert.Equalf(0, int(d.count), "rest:%d is not zero after closing", d.count)
	}
}

func TestBucket(t *testing.T) {
	t.Run("bucket.push", testBucketPush)
	t.Run("(closed) bucket.push", testClosedBucketPush)
	t.Run("bucket.pop", testBucketPop)
	t.Run("(closed) bucket.pop", testClosedBucketPop)
	t.Run("bucket.push/pop", testBucketPushAndPop)
	t.Run("bucket.cleanup", testBucketCleanup)
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
				c.Release()
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
		assert.Equalf(e.b._depth(), e.b.depth, "%s bucket._depth:%d != bucket.depth:%d", env, e.b._depth(), e.b.depth)
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
				c.Release()
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
		assert.Equalf(e.b._depth(), e.b.depth, "%s bucket._depth:%d != bucket.depth:%d", env, e.b._depth(), e.b.depth)
		assert.Equalf((*element)(nil), e.b.cut, "%s bucket.cut:%v != nil", env, e.b.cut)
	}
}

func testBucketPop(t *testing.T) {
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

		for i := 0; i < e.b.capacity; i++ {
			conn, _ := d.dial("192.168.1.1:80")
			e.b.push(e.b.bind(conn))
		}

		execute(16, e.n, func() {
			if e.b.pop() != nil {
				atomic.AddInt64(&succ, 1)
			} else {
				atomic.AddInt64(&fail, 1)
			}
		})

		assert, env := assert.New(t), sprintf("[capacity:%d number:%d]", e.b.capacity, e.n)
		assert.Equalf(e.n, int(succ+fail), "%s total:%d != succ:%d + fail:%d", env, e.n, succ, fail)
		assert.Equalf(min(e.b.capacity, e.n), int(succ), "%s min(capacity:%d, number:%d) != succ:%d", env, e.b.capacity, e.n, succ)
		assert.Equalf(max(e.n-e.b.capacity, 0), int(fail), "%s max(number:%d - capacity:%d, 0) != fail:%d", env, e.n, e.b.capacity, fail)
		assert.Equalf(max(e.b.capacity-e.n, 0), e.b.size, "%s max(capacity:%d - number:%d, 0), bucket.size:%d", env, e.b.capacity, e.n, e.b.size)
		assert.Equalf(e.b.size, e.b._size(), "%s bucket.size:%d != bucket._size:%d", env, e.b.size, e.b._size())
		assert.Equalf(e.b.size, int(e.b.idle), "%s bucket.size:%d != bucket.idle:%d", env, e.b.size, e.b.idle)
		assert.Equalf(0, e.b.depth, "%s bucket.depth:%d != 0", env, e.b.depth)
		assert.Equalf(e.b._depth(), e.b.depth, "%s bucket._depth:%d != bucket.depth:%d", env, e.b._depth(), e.b.depth)

		if e.b.size == 0 {
			assert.Equalf(element{}, *(e.b.top), "%s bucket.top:%v is not empty", env, e.b.top)
		}

		if e.b.capacity != 0 {
			assert.Equalf(e.b.top, e.b.cut, "%s bucket.cut:%v != bucket.top:%v", env, e.b.cut, e.b.top)
		}

		succ = 0
		execute(16, e.b.capacity, func() {
			conn, _ := d.dial("192.168.1.1:80")
			if e.b.push(e.b.bind(conn)) {
				atomic.AddInt64(&succ, 1)
			}
		})

		assert.Equalf(e.b.capacity, e.b.size, "%s bucket.size:%d != bucket.capacity:%d", env, e.b.capacity, e.b.size)
		assert.Equalf(int(succ), e.b.depth, "%s bucket.depth:%d != bucket.size:%d", env, e.b.depth, e.b.size)
		assert.Equalf(e.b._depth(), e.b.depth, "%s bucket._depth:%d != bucket.depth:%d", env, e.b._depth(), e.b.depth)
	}
}

func testClosedBucketPop(t *testing.T) {
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

		for i := 0; i < e.b.capacity; i++ {
			conn, _ := d.dial("192.168.1.1:80")
			e.b.push(e.b.bind(conn))
		}
		e.b._close() // close the bucket.

		execute(16, e.n, func() {
			if e.b.pop() != nil {
				atomic.AddInt64(&succ, 1)
			} else {
				atomic.AddInt64(&fail, 1)
			}
		})

		assert, env := assert.New(t), sprintf("[capacity:%d number:%d closed:true]", e.b.capacity, e.n)
		assert.Equalf(0, int(succ), "%s 0 != succ:%d", env, succ)
		assert.Equalf(e.n, int(fail), "%s number:%d != fail:%d", env, e.n, fail)
		assert.Equalf(e.b.capacity, e.b.size, "%s bucket.capacity:%d != bucket.size:%d", env, e.b.capacity, e.b.size)
		assert.Equalf(e.b.size, e.b._size(), "%s bucket.size:%d != bucket._size:%d", env, e.b.size, e.b._size())
		assert.Equalf(e.b.size, int(e.b.idle), "%s bucket.size:%d != bucket._idle:%d", env, e.b.size, e.b.idle)
		assert.Equalf(0, e.b.depth, "%s bucket.depth:%d is not zero", env, e.b.depth)
		assert.Equalf(e.b._depth(), e.b.depth, "%s bucket._depth:%d != bucket.depth:%d", env, e.b._depth(), e.b.depth)
		assert.Equalf((*element)(nil), e.b.cut, "%s bucket.cut:%v is not nil", env, e.b.cut)
	}
}

func testBucketPushAndPop(t *testing.T) {
	for _, e := range []struct {
		b     *bucket
		pushn int
		popn  int
	}{
		{b: &bucket{capacity: 0, top: &element{}}, pushn: 4096, popn: 4096},
		{b: &bucket{capacity: 1, top: &element{}}, pushn: 4096, popn: 4096},
		{b: &bucket{capacity: 2048, top: &element{}}, pushn: 4096, popn: 4096},
		{b: &bucket{capacity: 4096, top: &element{}}, pushn: 4096, popn: 4096},
		{b: &bucket{capacity: 4096, top: &element{}}, pushn: 8192, popn: 4096},
		{b: &bucket{capacity: 4096, top: &element{}}, pushn: 4096, popn: 8192},
		{b: &bucket{capacity: 8192, top: &element{}}, pushn: 4096, popn: 4096},
		{b: &bucket{capacity: 8192, top: &element{}}, pushn: 4096, popn: 8192},
		{b: &bucket{capacity: 8192, top: &element{}}, pushn: 100000, popn: 100000},
	} {
		var (
			d                  = &dialer{}
			wg                 sync.WaitGroup
			pushSucc, pushFail int64
			popSucc, popFail   int64
		)

		wg.Add(1)
		go func() {
			execute(16, e.pushn, func() {
				conn, _ := d.dial("192.168.1.1:80")
				c := e.b.bind(conn)
				if e.b.push(c) {
					atomic.AddInt64(&pushSucc, 1)
				} else {
					atomic.AddInt64(&pushFail, 1)
					c.Release()
				}
			})
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			execute(16, e.popn, func() {
				if c := e.b.pop(); c != nil {
					atomic.AddInt64(&popSucc, 1)
				} else {
					atomic.AddInt64(&popFail, 1)
				}
			})
			wg.Done()
		}()
		wg.Wait()

		t.Logf("push (succ:%d/fail:%d) pop (succ:%d/fail:%d) bucket.size:%d bucket.depth:%d", pushSucc, pushFail, popSucc, popFail, e.b.size, e.b.depth)

		assert, env := assert.New(t), sprintf("[capacity:%d push-number:%d pop-number:%d]", e.b.capacity, e.pushn, e.popn)
		assert.Equalf(e.pushn, int(pushSucc+pushFail), "%s push-number:%d != push-succ:%d + push-fail:%d", env, e.pushn, pushSucc, pushFail)
		assert.Equalf(e.popn, int(popSucc+popFail), "%s pop-number:%d != pop-succ:%d + pop-fail:%d", env, e.popn, popSucc, popFail)
		assert.Equalf(max(int(pushSucc-popSucc), 0), e.b.size, "%s max(push-succ:%d - pop-succ:%d, 0) != bucket.size:%d", env, pushSucc, popSucc, e.b.size)
		assert.Equalf(e.b.size, e.b._size(), "%s bucket.size:%d != bucket._size:%d", env, e.b.size, e.b._size())
		assert.Equalf(e.b.size, int(e.b.idle), "%s bucket.size:%d != bucket.idle:%d", env, e.b.size, e.b.idle)
		assert.Equalf(e.b._depth(), e.b.depth, "%s bucket._depth:%d != bucket.depth:%d", env, e.b._depth(), e.b.depth)

		if e.b.depth != 0 && e.b.depth == e.b.size {
			assert.NotEqualf((*element)(nil), e.b.cut, "%s bucket.cut:%v is nil", env, e.b.cut)
			assert.Equalf((*Conn)(nil), e.b.cut.conn, "%s bucket.cut.conn:%v != nil", env, e.b.cut.conn)
			assert.Equalf((*element)(nil), e.b.cut.next, "%s bucket.cut.next:%v != nil", env, e.b.cut.next)
		}
	}
}

func testBucketCleanup(t *testing.T) {
	for _, e := range []struct {
		b        *bucket
		part     int
		shutdown bool
		use      bool
	}{
		{b: &bucket{capacity: 1024, top: &element{}}, part: 128, shutdown: false, use: true},
		{b: &bucket{capacity: 1024, top: &element{}}, part: 127, shutdown: false, use: true},
		{b: &bucket{capacity: 4096, top: &element{}}, part: 77, shutdown: false, use: true},
		{b: &bucket{capacity: 8192, top: &element{}}, part: 77, shutdown: false, use: true},
		{b: &bucket{capacity: 8192, top: &element{}}, part: 77, shutdown: false, use: false},
		{b: &bucket{capacity: 1024, top: &element{}}, part: 127, shutdown: true, use: true},
		{b: &bucket{capacity: 4096, top: &element{}}, part: 77, shutdown: true, use: true},
	} {
		var (
			d = &dialer{}
			b = &bucket{capacity: e.b.capacity, top: &element{}} // cache.
		)

		execute(16, e.b.capacity, func() {
			conn, _ := d.dial("192.168.1.1:80")
			c := e.b.bind(conn)
			e.b.push(c)
		})

		assert, env := assert.New(t), sprintf("[capacity:%d part:%d shutdown:%v]",
			e.b.capacity, e.part, e.shutdown)

		for lastRest, rest := 0, e.b.capacity; rest > 0; {
			lastRest, rest = rest, max(0, rest-e.part)

			if e.use {
				execute(16, rest, func() { b.push(e.b.pop()) })
				execute(16, rest, func() { b.pop().Close() })
			}

			unused := e.b.cleanup(e.shutdown)

			if !e.shutdown && e.use {
				t.Logf("%s last_rest:%d rest:%d unused:%d bucket.size:%d", env, lastRest, rest, unused, e.b.size)
				assert.Equalf(lastRest-rest, unused, "%s last_rest:%d - rest:%d != unused:%d", env, lastRest, rest, unused)
				assert.Equalf(rest, e.b.size, "%s rest:%d != bucket.size:%d", env, rest, e.b.size)
				assert.Equalf(e.b.size, int(e.b.idle), "%s bucket.size:%d != bucket.idle:%d", env, e.b.size, e.b.idle)
				assert.Equalf(rest, int(d.count), "%s rest:%d != release:%d", env, rest, d.count)
				assert.NotEqualf((*element)(nil), b.top, "%s bucket.top is nil", env)
				assert.Equalf(e.b._depth(), e.b.depth, "%s bucket._depth:%d != bucket.depth:%d", env, e.b._depth(), e.b.depth)
			} else {
				assert.Equalf(0, e.b.size, "%s bucket.size:%d != 0", env, e.b.size)
				assert.Equalf(0, int(e.b.idle), "%s bucket.idle:%d != 0", env, e.b.idle)
				assert.Equalf(0, int(d.count), "%s release:%d != 0", env, d.count)
				assert.NotEqualf((*element)(nil), b.top, "%s bucket.top is nil", env)
				break
			}
		}
	}
}

// Returns the readable form of a pool's statistical data.
func stats2str(s *Stats) string {
	sort.Sort(destinations(s.Destinations))
	strs, total, idle := make([]string, len(s.Destinations)+2), int64(0), int64(0)
	strs[0] = (time.Unix(s.Timestamp, 0)).String()
	for i, d := range s.Destinations {
		strs[i+2] = sprintf("%-24s total: %-6d idle: %d", d.Address, d.Total, d.Idle)
		total, idle = total+d.Total, idle+d.Idle
	}
	strs[1] = sprintf("%-24s total: %-6d idle: %d", "all", total, idle)
	return strings.Join(strs, "\n")
}

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

type dialer struct {
	port  int32
	count int64
	total int64
}

func (d *dialer) dial(address string) (net.Conn, error) {
	c := &connection{
		d:      d,
		local:  resolveTCPAddr(sprintf("127.0.0.1:%d", atomic.AddInt32(&d.port, 1))),
		remote: resolveTCPAddr(address),
	}
	atomic.AddInt64(&d.count, 1)
	atomic.AddInt64(&d.total, 1)
	return c, nil
}

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

var sprintf = fmt.Sprintf

func logf(format string, args ...interface{}) {
	if testing.Verbose() {
		log.Printf(format, args...)
	}
}
