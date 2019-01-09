// connpool_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-07-11
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2019-01-09

package connpool

import (
	"fmt"
	"github.com/bmizerany/assert"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewPool(t *testing.T) {
	elements := []struct {
		dial     Dial
		capacity int
		timeout  time.Duration
		ok       bool
	}{
		// Correct.
		{
			dial: func(address string) (net.Conn, error) {
				return net.Dial("tcp", address)
			},
			capacity: 128,
			timeout:  5 * time.Minute,
			ok:       true,
		},
		{
			dial: func(address string) (net.Conn, error) {
				return (&dialer{}).Dial(address)
			},
			capacity: 128,
			timeout:  5 * time.Minute,
			ok:       true,
		},
		{
			dial: func(address string) (net.Conn, error) {
				return (&dialer{}).Dial(address)
			},
			capacity: 0,
			timeout:  5 * time.Minute,
			ok:       true,
		},
		{
			dial: func(address string) (net.Conn, error) {
				return (&dialer{}).Dial(address)
			},
			capacity: 128,
			timeout:  1 * time.Minute,
			ok:       true,
		},

		// Incorrect.
		{
			dial:     nil,
			capacity: 128,
			timeout:  3 * time.Minute,
			ok:       false,
		},
		{
			dial: func(address string) (net.Conn, error) {
				return (&dialer{}).Dial(address)
			},
			capacity: -1,
			timeout:  3 * time.Minute,
			ok:       false,
		},
		{
			dial: func(address string) (net.Conn, error) {
				return (&dialer{}).Dial(address)
			},
			capacity: 128,
			timeout:  time.Second,
			ok:       false,
		},
	}

	for _, e := range elements {
		pool, err := New(e.dial, e.capacity, e.timeout)
		if e.ok {
			assert.NotEqual(t, (*Pool)(nil), pool)
			assert.Equal(t, nil, err)
		} else {
			t.Logf("new pool failed: %s", err)
			assert.Equal(t, (*Pool)(nil), pool)
			assert.NotEqual(t, nil, err)
		}
	}
}

func TestPoolNew(t *testing.T) {
	type address struct {
		value string
		count int64
	}

	selectAddress := func(addresses []*address) *address {
		return addresses[int(rand.Int63())%len(addresses)]
	}

	elements := []struct {
		capacity  int
		timeout   time.Duration
		d         *dialer
		ws        *workers
		addresses []*address
	}{
		{
			capacity: 128,
			timeout:  3 * time.Minute,
			d:        &dialer{},
			ws:       &workers{wn: 1, number: 512},
			addresses: []*address{
				{value: "192.168.1.1:80"},
				{value: "192.168.1.2:80"},
				{value: "192.168.1.3:80"},
				{value: "192.168.1.4:80"},
			},
		},
		{
			capacity: 256,
			timeout:  3 * time.Minute,
			d:        &dialer{},
			ws:       &workers{wn: 4, number: 512},
			addresses: []*address{
				{value: "192.168.1.1:80"},
				{value: "192.168.1.2:80"},
				{value: "192.168.1.3:80"},
				{value: "192.168.1.4:80"},
				{value: "192.168.1.5:80"},
				{value: "192.168.1.6:80"},
			},
		},
		{
			capacity: 512,
			timeout:  3 * time.Minute,
			d:        &dialer{},
			ws:       &workers{wn: 16, number: 2048},
			addresses: []*address{
				{value: "192.168.1.1:80"},
				{value: "192.168.1.2:80"},
				{value: "192.168.1.3:80"},
				{value: "192.168.1.4:80"},
				{value: "192.168.1.5:80"},
				{value: "192.168.1.6:80"},
				{value: "192.168.1.7:80"},
				{value: "192.168.1.8:80"},
				{value: "192.168.1.9:80"},
				{value: "192.168.1.10:80"},
			},
		},
	}

	for _, e := range elements {
		e := e
		pool, _ := New(e.d.Dial, e.capacity, e.timeout)
		e.ws.cb = func(i int) error {
			addr := selectAddress(e.addresses)
			c, _ := pool.New(addr.value)
			c.Close()
			atomic.AddInt64(&addr.count, 1)
			return nil
		}

		e.ws.initialize()
		e.ws.run()

		var count, size, actual, idle, total int
		for _, addr := range e.addresses {
			b := pool.selectBucket(addr.value)
			t.Logf("%s (%d) size/actual-size/idle (%d/%d/%d) total (%d)",
				addr.value, addr.count, b.size, b._size(), b.idle, b.total)
			count += int(addr.count)
			size += b.size
			actual += b._size()
			idle += int(b.idle)
			total += int(b.total)
		}

		t.Logf("%s (%d) size/actual-size/idle (%d/%d/%d) total/dialer-count (%d/%d)",
			"summary", count, size, actual, idle, total, e.d.count)
		assert.Equal(t, e.ws.wn*e.ws.number, count)
		assert.Equal(t, size, actual)
		assert.Equal(t, size, idle)
		assert.Equal(t, int(e.d.count), total)

		pool.Close()

		for _, addr := range e.addresses {
			b := pool.selectBucket(addr.value)
			assert.Equal(t, 0, b.size)
			assert.Equal(t, 0, b._size())
			assert.Equal(t, 0, int(b.idle))
			assert.Equal(t, 0, int(b.total))
		}
		assert.Equal(t, 0, int(e.d.count))
	}
}

func TestPoolGet(t *testing.T) {
	type address struct {
		value string
		count int64
	}

	selectAddress := func(addresses []*address) *address {
		return addresses[int(rand.Int63())%len(addresses)]
	}

	elements := []struct {
		capacity  int
		timeout   time.Duration
		delay     time.Duration
		d         *dialer
		ws        *workers
		addresses []*address
	}{
		{
			capacity: 128,
			timeout:  3 * time.Minute,
			d:        &dialer{},
			ws:       &workers{wn: 1, number: 512},
			addresses: []*address{
				{value: "192.168.1.1:80"},
				{value: "192.168.1.2:80"},
				{value: "192.168.1.3:80"},
				{value: "192.168.1.4:80"},
			},
		},
		{
			capacity: 128,
			timeout:  3 * time.Minute,
			d:        &dialer{},
			ws:       &workers{wn: 128, number: 512},
			addresses: []*address{
				{value: "192.168.1.1:80"},
				{value: "192.168.1.2:80"},
				{value: "192.168.1.3:80"},
				{value: "192.168.1.4:80"},
			},
		},
		{
			capacity: 128,
			timeout:  3 * time.Minute,
			d:        &dialer{},
			ws:       &workers{wn: 256, number: 512},
			addresses: []*address{
				{value: "192.168.1.1:80"},
				{value: "192.168.1.2:80"},
			},
		},
		{
			capacity: 128,
			timeout:  3 * time.Minute,
			d:        &dialer{},
			ws:       &workers{wn: 512, number: 1024},
			addresses: []*address{
				{value: "192.168.1.1:80"},
				{value: "192.168.1.2:80"},
			},
		},
		{
			capacity: 128,
			timeout:  3 * time.Minute,
			delay:    10 * time.Millisecond,
			d:        &dialer{},
			ws:       &workers{wn: 512, number: 1024},
			addresses: []*address{
				{value: "192.168.1.1:80"},
				{value: "192.168.1.2:80"},
				{value: "192.168.1.3:80"},
				{value: "192.168.1.4:80"},
				{value: "192.168.1.5:80"},
				{value: "192.168.1.6:80"},
				{value: "192.168.1.7:80"},
				{value: "192.168.1.8:80"},
				{value: "192.168.1.9:80"},
				{value: "192.168.1.10:80"},
			},
		},
	}

	for _, e := range elements {
		e := e
		pool, _ := New(e.d.Dial, e.capacity, e.timeout)
		e.ws.cb = func(i int) error {
			addr := selectAddress(e.addresses)
			c, _ := pool.Get(addr.value)
			if e.delay != 0 {
				time.Sleep(e.delay)
			}
			c.Close()
			atomic.AddInt64(&addr.count, 1)
			return nil
		}

		e.ws.initialize()
		e.ws.run()

		var count, size, actual, idle, total int
		for _, addr := range e.addresses {
			b := pool.selectBucket(addr.value)
			t.Logf("%s (%d) size/actual-size/idle (%d/%d/%d) total (%d)",
				addr.value, addr.count, b.size, b._size(), b.idle, b.total)
			count += int(addr.count)
			size += b.size
			actual += b._size()
			idle += int(b.idle)
			total += int(b.total)
		}

		t.Logf("%s (%d) size/actual-size/idle (%d/%d/%d) total/dialer-count (%d/%d)",
			"summary", count, size, actual, idle, total, e.d.count)
		assert.Equal(t, e.ws.wn*e.ws.number, count)
		assert.Equal(t, size, actual)
		assert.Equal(t, size, idle)
		assert.Equal(t, int(e.d.count), total)

		pool.Close()

		for _, addr := range e.addresses {
			b := pool.selectBucket(addr.value)
			assert.Equal(t, 0, b.size)
			assert.Equal(t, 0, b._size())
			assert.Equal(t, 0, int(b.idle))
			assert.Equal(t, 0, int(b.total))
		}
		assert.Equal(t, 0, int(e.d.count))
	}
}

func TestBucketPush(t *testing.T) {
	elements := []struct {
		b         *bucket
		ws        *workers
		success   int64
		full      int64
		closed    int64
		threshold int
		d         *dialer
	}{
		{
			b:         &bucket{capacity: 128},
			ws:        &workers{wn: 1, number: 256},
			threshold: 256,
			d:         &dialer{},
		},
		{
			b:         &bucket{capacity: 256},
			ws:        &workers{wn: 4, number: 1024},
			threshold: 1024,
			d:         &dialer{},
		},
		{
			b:         &bucket{capacity: 256},
			ws:        &workers{wn: 4, number: 1024},
			threshold: 512,
			d:         &dialer{},
		},
		{
			b:         &bucket{capacity: 512},
			ws:        &workers{wn: 16, number: 4096},
			threshold: 1024,
			d:         &dialer{},
		},
	}

	for _, e := range elements {
		e := e
		e.ws.cb = func(i int) error {
			conn, _ := e.d.Dial("192.168.1.100:80")
			c := &Conn{Conn: conn, b: e.b}

			if i == e.threshold+1 {
				e.b._close()
			}

			switch e.b.push(c) {
			case nil:
				atomic.AddInt64(&e.success, 1)
			case bucketIsFull:
				atomic.AddInt64(&e.full, 1)
			case bucketIsClosed:
				atomic.AddInt64(&e.closed, 1)
			default:
			}

			return nil
		}

		e.ws.initialize()
		e.ws.run()

		total := e.ws.wn * e.ws.number
		t.Logf("bucket push: total (%d) success (%d) full (%d) closed (%d)",
			total, e.success, e.full, e.closed)

		assert.Equal(t, int(total), int(e.success+e.full+e.closed))
		assert.Equal(t, e.b.size, e.b._size())
		if e.ws.number <= e.threshold {
			assert.Equal(t, int(e.full), total-e.b.size)
		} else {
			assert.Equal(t, true, int(e.closed) > e.ws.number-e.threshold)
		}
	}
}

func TestBucketPop(t *testing.T) {
	elements := []struct {
		b         *bucket
		ws        *workers
		success   int64
		fail      int64
		threshold int
		d         *dialer
	}{
		{
			b:         &bucket{capacity: 128},
			ws:        &workers{wn: 1, number: 256},
			threshold: 256,
			d:         &dialer{},
		},
		{
			b:         &bucket{capacity: 256},
			ws:        &workers{wn: 4, number: 1024},
			threshold: 1024,
			d:         &dialer{},
		},
		{
			b:         &bucket{capacity: 512},
			ws:        &workers{wn: 4, number: 1024},
			threshold: 256,
			d:         &dialer{},
		},
		{
			b:         &bucket{capacity: 1024},
			ws:        &workers{wn: 16, number: 4096},
			threshold: 512,
			d:         &dialer{},
		},
	}

	for _, e := range elements {
		e := e
		bucketFill(e.b, &dialer{})
		e.ws.cb = func(i int) error {
			if i == e.threshold+1 {
				e.b._close()
			}

			if e.b.pop() != nil {
				atomic.AddInt64(&e.success, 1)
			} else {
				atomic.AddInt64(&e.fail, 1)
			}
			return nil
		}

		e.ws.initialize()
		e.ws.run()

		total := e.ws.wn * e.ws.number
		t.Logf("bucket pop: total (%d) success (%d) fail (%d)",
			total, e.success, e.fail)

		assert.Equal(t, int(total), int(e.success+e.fail))
		assert.Equal(t, e.b.size, e.b._size())
		if e.ws.number <= e.threshold {
			assert.Equal(t, e.b.capacity, int(e.success))
		} else {
			assert.Equal(t, true, int(e.fail) > e.ws.number-e.threshold)
		}
	}
}

func TestBucketCleanup(t *testing.T) {
	type element struct {
		t  *testing.T
		b  *bucket
		cb func(e *element) error
		d  *dialer
		*boolgen
		interruptNumber int
		pushNumber      int
		popNumber       int
	}
	elements := []*element{
		&element{
			t: t,
			b: &bucket{
				capacity:  256,
				interrupt: make(chan chan struct{}),
			},
			d: &dialer{},
			cb: func(e *element) error {
				e.interruptNumber++
				return nil
			},
		},
		&element{
			t: t,
			b: &bucket{
				capacity:  512,
				interrupt: make(chan chan struct{}),
			},
			d: &dialer{},
			cb: func(e *element) error {
				e.interruptNumber++
				e.pushNumber += bucketPush(e.b, e.d, 4)
				return nil
			},
		},
		&element{
			t: t,
			b: &bucket{
				capacity:  1024,
				interrupt: make(chan chan struct{}),
			},
			d: &dialer{},
			cb: func(e *element) error {
				e.interruptNumber++
				e.popNumber += bucketPop(e.b, 4)
				return nil
			},
		},
		&element{
			t: t,
			b: &bucket{
				capacity:  1024,
				interrupt: make(chan chan struct{}),
			},
			d:       &dialer{},
			boolgen: newBoolgen(),
			cb: func(e *element) error {
				e.interruptNumber++
				if e.boolgen.Bool() {
					e.pushNumber += bucketPush(e.b, e.d, 6)
				} else {
					e.popNumber += bucketPop(e.b, 7)
				}
				return nil
			},
		},
		&element{
			t: t,
			b: &bucket{
				capacity:  512,
				interrupt: make(chan chan struct{}),
			},
			d:       &dialer{},
			boolgen: newBoolgen(),
			cb: func(e *element) error {
				e.interruptNumber++
				if e.boolgen.Bool() {
					e.pushNumber += bucketPush(e.b, e.d, 16)
				} else {
					e.popNumber += bucketPop(e.b, 4)
				}
				return nil
			},
		},
		// Trivial case (zero capacity).
		&element{
			t: t,
			b: &bucket{
				capacity:  0,
				interrupt: make(chan chan struct{}),
			},
			d:       &dialer{},
			boolgen: newBoolgen(),
			cb: func(e *element) error {
				e.interruptNumber++
				if e.boolgen.Bool() {
					e.pushNumber += bucketPush(e.b, e.d, 16)
				} else {
					e.popNumber += bucketPop(e.b, 4)
				}
				return nil
			},
		},
	}

	for _, e := range elements {
		var (
			e         = e
			unused    = 0
			cleanDone = make(chan struct{})
		)

		bucketFill(e.b, e.d)
		go func() {
			unused = e.b.cleanup(false)
			close(cleanDone)
		}()

		for done := range e.b.interrupt {
			e.cb(e)
			close(done)
		}
		<-cleanDone

		e.t.Logf("size/actual-size/idle (%d/%d/%d)  push/pop/unused (%d/%d/%d) total/dialer-count(%d/%d)",
			e.b.size, e.b._size(), e.b.idle,
			e.pushNumber, e.popNumber, unused,
			e.b.total, e.d.count)

		assert.Equal(t, e.b.size, e.b._size())
		assert.Equal(t, e.b.size, int(e.b.idle))
		assert.Equal(t, e.b.total, e.d.count)
		assert.Equal(t, int(e.b.total)-e.b.capacity, e.pushNumber-(e.popNumber+unused))

		size := e.b.size
		unused = e.b.cleanup(true)
		assert.Equal(t, 0, int(e.d.count))
		assert.Equal(t, size, unused)
	}
}

func TestBucketCleanupEx(t *testing.T) {
	elements := []struct {
		b *bucket
		d *dialer
	}{
		{
			b: &bucket{capacity: 256},
			d: &dialer{},
		},
		{
			b: &bucket{capacity: 1024},
			d: &dialer{},
		},
		{
			b: &bucket{capacity: 2048},
			d: &dialer{},
		},
		{
			b: &bucket{capacity: 4096},
			d: &dialer{},
		},
		{
			b: &bucket{capacity: 10000},
			d: &dialer{},
		},
	}

	for _, e := range elements {
		_, expectedUnused := bucketRandomPush(e.b, e.d, e.b.capacity)
		assert.Equal(t, true, checkOrder(e.b, false))
		unused := e.b.cleanup(false)
		t.Logf("expected/actual unused (%d/%d) size/actual-size (%d/%d)",
			expectedUnused, unused, e.b.size, e.b._size())
		assert.Equal(t, e.b.capacity, e.b.size+unused)
		assert.Equal(t, expectedUnused, unused)
		assert.Equal(t, true, checkOrder(e.b, true))
	}
}

func checkOrder(b *bucket, asc bool) bool {
	var prev int

	if !asc {
		// descending order.
		prev = b.capacity + 1
	}

	for top := b.top; top != nil; top = top.next {
		current := top.conn.Conn.(*connection).index
		if asc && current < prev {
			return false
		} else if !asc && current > prev {
			return false
		}
		prev = current
	}
	return true
}

// This is an auxiliary connection type to print some operation information.
// It satisfies net.Conn interface (But it doesn't satisfy all requirements
// in comments of each method).
type connection struct {
	d      *dialer
	local  net.Addr
	remote net.Addr
	index  int
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

type dialer struct {
	localPort int32
	count     int64
}

func (d *dialer) Dial(address string) (net.Conn, error) {
	c := &connection{d: d}
	c.index = int(atomic.AddInt32(&d.localPort, 1))
	c.local, _ = net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:%d", c.index))
	c.remote, _ = net.ResolveTCPAddr("tcp", address)
	atomic.AddInt64(&d.count, 1)
	return c, nil
}

type worker struct {
	number int
	cb     func(int) error
}

func (w *worker) run(wg *sync.WaitGroup) error {
	defer wg.Done()
	for i := 0; i < w.number; i++ {
		if err := w.cb(i); err != nil {
			return err
		}
	}
	return nil
}

type workers struct {
	wn     int
	number int
	cb     func(int) error

	ws []*worker
	wg *sync.WaitGroup
}

func (ws *workers) initialize() {
	ws.ws = make([]*worker, ws.wn)
	ws.wg = &sync.WaitGroup{}

	for i := 0; i < ws.wn; i++ {
		ws.ws[i] = &worker{ws.number, ws.cb}
	}
}

func (ws *workers) run() {
	for _, w := range ws.ws {
		w := w
		ws.wg.Add(1)
		go w.run(ws.wg)
	}
	ws.wg.Wait()
}

// 'number' is the expected number of pushing operations, and return value
// is the actual number of pushing operations.
func bucketPush(b *bucket, d *dialer, number int) (i int) {
	for i = 0; i < number; i++ {
		conn, _ := d.Dial("192.168.1.100:80")
		c := b.bind(conn)
		if b.push(c) != nil {
			c.Close()
			break
		}
	}
	return
}

func bucketRandomPush(b *bucket, d *dialer, number int) (i, unused int) {
	bg := newBoolgen()
	for i = 0; i < number; i++ {
		conn, _ := d.Dial("192.168.1.100:80")
		c := b.bind(conn)
		if b.push(c) != nil {
			c.Close()
			break
		}

		if bg.Bool() {
			c.state = 0
			unused++
		}
	}
	return
}

// 'number' is the expected number of popping operations, and return value
// is the actual number of popping operations.
func bucketPop(b *bucket, number int) (i int) {
	for i = 0; i < number; i++ {
		if conn := b.pop(); conn != nil {
			conn.Release()
		} else {
			break
		}
	}
	return
}

func bucketFill(b *bucket, d *dialer) {
	bucketPush(b, d, b.capacity-b.size)
}

// The original design of the following struct is from StackOverflow:
// https://stackoverflow.com/questions/45030618/generate-a-random-bool-in-go?answertab=active#tab-top
type boolgen struct {
	src       rand.Source
	cache     int64
	remaining int
}

func newBoolgen() *boolgen {
	return &boolgen{src: rand.NewSource(time.Now().UnixNano())}
}

func (b *boolgen) Bool() bool {
	if b.remaining == 0 {
		b.cache, b.remaining = b.src.Int63(), 63
	}

	result := b.cache&0x01 == 1
	b.cache >>= 1
	b.remaining--

	return result
}
