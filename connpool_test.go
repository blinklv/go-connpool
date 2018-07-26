// connpool_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-07-11
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2018-07-26

package connpool

import (
	"fmt"
	"github.com/bmizerany/assert"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

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
		fillBucket(&dialer{}, e.b)
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

func TestBucketClean(t *testing.T) {
	type element struct {
		b               *bucket
		cb              func(e *element) error
		d               *dialer
		interruptNumber int
	}
	elements := []*element{
		&element{
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
	}

	for _, e := range elements {
		e := e
		fillBucket(e.d, e.b)
		go e.b.clean(false)
		for done := range e.b.interrupt {
			e.cb(e)
			close(done)
		}
		e.b.clean(true)
		t.Logf("interrupt number (%d) rest connections (%d) ",
			e.interruptNumber, e.d.count)
	}
}

// This is an auxiliary connection type to print some operation information.
// It satisfies net.Conn interface (But it doesn't satisfy all requirements
// in comments of each method).
type connection struct {
	d      *dialer
	local  net.Addr
	remote net.Addr
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
	c.local, _ = net.ResolveTCPAddr("tcp",
		fmt.Sprintf("127.0.0.1:%d", atomic.AddInt32(&d.localPort, 1)))
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

func fillBucket(d *dialer, b *bucket) {
	for {
		conn, _ := d.Dial("192.168.1.100:80")
		c := &Conn{Conn: conn, b: b}

		if b.push(c) != nil {
			c.Close()
			break
		}
	}
	return
}
