// connpool_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-07-11
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2018-07-12

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

// This is an auxiliary connection type to print some operation information.
// It satisfies net.Conn interface (But it doesn't satisfy all requirements
// in comments of each method).
type dummyConn struct {
	remoteAddr string
	localAddr  string
}

func (dc *dummyConn) Read(b []byte) (n int, err error) {
	return len(b), nil
}

func (dc *dummyConn) Write(b []byte) (n int, err error) {
	return len(b), nil
}

func (dc *dummyConn) Close() error {
	fmt.Printf("close connection (%s -> %s)\n", dc.localAddr, dc.remoteAddr)
	return nil
}

func (dc *dummyConn) LocalAddr() net.Addr {
	addr, _ := net.ResolveTCPAddr("tcp", dc.localAddr)
	return addr
}

func (dc *dummyConn) RemoteAddr() net.Addr {
	addr, _ := net.ResolveTCPAddr("tcp", dc.remoteAddr)
	return addr
}

func (dc *dummyConn) SetDeadline(t time.Time) error      { return nil }
func (dc *dummyConn) SetReadDeadline(t time.Time) error  { return nil }
func (dc *dummyConn) SetWriteDeadline(t time.Time) error { return nil }

var dummyPort int32

// A trivial implementation of the Dial type.
func dummyDial(address string) (net.Conn, error) {
	return &dummyConn{address, fmt.Sprintf("127.0.0.1:%d", atomic.AddInt32(&dummyPort, 1))}, nil
}

func genConn(b *bucket, address string) *Conn {
	c, _ := dummyDial(address)
	return &Conn{c, b, 0}
}

func TestNew(t *testing.T) {
	arguments := []struct {
		dial     Dial
		capacity int
		timeout  time.Duration
		ok       bool
	}{
		{nil, 32, 5 * time.Minute, false},
		{dummyDial, -10, 5 * time.Minute, false},
		{func(address string) (net.Conn, error) { return net.Dial("tcp", address) }, 32, time.Second, false},
		{dummyDial, 128, 5 * time.Minute, true},
	}

	for _, argument := range arguments {
		pool, err := New(argument.dial, argument.capacity, argument.timeout)
		if argument.ok {
			assert.NotEqual(t, nil, pool)
			assert.Equal(t, nil, err)
			assert.Equal(t, nil, pool.Close())
		} else {
			assert.Equal(t, (*Pool)(nil), pool)
			assert.NotEqual(t, nil, err)
		}
	}
}

func TestBucketPush(t *testing.T) {
	samples := []struct {
		b            *bucket
		workers      int
		opNumber     int
		closedNumber int
	}{
		{&bucket{capacity: 128}, 1, 10000, 10001},
		{&bucket{capacity: 256}, 1, 10000, 5001},
		{&bucket{capacity: 512}, 2, 10000, 5001},
		{&bucket{capacity: 1024}, 10, 100000, 50001},
		{&bucket{capacity: 50000}, 128, 100000, 80000},
	}

	for _, sample := range samples {
		var (
			count   int64
			success int64
			full    int64
			closed  int64
			end     = &sync.WaitGroup{}
			pause   = &sync.WaitGroup{}
			cont    = make(chan struct{})
		)

		push := func() {
			switch sample.b.push(genConn(sample.b, "192.168.1.100:80")) {
			case bucketIsFull:
				atomic.AddInt64(&full, 1)
			case bucketIsClosed:
				atomic.AddInt64(&closed, 1)
			default:
				atomic.AddInt64(&success, 1)
			}
		}

		actualSize := func(b *bucket) int {
			actual := 0
			for b.top != nil {
				actual++
				b.top = b.top.next
			}
			return actual
		}

		for worker := 0; worker < sample.workers; worker++ {
			pause.Add(1)
			end.Add(1)
			go func() {
				i := 0
				for {
					i = int(atomic.AddInt64(&count, 1))
					if i >= sample.closedNumber {
						break
					}
					push()
				}

				pause.Done()
				pause.Wait()
				if i == sample.closedNumber {
					sample.b.closed = true
					close(cont)
				}
				<-cont

				for i <= sample.opNumber {
					push()
					i = int(atomic.AddInt64(&count, 1))
				}

				end.Done()
			}()
		}
		end.Wait()

		assert.Equal(t, sample.opNumber, int(success+full+closed))
		assert.Equal(t, sample.closedNumber, int(success+full+1))
		assert.Equal(t, int(success), sample.b.size)
		assert.Equal(t, sample.b.size, actualSize(sample.b))
	}
}

func TestBucketPop(t *testing.T) {
	samples := []struct {
		b            *bucket
		workers      int
		opNumber     int
		closedNumber int
	}{
		{&bucket{capacity: 128}, 1, 10000, 10001},
		{&bucket{capacity: 256}, 4, 10000, 5001},
		{&bucket{capacity: 1024}, 20, 10000, 5001},
		{&bucket{capacity: 7000}, 32, 10000, 5001},
	}

	for _, sample := range samples {
		var (
			count   int64
			success int64
			fail    int64
			end     = &sync.WaitGroup{}
			pause   = &sync.WaitGroup{}
			cont    = make(chan struct{})
		)

		pop := func() {
			if sample.b.pop() != nil {
				atomic.AddInt64(&success, 1)
			} else {
				atomic.AddInt64(&fail, 1)
			}
		}

		for i := 0; i < sample.b.capacity; i++ {
			sample.b.push(genConn(sample.b, "192.168.1.100:80"))
		}

		for worker := 0; worker < sample.workers; worker++ {
			pause.Add(1)
			end.Add(1)
			go func() {
				i := 0
				for {
					i = int(atomic.AddInt64(&count, 1))
					if i >= sample.closedNumber {
						break
					}
					pop()
				}

				pause.Done()
				pause.Wait()
				if i == sample.closedNumber {
					sample.b.closed = true
					close(cont)
				}
				<-cont

				for i <= sample.opNumber {
					pop()
					i = int(atomic.AddInt64(&count, 1))
				}

				end.Done()
			}()
		}
		end.Wait()

		assert.Equal(t, sample.opNumber, int(success+fail))
		if sample.b.capacity < sample.closedNumber {
			assert.Equal(t, sample.b.capacity, int(success))
			assert.Equal(t, 0, sample.b.size)
		} else {
			assert.Equal(t, sample.b.capacity-sample.closedNumber+1, sample.b.size)
			assert.Equal(t, sample.closedNumber-1, int(success))
		}
	}
}
