// example_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-07-31
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2018-08-06

package connpool_test

import (
	"fmt"
	"github.com/blinklv/go-connpool"
	"log"
	"net"
	"sync/atomic"
	"time"
)

func dial(address string) (net.Conn, error) {
	return &connection{}, nil
}

func selectAddress() string { return "" }

func handle(net.Conn) error { return nil }

func roundtrip(net.Conn, string) (string, error) {
	return "Sometimes, just one second.", nil
}

var pool, _ = connpool.New(dial, 128, 5*time.Minute)

// The following example illustrates how to use this package in your client program.
func Example() {
	conn, err := pool.Get(selectAddress())
	if err != nil {
		return
	}
	defer conn.Close()

	response, err := roundtrip(conn, "How long is forever?")
	if err != nil {
		conn.(*connpool.Conn).Release()
		if conn, err = pool.New(selectAddress()); err != nil {
			return
		}
		response, err = roundtrip(conn, "How long is forever?")
	}

	fmt.Printf("%s", response)
	// Output:
	// Sometimes, just one second.
}

func ExampleNew() {
	dial := func(address string) (net.Conn, error) {
		return net.Dial("tcp", address)
	}

	pool, err := connpool.New(dial, 128, 5*time.Minute)
	if err != nil {
		log.Fatalf("create connection pool failed %s", err)
	}
	pool.Close()
}

func ExamplePool_Get() {
	conn, err := pool.Get(selectAddress())
	if err != nil {
		log.Fatalf("get a connection failed %s", err)
	}
	conn.Close()
}

func ExampleConn_Release() {
	conn, _ := pool.New(selectAddress())
	conn.(*connpool.Conn).Release()
}

// The following is a black box test for this package.
var servers = map[string]*server{}

type addressUnit struct {
	address string
	count   int64
	fail    int64
}

type scheduler struct {
	i            int64
	n            int
	addressMap   map[string]*addressUnit
	addressArray []*addressUnit
}

func (s *scheduler) initialize() {
	for address, _ := range servers {
		u := &addressUnit{address: address}
		s.addressMap[address] = u
		s.addressArray = append(s.addressArray, u)
	}
}

func (s *scheduler) get() (string, error) {
	i := int(atomic.AddInt64(&s.i, 1))
	return s.addressArray[i%s.n].address, nil
}

func (s *scheduler) feedback(address string, ok bool) {
	u := s.addressMap[address]
	atomic.AddInt64(&u.count, 1)
	if !ok {
		atomic.AddInt64(&u.fail, 1)
	}
}

// An implementation of the net.Error interface.
type netError struct {
	error
	timeout   bool
	temporary bool
	broken    bool
}

func (ne *netError) Timeout() bool {
	return ne.timeout
}

func (ne *netError) Temporary() bool {
	return ne.temporary
}

func (ne *netError) Broken() bool {
	return ne.broken
}

type client struct {
	pool *connpool.Pool
}

type server struct {
	address       net.Addr // listen address
	delay         time.Duration
	lifetime      time.Duration
	sessionNumber int64
	accept        chan struct {
		*pipe
		remote net.Addr
	}
}

func (s *server) run() {
	log.Printf("server (%s) start", s.address)
outer:
	for {
		select {
		case c := <-s.accept:
			go (&session{s}).handle(&connection{
				pipe:   c.pipe,
				local:  s.address,
				remote: c.remote,
			})
		case <-time.After(s.lifetime):
			break outer
		}
	}
	close(s.accept)
	log.Printf("server (%s) stop", s.address)
}

type session struct {
	s *server
}

func (s *session) handle(c net.Conn) {
	var (
		n   int
		err error
	)

	atomic.AddInt64(&s.s.sessionNumber, 1)
	for {
		b := make([]byte, 1024)
		n, err = c.Read(b)
		if err != nil {
			break
		}
		if _, err = c.Write(b[:n]); err != nil {
			break
		}
	}

	if !err.(*netError).Broken() {
		log.Printf("handle connection (%s -> %s) error %s",
			c.RemoteAddr(), c.LocalAddr(), err)
	}
	atomic.AddInt64(&s.s.sessionNumber, -1)
}

type dialer struct {
	localPort int32
}

func (d *dialer) Dial(address string) (conn net.Conn, err error) {
	c := &connection{
		pipe: &pipe{
			read:  make(chan []byte),
			write: make(chan []byte),
		},
		local:  resolveAddr(fmt.Sprintf("127.0.0.1:%d", atomic.AddInt32(&d.localPort, 1))),
		remote: resolveAddr(address),
	}

	// The connection will be returned to the user only after the server
	// accept it.
	s := servers[address]

	defer func() {
		if x := recover(); x != nil {
			conn, err = nil, fmt.Errorf("server (%s) is down", address)
		}
	}()

	s.accept <- struct {
		*pipe
		remote net.Addr
	}{
		&pipe{read: c.pipe.write, write: c.pipe.read},
		c.local,
	}

	return c, nil
}

type pipe struct {
	read  chan []byte
	write chan []byte
}

func (p *pipe) close() {
	close(p.read)
	close(p.write)
}

type connection struct {
	*pipe
	local         net.Addr
	remote        net.Addr
	readDeadline  time.Time
	writeDeadline time.Time
}

func (c *connection) Read(b []byte) (int, error) {
	timeout := make(<-chan time.Time)
	if !c.readDeadline.IsZero() {
		timeout = after(c.readDeadline.Sub(time.Now()))
	}

	select {
	case data, more := <-c.pipe.read:
		if !more {
			return 0, &netError{
				error:  fmt.Errorf("read broken pipe (%s -> %s)", c.remote, c.local),
				broken: true,
			}
		}
		// excess part will be discarded.
		return copy(b, data), nil
	case <-timeout:
		return 0, &netError{
			error:   fmt.Errorf("read timeout (%s -> %s)", c.remote, c.local),
			timeout: true,
		}
	}
}

func (c *connection) Write(b []byte) (n int, err error) {
	timeout := make(<-chan time.Time)
	if !c.writeDeadline.IsZero() {
		timeout = after(c.writeDeadline.Sub(time.Now()))
	}

	defer func() {
		if x := recover(); x != nil {
			n, err = 0, &netError{
				error:  fmt.Errorf("write broken pipe (%s -> %s)", c.local, c.remote),
				broken: true,
			}
		}
	}()

	select {
	case c.pipe.write <- b:
		return len(b), nil
	case <-timeout:
		return 0, &netError{
			error:   fmt.Errorf("write timeout (%s -> %s)", c.local, c.remote),
			timeout: true,
		}
	}
}

func (c *connection) Close() error {
	c.pipe.close()
	return nil
}

func (c *connection) LocalAddr() net.Addr {
	return c.local
}

func (c *connection) RemoteAddr() net.Addr {
	return c.remote
}

func (c *connection) SetDeadline(t time.Time) error {
	c.readDeadline, c.writeDeadline = t, t
	return nil
}

func (c *connection) SetReadDeadline(t time.Time) error {
	c.readDeadline = t
	return nil
}

func (c *connection) SetWriteDeadline(t time.Time) error {
	c.writeDeadline = t
	return nil
}

func after(d time.Duration) <-chan time.Time {
	if d > 0 {
		return time.After(d)
	}

	c := make(chan time.Time)
	close(c)
	return c
}

func resolveAddr(address string) net.Addr {
	addr, _ := net.ResolveTCPAddr("tcp", address)
	return addr
}
