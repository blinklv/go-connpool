// example_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2019-03-22
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2019-03-25

package connpool_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/blinklv/go-connpool"
	"github.com/stretchr/testify/assert"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The following codes are the black box test of this package
func TestPackage(t *testing.T) {
	ss := servers{
		&server{
			address: "127.0.0.1:28081",
			c:       &codec{},
		},
		&server{
			address: "127.0.0.1:28082",
			c:       &codec{},
		},
		&server{
			address: "127.0.0.1:28083",
			c:       &codec{},
		},
		&server{
			address: "127.0.0.1:28084",
			c:       &codec{},
		},
	}

	c := &client{
		d:    &dialer{},
		s:    &scheduler{},
		c:    &codec{},
		exit: make(chan chan struct{}),
	}
	c.pool, _ = connpool.New(c.d.dial, poolCapacity, poolCleanupPeriod)
	c.s.init(ss)

	results := autoStats(c, ss)

	go ss.run()
	go c.run()

outer:
	for {
		select {
		case result := <-results:
			assert.Equalf(t, true, aequal(result.dial_rest, result.client_total_conn, 0.05),
				"dial_rest:%d !~= client_total_conn:%d", result.dial_rest, result.client_total_conn)
			assert.Equalf(t, true, aequal(result.client_total_conn, result.server_session_num, 0.05),
				"client_total_conn:%d !~= server_session_num:%d", result.client_total_conn, result.server_session_num)
			assert.Equalf(t, true, aequal(result.client_total_req, result.server_total_req, 0.05),
				"client_total_req:%d !~= server_total_req:%d", result.client_total_req, result.server_total_req)
			assert.Equalf(t, true, aequal(result.client_succ_req, result.server_succ_req, 0.05),
				"client_succ_req:%d !~= server_succ_req:%d", result.client_succ_req, result.server_succ_req)
		case done := <-c.exit:
			close(done)
			break outer
		}
	}

	stats(c, ss)
	ss.exit()
	stats(c, ss)
}

const (
	statsPeriod            = 10 * time.Second
	clientWorkerNumber     = 1024
	requestNumberPerWorker = 50000
	poolCapacity           = clientWorkerNumber
	poolCleanupPeriod      = 1 * time.Minute
)

var (
	sprintf = fmt.Sprintf
	errorf  = fmt.Errorf
)

type servers []*server

func (ss servers) run() {
	wg := sync.WaitGroup{}
	for _, s := range ss {
		wg.Add(1)
		go func(s *server) {
			s.run()
			wg.Done()
		}(s)
	}
	wg.Wait()
}

func (ss servers) exit() {
	for _, s := range ss {
		s.exit()
	}
}

type server struct {
	address string
	ln      net.Listener
	c       *codec

	session_num int64
	total_req   int64
	succ_req    int64
}

func (s *server) run() {
	var (
		err       error
		tempDelay time.Duration
	)

	s.ln, err = net.Listen("tcp", s.address)
	if err != nil {
		logf("start server (%s) failed: %s", s, err)
		return
	}

	logf("start server (%s) start", s)
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				logf("server (%s) accept connection failed: %s; retrying in %v", s, err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			break
		}
		go (&session{s}).handle(conn)
	}
	logf("stop server (%s)", s)
}

func (s *server) exit() {
	s.ln.Close()
}

func (s *server) String() string {
	return s.address
}

type session struct {
	s *server
}

func (sess *session) handle(conn net.Conn) {
	atomic.AddInt64(&sess.s.session_num, 1)
	for {
		request, err := sess.s.c.decode(conn)
		if err != nil {
			break
		}

		atomic.AddInt64(&sess.s.total_req, 1)

		if err = sess.s.c.encode(conn, request); err != nil {
			logf("handle connection (%s <- %s) failed: %s",
				conn.RemoteAddr(), conn.LocalAddr(), err)
			break
		}
		atomic.AddInt64(&sess.s.succ_req, 1)
	}
	conn.Close()
	atomic.AddInt64(&sess.s.session_num, -1)
}

type client struct {
	pool *connpool.Pool
	d    *dialer
	s    *scheduler
	c    *codec
	seq  int64
	exit chan chan struct{}
}

func (c *client) run() {
	logf("start client")

	wg := &sync.WaitGroup{}
	for i := 0; i < clientWorkerNumber; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < requestNumberPerWorker; j++ {
				address, _ := c.s.get()
				c.s.feedback(address, c.request(address) == nil)
			}
			wg.Done()
		}()
		time.Sleep(time.Second)
	}

	wg.Wait()

	c.pool.Close()
	done := make(chan struct{})
	c.exit <- done
	<-done
	logf("stop client")
}

func (c *client) request(address string) error {
	conn, err := c.pool.Get(address)
	if err != nil {
		return err
	}

	if err = c.roundtrip(conn); err != nil {
		conn.(*connpool.Conn).Release()
		if conn, err = c.pool.New(address); err != nil {
			return err
		}
		err = c.roundtrip(conn)
	}

	if conn != nil {
		conn.Close()
	}

	return nil
}

func (c *client) roundtrip(conn net.Conn) error {
	request := []byte(sprintf("message %d", atomic.AddInt64(&c.seq, 1)))
	if err := c.c.encode(conn, request); err != nil {
		return err
	}

	response, err := c.c.decode(conn)
	if err != nil {
		return err
	}

	if bytes.Compare(request, response) != 0 {
		return errorf("request (%s) != response (%s)", request, response)
	}

	return nil
}

type stats_result struct {
	dial_total         int64
	dial_rest          int64
	client_total_req   int64
	client_succ_req    int64
	client_total_conn  int64
	client_idle_conn   int64
	server_total_req   int64
	server_succ_req    int64
	server_session_num int64
}

func autoStats(c *client, ss servers) chan *stats_result {
	results := make(chan *stats_result)
	go func() {
		timer := time.NewTimer(statsPeriod)
		for {
			<-timer.C
			results <- stats(c, ss)
			timer.Reset(statsPeriod)
		}
	}()
	return results
}

func stats(c *client, ss servers) *stats_result {
	var (
		strs   = make([]string, len(c.s.addrs)+len(ss)+4)
		stats  = c.pool.Stats()
		result = &stats_result{}
		i, o   int
	)

	strs[0] = "*statistics*"
	result.dial_total, result.dial_rest = atomic.LoadInt64(&c.d.total), atomic.LoadInt64(&c.d.count)
	strs[1] = sprintf("%24s total:%-10d rest:%-10d", "dial", result.dial_total, result.dial_rest)

	i, o = 3, 2
	for _, addr := range c.s.addrs {
		for _, dest := range stats.Destinations {
			if addr.value == dest.Address {
				strs[i] = sprintf("%24s total-req:%-10d succ-req:%-10d total-conn:%-10d idle-conn:%-10d",
					addr.value, addr.total, addr.succ, dest.Total, dest.Idle)

				result.client_total_req += addr.total
				result.client_succ_req += addr.succ
				result.client_total_conn += dest.Total
				result.client_idle_conn += dest.Idle
			}
		}
		i++
	}
	strs[o] = sprintf("%24s total-req:%-10d succ-req:%-10d total-conn:%-10d idle-conn:%-10d",
		"client",
		result.client_total_req,
		result.client_succ_req,
		result.client_total_conn,
		result.client_idle_conn)

	o = i
	for _, s := range ss {
		tr, sr, sn := atomic.LoadInt64(&s.total_req), atomic.LoadInt64(&s.succ_req), atomic.LoadInt64(&s.session_num)
		strs[i+1] = sprintf("%24s total-req:%-10d succ-req:%-10d session-num:%-10d", s, tr, sr, sn)
		result.server_total_req += tr
		result.server_succ_req += sr
		result.server_session_num += sn
		i++
	}

	strs[o] = sprintf("%24s total-req:%-10d succ-req:%-10d session-num:%-10d",
		"server",
		result.server_total_req,
		result.server_succ_req,
		result.server_session_num)

	logf("%s", strings.Join(strs, "\n"))
	return result
}

type codec struct{}

const (
	sop = "SOP" // start of packet
	eop = "EOP" // end of packet
)

func (c *codec) encode(w io.Writer, data []byte) error {
	var (
		err error
	)

	if _, err = io.WriteString(w, sop); err != nil {
		return err
	}
	if err = binary.Write(w, binary.BigEndian, uint32(len(data))); err != nil {
		return err
	}
	if _, err = w.Write(data); err != nil {
		return err
	}
	if _, err = io.WriteString(w, eop); err != nil {
		return err
	}

	return nil
}

func (c *codec) decode(r io.Reader) ([]byte, error) {
	var (
		err  error
		n    uint32
		data []byte
		buf  = make([]byte, 3)
	)

	if _, err = io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	if string(buf) != sop {
		return nil, errors.New("start of packet is invalid")
	}

	if err = binary.Read(r, binary.BigEndian, &n); err != nil {
		return nil, err
	}

	data = make([]byte, int(n))
	if _, err = io.ReadFull(r, data); err != nil {
		return nil, err
	}

	if _, err = io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	if string(buf) != eop {
		return nil, errors.New("end of packet is invalid")
	}

	return data, nil
}

type scheduler struct {
	i     int64
	n     int
	addrs []*address
}

func (s *scheduler) init(ss servers) {
	for _, svr := range ss {
		s.addrs = append(s.addrs, &address{value: svr.address})
	}
	s.n = len(s.addrs)
}

func (s *scheduler) get() (string, error) {
	i := int(atomic.AddInt64(&s.i, 1))
	return s.addrs[i%s.n].value, nil
}

func (s *scheduler) feedback(address string, ok bool) {
	for _, addr := range s.addrs {
		if addr.value == address {
			atomic.AddInt64(&addr.total, 1)
			if ok {
				atomic.AddInt64(&addr.succ, 1)
			}
		}
	}
}

type address struct {
	value string
	total int64
	succ  int64
}

type dialer struct {
	count int64
	total int64
}

func (d *dialer) dial(address string) (net.Conn, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	atomic.AddInt64(&d.count, 1)
	atomic.AddInt64(&d.total, 1)
	return &connection{conn, d}, nil
}

type connection struct {
	net.Conn
	d *dialer
}

func (c *connection) Close() error {
	atomic.AddInt64(&c.d.count, -1)
	return c.Conn.Close()
}

func logf(format string, args ...interface{}) {
	if testing.Verbose() {
		log.Printf(format, args...)
	}
}

// Approximately Equal ('≈').
func aequal(a, b int64, e float64) bool {
	avg := (a + b) / 2
	return avg == 0 || float64(abs(a-b))/float64(avg) <= e
}

// Absolute value of x.
func abs(x int64) int64 {
	if x >= 0 {
		return x
	}
	return -x
}
