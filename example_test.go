// example_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2019-03-22
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2019-03-27

package connpool_test

import (
	"bytes"
	"encoding/binary"
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

/* Auxilairy Functions and Variables for Example */
func dial(address string) (net.Conn, error) {
	return &connection{}, nil
}

func selectAddress() string { return "" }

func handle(net.Conn) error { return nil }

func roundtrip(net.Conn, string) (string, error) {
	return "Sometimes, just one second.", nil
}

var pool, _ = connpool.New(dial, 128, 5*time.Minute)

/* Black Box Test */
func TestPackage(t *testing.T) {
	ss := servers{
		&server{address: "127.0.0.1:28081"},
		&server{address: "127.0.0.1:28082"},
		&server{address: "127.0.0.1:28083"},
		&server{address: "127.0.0.1:28084"},
	}

	c := &client{
		d:    &dialer{},
		s:    (&scheduler{}).init(ss),
		exit: make(chan struct{}),
	}
	c.pool, _ = connpool.New(c.d.dial, poolCapacity, poolCleanupPeriod)

	go ss.run()
	time.Sleep(time.Second) // Wait for servers have already been running.
	go c.run()

	timer := time.NewTimer(samplingPeriod)

	var old = &stats{}
	exec := func(old *stats) *stats {
		s := &stats{}
		ss.sampling(s)
		c.sampling(s)
		s.assert(t)

		var diff = &stats{}
		*diff = *s

		diff.client_total_req -= old.client_total_req
		diff.client_succ_req -= old.client_succ_req
		diff.server_total_req -= old.server_total_req
		diff.server_succ_req -= old.server_succ_req

		logf("%s\n%s", "statistics", diff)
		return s
	}

	for {
		select {
		case <-timer.C:
			old = exec(old)
			timer.Reset(samplingPeriod)
		case <-c.exit:
			old = exec(old)
			return
		}
	}
}

/* Environment Constant */

const (
	samplingPeriod      = 10 * time.Second // The period of sampling statistical data.
	requestWorkerNum    = 1024             // The number of workers which send requests.
	workerBatch         = 16               // The number of workers generated at once.
	workerBatchInterval = 5 * time.Second  // The interval between two generating workers.
	requestNum          = 10000            // Request number per worker.
	requestMsgSize      = 1024             // Message size of a request.
	handleDelay         = time.Duration(0) // Server process delay.
	poolCapacity        = requestWorkerNum // The capacity of the pool.
	poolCleanupPeriod   = time.Minute      // The cleanup period of the pool.
)

/* Auxiliary Structs and Their Methods */

// Statistical data.
type stats struct {
	dial_num          int64 // Number of invoking the dial method.
	dial_total_conn   int64 // Number of current total connections.
	client_total_conn int64 // Number of current total connections related to the client.
	client_idle_conn  int64 // Number of idle connections related to the client.
	client_total_req  int64 // Number of total requests sent by the client.
	client_succ_req   int64 // Number of success requests sent by the client.
	client_worker_num int64 // Number of client workers.
	server_sess_num   int64 // Number of sessions related to the server.
	server_total_req  int64 // Number of total requests received by the server.
	server_succ_req   int64 // Number of success requests received by the server.
}

func (s *stats) String() string {
	return sprintf(
		strings.Repeat("%-24s:%10d\n", 10),
		"dial-num", s.dial_num,
		"dial-total-conn", s.dial_total_conn,
		"client-total-conn", s.client_total_conn,
		"client-idle-conn", s.client_idle_conn,
		"client-total-req", s.client_total_req,
		"client-succ-req", s.client_succ_req,
		"client-worker-num", s.client_worker_num,
		"server-sess-num", s.server_sess_num,
		"server-total-req", s.server_total_req,
		"server-succ-req", s.server_succ_req)
}

func (s *stats) assert(t *testing.T) {
	const precision = 0.05
	assert.Equalf(t, true, aequal(s.dial_total_conn, s.client_total_conn, precision),
		"|dial_total_conn:%d - client_total_conn:%d| > %v",
		s.dial_total_conn, s.client_total_conn, precision)
	assert.Equalf(t, true, aequal(s.server_sess_num, s.client_total_conn, precision),
		"|server_sess_num:%d - client_total_conn:%d| > %v",
		s.server_sess_num, s.client_total_conn, precision)
	assert.Equalf(t, true, aequal(s.client_total_req, s.server_total_req, precision),
		"|client_total_req:%d - server_total_req:%d| > %v",
		s.client_total_req, s.server_total_req, precision)
	assert.Equalf(t, true, aequal(s.client_succ_req, s.server_succ_req, precision),
		"|client_succ_req:%d - server_succ_req:%d| > %v",
		s.client_succ_req, s.server_succ_req, precision)
}

// Manage multiple server simulators.
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

// Sampling from multiple servers.
func (ss servers) sampling(s *stats) {
	for _, svr := range ss {
		s.server_sess_num += load(&svr.sess_num)
		s.server_total_req += load(&svr.total_req)
		s.server_succ_req += load(&svr.succ_req)
	}
}

// Server simulator.
type server struct {
	address string // Listen address.
	ln      net.Listener

	sess_num  int64 // Number of sessions.
	total_req int64 // Number of total requests to the server.
	succ_req  int64 // Number of success requests to the server.
}

func (s *server) run() {
	var err error
	s.ln, err = net.Listen("tcp", s.address)
	if err != nil {
		logf("start server (%s) failed (%s)", s.address, err)
		return
	}

	var delay time.Duration // How long to sleep on accept failure.

	logf("start server (%s)", s.address)
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if netError, ok := err.(net.Error); ok && netError.Temporary() {
				if delay == 0 {
					delay = 5 * time.Millisecond
				} else {
					delay *= 2
				}
				if delay > time.Second {
					delay = time.Second
				}

				logf("accept error (%v), retrying in %v", err, delay)
				time.Sleep(delay)
				continue
			}
			break
		}
		delay = 0

		go (&session{s}).handle(conn)
	}
	logf("stop server (%s)", s.address)
}

func (s *server) exit() {
	s.ln.Close()
}

// Connection session of the server simulator.
type session struct {
	*server
}

func (sess *session) handle(conn net.Conn) {
	add(&sess.sess_num, 1)
	for {
		request, err := decode(conn)
		if err != nil {
			break
		}

		add(&sess.total_req, 1)
		if err = encode(conn, request); err != nil {
			logf("handle connection (%s <- %s) failed (%s)",
				conn.RemoteAddr(), conn.LocalAddr(), err)
			break
		}
		add(&sess.succ_req, 1)

		if handleDelay != 0 {
			time.Sleep(handleDelay)
		}
	}
	conn.Close()
	add(&sess.sess_num, -1)
}

// Client simulator.
type client struct {
	pool       *connpool.Pool
	d          *dialer
	s          *scheduler
	exit       chan struct{}
	seq        int64 // Message sequence.
	worker_num int64 // Number of current workers.
}

func (c *client) run() {
	logf("start client")

	wg := &sync.WaitGroup{}
	for i := 0; i < requestWorkerNum; i++ {
		wg.Add(1)
		add(&c.worker_num, 1)
		go func() {
			for j := 0; j < requestNum; j++ {
				c.handle()
			}
			wg.Done()
			add(&c.worker_num, -1)
		}()

		if (i+1)%workerBatch == 0 {
			time.Sleep(workerBatchInterval)
		}
	}

	wg.Wait()
	c.pool.Close()
	logf("stop client")
	close(c.exit)
}

func (c *client) handle() {
	var (
		address, _ = c.s.get()
		request    = []byte(sprintf("message %d", add(&c.seq, 1)))
	)

	if padlen := requestMsgSize - len(request); padlen > 0 {
		request = append(request, bytes.Repeat([]byte("!"), padlen)...)
	}

	response, err := c.roundtrip(address, request)
	if err == nil && bytes.Compare(request, response) != 0 {
		err = errorf("request (%s) != response (%s)", request, response)
	}
	c.s.feedback(address, err == nil)
}

func (c *client) roundtrip(address string, request []byte) ([]byte, error) {
	conn, err := c.pool.Get(address)
	if err != nil {
		return nil, err
	}

	var response []byte
	if response, err = c._roundtrip(conn, request); err != nil {
		conn.(*connpool.Conn).Release()
		if conn, err = c.pool.New(address); err != nil {
			return nil, err
		}
		response, err = c._roundtrip(conn, request)
	}

	if conn != nil {
		conn.Close()
	}

	return response, nil
}

func (c *client) _roundtrip(conn net.Conn, request []byte) ([]byte, error) {
	if err := encode(conn, request); err != nil {
		return nil, err
	}

	return decode(conn)
}

// Sampling from the client.
func (c *client) sampling(s *stats) {
	s.dial_num, s.dial_total_conn, s.client_worker_num = load(&c.d.dial_num), load(&c.d.total_conn), load(&c.worker_num)
	for _, d := range (c.pool.Stats()).Destinations {
		s.client_total_conn += d.Total
		s.client_idle_conn += d.Idle
	}
	for _, addr := range c.s.addrs {
		s.client_total_req += load(&addr.total_req)
		s.client_succ_req += load(&addr.succ_req)
	}
}

type scheduler struct {
	i     int64
	n     int // Number of addresses.
	addrs []*struct {
		value     string
		total_req int64 // Number of total requests to the address.
		succ_req  int64 // Number of success requests to the address.
	}
}

// Initializes a scheduler obj and returns it.
func (s *scheduler) init(ss servers) *scheduler {
	for _, svr := range ss {
		s.addrs = append(s.addrs, &struct {
			value     string
			total_req int64
			succ_req  int64
		}{value: svr.address})
	}
	s.n = len(s.addrs)
	return s
}

// Get a server address from the scheduler.
func (s *scheduler) get() (string, error) {
	i := int(add(&s.i, 1))
	return s.addrs[i%s.n].value, nil
}

// Feedback to the scheduler whether the server corresponding to the address is stable.
func (s *scheduler) feedback(address string, ok bool) {
	for _, addr := range s.addrs {
		if addr.value == address {
			add(&addr.total_req, 1)
			if ok {
				add(&addr.succ_req, 1)
			}
		}
	}
}

type dialer struct {
	dial_num   int64 // Number of invoking the dial method.
	total_conn int64 // Number of current total connections.
}

// An implementation of connpool.Dial interface to create TCP connections.
func (d *dialer) dial(address string) (net.Conn, error) {
	add(&d.dial_num, 1)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	add(&d.total_conn, 1)
	return &connection{conn, d}, nil
}

// Wraps the raw net.Conn interface to track current total connections.
type connection struct {
	net.Conn
	d *dialer
}

func (c *connection) Close() error {
	add(&c.d.total_conn, -1)
	return c.Conn.Close()
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

// Output log information in verbose mode.
func logf(format string, args ...interface{}) {
	if testing.Verbose() {
		log.Printf(format, args...)
	}
}

// Approximately Equal ('≈'); check whether two integers are equal in error range.
func aequal(a, b int64, e float64) bool {
	// Don't compare small values.
	if a < 16 || b < 16 {
		return true
	}

	avg := (a + b) / 2
	return avg == 0 || float64(abs(a-b))/float64(avg) <= e
}

// Absolute value of an integer.
func abs(x int64) int64 {
	if x >= 0 {
		return x
	}
	return -x
}

// The following codes implements a simple binary protocol encode/decode.
//
// +--------+--------+--------+--------+--------+--------+--------+--------+
// | Start of Packet |      Length     |    Body Data    |   End of Packet |
// |      3 bytes    |      4 bytes    |      n bytes    |      3 bytes    |
// +--------+--------+--------+--------+--------+--------+--------+--------+

var (
	sop = []byte("SOP") // start of packet
	eop = []byte("EOP") // end of packet
)

func encode(w io.Writer, data []byte) error {
	_, err := w.Write(sop)
	if err != nil {
		return err
	}

	n := uint32(len(data))
	_, err = w.Write([]byte{byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)})
	if err != nil {
		return err
	}

	if _, err = w.Write(data); err != nil {
		return err
	}

	_, err = w.Write(eop)
	return err
}

func decode(r io.Reader) ([]byte, error) {
	var (
		err  error
		n    uint32
		data []byte
		buf  = make([]byte, 3)
	)

	if _, err = io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	if bytes.Compare(buf, sop) != 0 {
		return nil, errorf("start of packet (%v) is invalid", buf)
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
	if bytes.Compare(buf, eop) != 0 {
		return nil, errorf("end of packet (%v) is invalid", buf)
	}

	return data, nil
}
