// example_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-07-31
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2018-08-01

package connpool_test

import (
	"fmt"
	"github.com/blinklv/go-connpool"
	"log"
	"net"
	"time"
)

type connection struct{}

func (c *connection) Read(b []byte) (int, error)         { return len(b), nil }
func (c *connection) Write(b []byte) (int, error)        { return len(b), nil }
func (c *connection) Close() error                       { return nil }
func (c *connection) LocalAddr() net.Addr                { return nil }
func (c *connection) RemoteAddr() net.Addr               { return nil }
func (c *connection) SetDeadline(t time.Time) error      { return nil }
func (c *connection) SetReadDeadline(t time.Time) error  { return nil }
func (c *connection) SetWriteDeadline(t time.Time) error { return nil }

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
