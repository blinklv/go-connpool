// example_test.go
//
// Author: blinklv <blinklv@icloud.com>
// Create Time: 2018-07-31
// Maintainer: blinklv <blinklv@icloud.com>
// Last Change: 2018-07-31

package connpool_test

import (
	"github.com/blinklv/go-connpool"
	"log"
	"net"
	"time"
)

func dial(address string) (net.Conn, error) {
	return net.Dial("tcp", address)
}

func selectAddress() string { return "" }

func handle(net.Conn) error { return nil }

var pool, _ = connpool.New(dial, 128, 5*time.Minute)

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
