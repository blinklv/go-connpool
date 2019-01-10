# go-connpool

[![Build Status](https://travis-ci.com/blinklv/go-connpool.svg?branch=master)](https://travis-ci.com/blinklv/go-connpool)
[![GoDoc](https://godoc.org/github.com/blinklv/go-connpool?status.svg)](https://godoc.org/github.com/blinklv/go-connpool)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A concurrency-safe [connection pool][] package in [Go][]. It can be used to manage and reuse connections based on the destination address of which. This design makes a pool work better with some [name server][]s.

## Motivation

In some cases, your backend servers have multiple addresses. Before you access a specific server, you need to get the address of which from a name server. The configuration of addresses in the name server usually will be modified in the future, like removing records or adjusting weights. So a connection pool which can respond to these changes quickly is needed, which is why this pool exists. 


## Installation

```bash
$ go get github.com/blinklv/go-connpool
```

## Usage

I only introduce some simple usages here. The more details of **API** you can get from [GoDoc](https://godoc.org/github.com/blinklv/go-connpool).

#### Create and close a connection pool

You need to create a `Pool` instance at first. There're three parameters you should specify. 

- `dial`: defines how to create a new connection.
- `capacity`: controls the maximum idle connections to keep **per-host** (*not all hosts*).
- `period`: specifies period between two cleanup ops, which closes some idle connections not used for a long time (*about 1 ~ 2 period*). It can't be less than 1 min in this version; otherwise, many CPU cycles are occupied by the cleanup task. I usually set it to 3 ~ 5 min, but if there exist too many resident connections in your program, this value should be larger.

You can make some operations on this `Pool` instance after creating it successfully. If you don't use it anymore, please remember to close it. Invoking `Pool.Close` method can ensure that all resources related to the connection pool will be released.

```go
dial := func(address string) (net.Conn, error) {
    return net.Dial("tcp", address)
}

pool, err := connpool.New(dial, 128, 5*time.Minute)
if err != nil {
    log.Fatalf("create connection pool failed %s", err)
}

run(pool)

pool.Close()
```

#### Get and create connections from a pool

You can get a connection from your `Pool` instance, the destination address of which is equal to the `address` argument passed by you. If there exist some idle connections related to this address in the pool, one of which will be returned directly to you. I think this is why we use the connection pool. 

```go
conn, err := pool.Get(selectAddress())
if err != nil {
    return err
}
if err = handle(conn); err != nil {
    return err
}
return conn.Close()
```

`selectAddress` and `handle` function in the above example are your custom functions. The former one can return an available destination address; it's usually related to a name server. The latter one specifies how to handle the connection. How should we do if we get a dead connection, which means the connection has been closed by the peer but not detected? Can we invoke `Pool.Get` method with the same address again? It can work in typical cases. However, what if we still fail? The most terrible thing is that all idle connections corresponded to the address have been dead, which might happen when the backend server was crashed. In this lousy case, retrying will take a lot of time. `Pool.New` is an alternative method; it's more suitable for solving this problem. 

```go
address := selectAddress()
conn, err := pool.Get(address)
if err != nil {
    return err
}

if err = handle(conn); err != nil && isClosed(err) {
    conn.(*connpool.Conn).Release()
    if conn, err = pool.New(address); err != nil {
        return err
    }
    err = handle(conn)
}

conn.Close()
return err
```

`Pool.New` creates a new connection by using the underlying dial field instead of acquiring an existing connection in the pool. This way can guarantee to get a valid connection in the first retrying unless the background can't serve normally. No matter which way we use to get a connection from the pool, we must close it at the end. `Conn.Close` method tries to put the connection into the pool if there is enough room in which. This step is very critical, so you shouldn't ignore it; Otherwise, no connection will be reused. In fact, even though you don't use any connection pool, closing connections is necessary to prevent resource leak. 

#### Release connections

Have you noticed the statement `conn.(*connpool.Conn).Release()` in the above example?

```go
if err := handle(conn); err != nil && isClosed(err) {
    conn.(*connpool.Conn).Release()
    // ... 
}
```

Although the connection was dead, it doesn't mean you free its resources. We shouldn't call its `Close` method, because the invalid connection will be likely put into the pool again. By contrast, `Conn.Release` method is better, which can free the underlying connection directly. This strategy avoids that dead connections are repeatedly used.


## License 

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details

[connection pool]: https://en.wikipedia.org/wiki/Connection_pool
[Go]: https://golang.org/
[name server]: https://en.wikipedia.org/wiki/Name_server
