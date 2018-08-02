# go-connpool

[![GoDoc](https://godoc.org/github.com/nsqio/go-nsq?status.svg)](https://godoc.org/github.com/blinklv/go-connpool)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A concurrent safe [connection pool][] package in [Go][]. It can be used to manage and reuse connections based on the destination address of which. This design make a pool work better with some [load balancers][load balancing].

## Install

```bash
$ go get github.com/blinklv/go-connpool
```

## Usage

I only introduce some simple usages here. The more details of **API** you can get from [GoDoc](https://godoc.org/github.com/blinklv/go-connpool).

#### Create and close a connection pool

You need to create a `Pool` instance at first. There're three parameters you should specify. 

- `dial`: defines how to create a new connection.
- `capacity`: controls the maximum idle connections to keep **per-host** (*not all hosts*).
- `timeout`: maximum amount of time an idle connection will remain idle before closing itself.

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

You can get a connection from your `Pool` instance, the destination address of which is equal to the `address` argument passed by you. If there exists some idle connections related to this address in the pool, one of which will be returned directly to you. I think this is why we use the connection pool. 

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

`selectAddress` and `handle` function in above example are your custom functions. The former one can return an available destination address; it's usually related to a load balancer. The latter one specifies how to handle the connection. How should we do if we get a dead connection? Which means the connection has been closed by the peer but not detected. Can we invoke `Pool.Get` method with the same address again? It can work in normal cases. However, what if we fail again? The most terrible thing is that all idle connections corresponded to the address have been dead, which might happen when the backend server was crashed. In this bad case, retrying will take a lot of time. `Pool.New` is an alternative method; it's more suitable for solving this problem. 

```go
address := selectAddress()
conn, err := pool.Get(address)
if err != nil {
    return err
}

if err := handle(conn); err != nil && isClosed(err) {
    conn.(*connpool.Conn).Release()
    if conn, err = pool.New(address); err != nil {
        return err
    }
    if err = handle(conn); err != nil {
        return err
    }
}

return conn.Close()
```

`Pool.New` creates a new connection by using the underlying dial field instead of acquiring a existing connection in the pool. This way can guarantee getting a valid connection in first retrying unless the background can't serve normally. No matter which way we use to get a connection from the pool, we must close it at the end. `Conn.Close` method tries to put the connection into the pool if there is enough room in which. This step is very critical, so you shouldn't ignore it; Otherwise, no connection will be reused. In fact, even though you don't use any connection pool, closing connections is necessary to prevent resource leak. 

#### Release connections

Have you noticed the statement `conn.(*connpool.Conn).Release()` in the above example?

```go
if err := handle(conn); err != nil && isClosed(err) {
    conn.(*connpool.Conn).Release()
    // ... 
}
```

Although the connection was dead, it doesn't mean you free its resources. We shouldn't call its `Close` method, because the invalid connection will be likely put into the pool again. By contrast, `Conn.Release` method is better, which can free the underlying connection directly. This strategy avoids that dead connections are repeatedly used.

[connection pool]: https://en.wikipedia.org/wiki/Connection_pool
[Go]: https://golang.org/
[load balancing]: https://en.wikipedia.org/wiki/Load_balancing_(computing)
