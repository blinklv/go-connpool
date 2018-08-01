# go-connpool

[![GoDoc](https://godoc.org/github.com/nsqio/go-nsq?status.svg)](https://godoc.org/github.com/blinklv/go-connpool)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A concurrent safe [connection pool][] package in [Go][]. It can be used to manage and reuse connections based on the destination address of which. This design make a pool work better with some [load balancers][load balancing].

## Install

```bash
$ go get github.com/blinklv/go-connpool
```

[connection pool]: https://en.wikipedia.org/wiki/Connection_pool
[Go]: https://golang.org/
[load balancing]: https://en.wikipedia.org/wiki/Load_balancing_(computing)
