# gomemcached
Memcached protocol implementation for memcached server.
You can use it to create a memcached server easily.

[![License: MIT](https://img.shields.io/badge/License-MIT-brightgreen.svg)](https://opensource.org/licenses/MIT) [![GoDoc](https://godoc.org/github.com/rpcxio/gomemcached?status.png)](http://godoc.org/github.com/rpcxio/gomemcached)  [![travis](https://travis-ci.org/smallnest/gomemcached.svg?branch=master)](https://travis-ci.org/smallnest/gomemcached) [![Go Report Card](https://goreportcard.com/badge/github.com/rpcxio/gomemcached)](https://goreportcard.com/report/github.com/rpcxio/gomemcached) [![Coverage Status](https://coveralls.io/repos/smallnest/gomemcached/badge.svg?branch=master&service=github)](https://coveralls.io/github/smallnest/gomemcached?branch=master) 


import this lib:

```sh
go get -u github.com/rpcxio/gomemcached
```


And register handlers.

```go
    addr = "127.0.0.1:" + strconv.Itoa(port)
	mockServer = NewServer(addr)
	mockServer.RegisterFunc("get", DefaultGet)
	mockServer.RegisterFunc("gets", DefaultGet)
	mockServer.RegisterFunc("set", DefaultSet)
	mockServer.RegisterFunc("delete", DefaultDelete)
	mockServer.RegisterFunc("incr", DefaultIncr)
	mockServer.RegisterFunc("flush_all", DefaultFlushAll)
	mockServer.RegisterFunc("version", DefaultVersion)
	mockServer.Start()
```


This project refers to the below projects:

- [luxuan/go-memcached-server](https://github.com/luxuan/go-memcached-server)
- [zobo/mrproxy](https://github.com/zobo/mrproxy)

I added more implementation and fix some issues, for example, panic on reading long value. I also add `Context` in handlers so that we can pass more info to handlers.