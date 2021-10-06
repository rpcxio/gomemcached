package mc

import (
	"context"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

var (
	mockServer *Server
	addr       string
)

func startMockServer(t *testing.T) {
	port, err := getFreePort()
	if err != nil {
		t.Fatalf("failed to get a free port: %v", err)
	}

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
}

func stopMockServer() {
	mockServer.Stop()
}

func TestMemcached(t *testing.T) {
	startMockServer(t)
	time.Sleep(time.Second)
	defer stopMockServer()

	mc := memcache.New(addr)
	mc.Set(&memcache.Item{Key: "foo", Value: []byte("my value")})

	it, err := mc.Get("foo")
	if err != nil {
		t.Errorf("failed to get: %v", err)
	}

	if it == nil || string(it.Value) != "my value" {
		t.Errorf("failed to get wrong value: %s", string(it.Value))
	}

	var num uint64 = 1234
	s := strconv.FormatUint(num, 10)
	mc.Set(&memcache.Item{Key: "num", Value: []byte(s)})
	num, err = mc.Increment("num", uint64(100))
	if err != nil {
		t.Errorf("failed to increment: %v", err)
	}

	if num != 1334 {
		t.Errorf("wrong increment implementation. got: %d", num)
	}

	err = mc.Delete("num")
	if err != nil {
		t.Errorf("failed to delete: %v", err)
	}

	err = mc.FlushAll()
	if err != nil {
		t.Errorf("failed to flush_all: %v", err)
	}
}

func getFreePort() (port int, err error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()

	addr := listener.Addr().String()
	_, portString, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(portString)
}

// mock
var memStore sync.Map

func DefaultGet(ctx context.Context, req *Request, res *Response) error {
	for _, key := range req.Keys {
		value, _ := memStore.Load(key)
		res.Values = append(res.Values, Value{key, "0", value.([]byte), ""})
	}

	res.Response = RespEnd
	return nil
}

func DefaultSet(ctx context.Context, req *Request, res *Response) error {
	key := req.Key
	value := req.Data
	memStore.Store(key, value)

	res.Response = RespStored
	return nil
}

func DefaultDelete(ctx context.Context, req *Request, res *Response) error {
	count := 0
	for _, key := range req.Keys {
		if _, exists := memStore.Load(key); exists {
			memStore.Delete(key)
			count++
		}
	}
	if count > 0 {
		res.Response = RespDeleted
	} else {
		res.Response = RespNotFound
	}
	return nil
}

func DefaultIncr(ctx context.Context, req *Request, res *Response) error {
	key := req.Key
	increment := req.Value
	var base uint64
	if value, exists := memStore.Load(key); exists {
		var err error
		base, err = strconv.ParseUint(string(value.([]byte)), 10, 64)
		if err != nil {
			return err
		}
	}

	value := strconv.FormatUint(base+increment, 10)
	memStore.Store(key, []byte(value))

	res.Response = value
	return nil
}

func DefaultFlushAll(ctx context.Context, req *Request, res *Response) error {
	memStore = sync.Map{}
	res.Response = RespOK
	return nil
}

func DefaultVersion(ctx context.Context, req *Request, res *Response) error {
	res.Response = "VERSION 1"
	return nil
}
