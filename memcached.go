package mc

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"net/url"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// ReaderBuffsize is used for bufio reader.
	ReaderBuffsize = 16 * 1024
	// WriterBuffsize is used for bufio writer.
	WriterBuffsize = 16 * 1024
)

var (
	RespOK        = "OK"
	RespEnd       = "END"
	RespStored    = "STORED"
	RespNotStored = "NOT_STORED"
	RespExists    = "EXISTS"
	RespDeleted   = "DELETED"
	RespTouched   = "TOUCHED"
	RespNotFound  = "NOT_FOUND"
	RespErr       = "ERROR "
	RespClientErr = "CLIENT_ERROR "
	RespServerErr = "SERVER_ERROR "
)

// RemoteConnKey is used as key in context.
type RemoteConnKey struct{}

// HandlerFunc is a function to handle a request and returns a response.
type HandlerFunc func(ctx context.Context, req *Request, res *Response) error

// Server implements memcached server.
type Server struct {
	addr    string
	ln      net.Listener
	methods map[string]HandlerFunc // should init this map before working
	clients sync.Map

	stopped int32
}

// NewServer creates a memcached server.
func NewServer(addr string) *Server {
	return &Server{
		addr:    addr,
		methods: make(map[string]HandlerFunc),
	}
}

// Start starts the memcached server in a goroutine.
// It listens on the TCP/unix network address s.Addr and then calls Serve to handle
// requests on incoming connections. Accepted connections are configured to enable
// TCP keep-alives when they are TCP network connections.
func (s *Server) Start() error {
	var err error

	if strings.Contains(s.addr, "://") {
		var u *url.URL
		u, err = url.Parse(s.addr)
		if err != nil {
			return err
		}

		switch u.Scheme {
		case "unix":
			s.ln, err = net.Listen("unix", u.Path)
		default:
			s.ln, err = net.Listen("tcp", u.Host)
		}
	} else {
		s.ln, err = net.Listen("tcp", s.addr)
	}

	if err != nil {
		return err
	}

	log.Printf("memcached server starts on %s", s.addr)
	go s.Serve(s.ln)
	return nil
}

// Serve accepts incoming connections on the Listener ln, creating a new service goroutine for each.
// The service goroutines read requests and then call registered handlers to reply to them.
func (s *Server) Serve(ln net.Listener) error {
	defer ln.Close()

	var tempDelay time.Duration // how long to sleep on accept failure
	for {
		conn, err := ln.Accept()
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
				log.Printf("accept error: %v; retrying in %v", err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			log.Printf("memcached server accept error: %v", err)
			return err
		}
		tempDelay = 0

		if atomic.LoadInt32(&s.stopped) != 0 {
			conn.Close()
			return nil
		}

		if tc, ok := conn.(*net.TCPConn); ok {
			tc.SetNoDelay(true)
			tc.SetKeepAlive(true)
		}

		s.clients.Store(conn, struct{}{})

		go s.handleConn(conn)
	}
}

// RegisterFunc registers a handler to handle this command.
func (s *Server) RegisterFunc(cmd string, fn HandlerFunc) error {
	s.methods[cmd] = fn
	return nil
}

func (s *Server) handleConn(conn net.Conn) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("memcached server panic error: %s, stack: %s", err, string(debug.Stack()))
		}
		s.clients.Delete(conn)
		conn.Close()
	}()

	r := bufio.NewReaderSize(conn, ReaderBuffsize)
	w := bufio.NewWriterSize(conn, WriterBuffsize)

	ctx := context.Background()
	ctx = context.WithValue(ctx, RemoteConnKey{}, conn)

	for atomic.LoadInt32(&s.stopped) == 0 {
		req, err := ReadRequest(r)
		if perr, ok := err.(Error); ok {
			log.Printf("%v ReadRequest protocol err: %v", conn, err)
			w.WriteString(RespClientErr + perr.Error() + "\r\n")
			w.Flush()
			continue
		} else if err != nil {
			log.Printf("ReadRequest from %s err: %v", conn.RemoteAddr().String(), err)
			return
		}

		cmd := req.Command
		if cmd == "quit" {
			log.Printf("client send quit, closed")
			return
		}

		res := &Response{}
		fn, exists := s.methods[cmd]
		if exists {
			err := fn(ctx, req, res)
			if err != nil {
				log.Printf("ERROR: %v, Conn: %v, Req: %+v\n", err, conn, req)
				res.Response = RespServerErr + err.Error()
			}
			if !req.Noreply {
				w.WriteString(res.String())
				w.Flush()
			}
		} else {
			res.Response = RespErr + cmd + " not implemented'"
			w.WriteString(res.String())
			w.Flush()
		}
	}
}

// Stop stops this memcached sever.
func (s *Server) Stop() error {
	var err error
	if !atomic.CompareAndSwapInt32(&s.stopped, 0, 1) {
		return nil
	}

	if s.ln == nil {
		fmt.Println("memcached server has not started")
		return nil
	}

	if err = s.ln.Close(); err != nil {
		fmt.Printf("failed to close listener: %v", err)
	}

	//Make on processing commamd to run over
	time.Sleep(200 * time.Millisecond)

	s.drainConn()

	// for s.count() != 0 {
	// 	time.Sleep(time.Millisecond)
	// }

	checkStart := time.Now()
	for {
		found := false
		s.clients.Range(func(k, v interface{}) bool {
			found = true
			return false
		})
		if found {
			time.Sleep(10 * time.Millisecond)
		}
		// wait at most 1 second
		if time.Since(checkStart).Seconds() > 1 {
			break
		}
	}

	fmt.Println("memcached server stop")
	return err
}

// close connection of clients.
func (s *Server) drainConn() {
	s.clients.Range(func(k, v interface{}) bool {
		k.(net.Conn).Close()
		return true
	})
}
