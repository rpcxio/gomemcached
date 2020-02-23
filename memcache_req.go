// Package mc implements memcached text protocol: https://github.com/memcached/memcached/blob/master/doc/protocol.txt.
// binary protocol () has not been implemented.
package mc

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// RealtimeMaxDelta is max delta time.
const RealtimeMaxDelta = 60 * 60 * 24 * 30

// Request is a generic memcached request.
// Some fields are meaningless for some special commands and they are zero values.
// Exptime will always be 0 or epoch (in seconds)
type Request struct {
	// Command is memcached command name, see https://github.com/memcached/memcached/wiki/Commands
	Command string
	Key     string
	Keys    []string
	Flags   string
	Exptime int64 //in second
	Data    []byte
	Value   int64
	Cas     string
	Noreply bool
}

// Error is memcached protocol error.
type Error struct {
	Description string
}

func (e Error) Error() string {
	return fmt.Sprintf("MC Protocol error: %s", e.Description)
}

// NewError creates a new error.
func NewError(description string) Error {
	return Error{description}
}

// ReadRequest reads a request from reader
func ReadRequest(r *bufio.Reader) (req *Request, err error) {
	lineBytes, _, err := r.ReadLine()
	if err != nil {
		return nil, err
	}
	line := string(lineBytes)
	arr := strings.Fields(line)
	if len(arr) < 1 {
		return nil, NewError("empty line")
	}

	switch arr[0] {
	case "set", "add", "replace", "append", "prepend":
		// format:
		// <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
		// <data block>\r\n
		if len(arr) < 5 {
			return nil, NewError(fmt.Sprintf("too few params to command %q", arr[0]))
		}
		req := &Request{}
		req.Command = arr[0]
		req.Key = arr[1]
		req.Flags = arr[2]

		// always use epoch
		req.Exptime, err = strconv.ParseInt(arr[3], 10, 64)
		if err != nil {
			return nil, NewError("cannot read exptime " + err.Error())
		}
		if req.Exptime > 0 {
			if req.Exptime <= RealtimeMaxDelta {
				req.Exptime = time.Now().Unix()/1e9 + req.Exptime
			}
		}
		bytes, err := strconv.Atoi(arr[4])
		if err != nil {
			return nil, NewError("cannot read bytes " + err.Error())
		}
		if len(arr) > 5 && arr[5] == "noreply" {
			req.Noreply = true
		}
		req.Data = make([]byte, bytes)

		n, err := io.ReadFull(r, req.Data)
		if err != nil {
			return nil, err
		}
		if n != bytes {
			return nil, NewError(fmt.Sprintf("Read only %d bytes of %d bytes of expected data", n, bytes))
		}
		c, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if c != '\r' {
			return nil, NewError("expected \\r")
		}
		c, err = r.ReadByte()
		if err != nil {
			return nil, err
		}
		if c != '\n' {
			return nil, NewError("expected \\n")
		}
		return req, nil
	case "cas":
		// format:
		// cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]\r\n
		// <data block>\r\n
		if len(arr) < 6 {
			return nil, NewError(fmt.Sprintf("too few params to command %q", arr[0]))
		}
		req := &Request{}
		req.Command = arr[0]
		req.Key = arr[1]
		req.Flags = arr[2]

		req.Exptime, err = strconv.ParseInt(arr[3], 10, 64)
		if err != nil {
			return nil, NewError("cannot read exptime " + err.Error())
		}
		if req.Exptime > 0 {
			if req.Exptime <= RealtimeMaxDelta {
				req.Exptime = time.Now().Unix()/1e9 + req.Exptime
			}
		}

		bytes, err := strconv.Atoi(arr[4])
		if err != nil {
			return nil, err
		}
		req.Cas = arr[5]
		if len(arr) > 6 && arr[6] == "noreply" {
			req.Noreply = true
		}
		req.Data = make([]byte, bytes)
		n, err := io.ReadFull(r, req.Data)
		if err != nil {
			return nil, err
		}
		if n != bytes {
			return nil, NewError(fmt.Sprintf("Read only %d bytes of %d bytes of expected data", n, bytes))
		}
		c, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if c != '\r' {
			return nil, NewError("expected \\r")
		}
		c, err = r.ReadByte()
		if err != nil {
			return nil, err
		}
		if c != '\n' {
			return nil, NewError("expected \\n")
		}
		return req, nil
	case "delete":
		// format:
		// delete <key> [noreply]\r\n
		if len(arr) < 2 {
			return nil, NewError(fmt.Sprintf("too few params to command %q", arr[0]))
		}
		req := &Request{}
		req.Command = arr[0]
		req.Keys = arr[1:]

		if len(arr) > 2 && arr[2] == "noreply" {
			req.Noreply = true
		}
		return req, nil
	case "get", "gets":
		// format:
		// get <key>*\r\n
		// gets <key>*\r\n
		if len(arr) < 2 {
			return nil, NewError(fmt.Sprintf("too few params to command %q", arr[0]))
		}
		req := &Request{}
		req.Command = arr[0]
		req.Keys = arr[1:]
		return req, nil
	case "incr", "decr":
		// format:
		// incr <key> <value> [noreply]\r\n
		// decr <key> <value> [noreply]\r\n
		if len(arr) < 3 {
			return nil, NewError(fmt.Sprintf("too few params to command %q", arr[0]))
		}
		req := &Request{}
		req.Command = arr[0]
		req.Key = arr[1]

		req.Value, err = strconv.ParseInt(arr[2], 10, 64)
		if err != nil {
			return nil, NewError("cannot read value " + err.Error())
		}

		if len(arr) > 3 && arr[3] == "noreply" {
			req.Noreply = true
		}
		return req, nil
	case "touch":
		// format:
		// touch <key> <exptime> [noreply]\r\n
		if len(arr) < 3 {
			return nil, NewError(fmt.Sprintf("too few params to command %q", arr[0]))
		}
		req := &Request{}
		req.Command = arr[0]
		req.Key = arr[1]

		req.Exptime, err = strconv.ParseInt(arr[2], 10, 64)
		if err != nil {
			return nil, NewError("cannot read exptime " + err.Error())
		}
		if req.Exptime > 0 {
			if req.Exptime <= RealtimeMaxDelta {
				req.Exptime = time.Now().Unix()/1e9 + req.Exptime
			}
		}

		if len(arr) > 3 && arr[3] == "noreply" {
			req.Noreply = true
		}
		return req, nil
	case "flush_all":
		// flush_all [delay]\r\n
		req := &Request{Command: arr[0]}

		if len(arr) > 1 {
			req.Exptime, err = strconv.ParseInt(arr[1], 10, 64)
			if err != nil {
				return nil, NewError("cannot read delay " + err.Error())
			}
		}

		return req, nil
	case "version", "quit":
		// version\r\n
		// quit\r\n
		return &Request{Command: arr[0]}, nil
	case "stats":
		// stats\r\n
		// stats <args>\r\n
		req := &Request{Command: arr[0]}
		if len(arr) > 1 {
			req.Keys = arr[1:]
		}
		return req, nil
	}
	return nil, NewError(fmt.Sprintf("unknown command %q", arr[0]))
}
