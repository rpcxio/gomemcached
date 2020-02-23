package mc

import (
	"bytes"
	"strconv"
)

// Response is a memcached response.
type Response struct {
	Response string
	Values   []Value
}

// Value is data in responses.
type Value struct {
	Key, Flags string
	//Exptime time.Time
	Data []byte
	Cas  string
}

// String converts Response to string to send over wire.
func (r Response) String() string {
	// format:
	// VALUE <key> <flags> <bytes> [<cas unique>]\r\n
	//<data block>\r\n

	var b bytes.Buffer

	for i := range r.Values {
		//b.WriteString(fmt.Sprintf("VALUE %s %s %d\r\n", r.Values[i].Key, r.Values[i].Flags, len(r.Values[i].Data)))
		b.WriteString("VALUE ")
		b.WriteString(r.Values[i].Key)
		b.WriteString(" ")
		b.WriteString(r.Values[i].Flags)
		b.WriteString(" ")
		b.WriteString(strconv.Itoa(len(r.Values[i].Data)))

		if r.Values[i].Cas != "" {
			b.WriteString(" ")
			b.WriteString(r.Values[i].Cas)
		}

		b.WriteString("\r\n")

		b.Write(r.Values[i].Data)
		b.WriteString("\r\n")
	}

	b.WriteString(r.Response)
	b.WriteString("\r\n")

	return b.String()
}
