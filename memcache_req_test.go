package mc

import (
	"bufio"
	"reflect"
	"strings"
	"testing"
)

func testReq(in string, t *testing.T) (ret *Request, err error) {
	r := strings.NewReader(in)
	return ReadRequest(bufio.NewReader(r))
}

func TestSet(t *testing.T) {
	ret, err := testReq("set KEY 0 0 10\r\n1234567890\r\n", t)
	if err != nil {
		t.Fatalf("ReadRequest %+v", err)
	}

	if ret.Command != "set" {
		t.Errorf("Command %s", ret.Command)
	}
	if ret.Key != "KEY" {
		t.Errorf("Key %s", ret.Key)
	}
	if ret.Flags != "0" {
		t.Errorf("Flags %s", ret.Flags)
	}
	if ret.Exptime != 0 {
		t.Errorf("Exptime %d", ret.Exptime)
	}
	if string(ret.Data) != "1234567890" {
		t.Errorf("Data %s", ret.Data)
	}
}

func TestGet(t *testing.T) {
	ret, err := testReq("get a bb c\r\n", t)
	if err != nil {
		t.Fatalf("ReadRequest %+v", err)
	}

	if ret.Command != "get" {
		t.Errorf("Command %s", ret.Command)
	}
	if !reflect.DeepEqual(ret.Keys, []string{"a", "bb", "c"}) {
		t.Errorf("Keys %v", ret.Keys)
	}
}

func TestCas(t *testing.T) {
	ret, err := testReq("cas KEY 0 0 10 UNIQ\r\n1234567890\r\n", t)
	if err != nil {
		t.Fatalf("ReadRequest %+v", err)
	}

	if ret.Command != "cas" {
		t.Errorf("Command %s", ret.Command)
	}
	if ret.Key != "KEY" {
		t.Errorf("Key %s", ret.Key)
	}
	if ret.Flags != "0" {
		t.Errorf("Flags %s", ret.Flags)
	}
	if ret.Exptime != 0 {
		t.Errorf("Exptime %d", ret.Exptime)
	}
	if ret.Cas != "UNIQ" {
		t.Errorf("Cas %d", ret.Exptime)
	}
	if string(ret.Data) != "1234567890" {
		t.Errorf("Data %s", ret.Data)
	}
}

func TestError(t *testing.T) {
	_, err := testReq("xxx KEY 0 0 10\r\n1234567890\r\n", t)
	if perr, ok := err.(Error); ok {
		t.Logf("Good error: %v", perr)
		return
	}
	t.Fatalf("ReadRequest did not return error")
}
