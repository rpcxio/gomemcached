package mc

import (
	"testing"
)

func TestRespEmpty(t *testing.T) {
	var res Response

	r := res.String()

	if r != "\r\n" {
		t.Errorf("Empty response is not empty: %v", r)
	}
}

func TestRespEnd(t *testing.T) {
	res := Response{Response: "END"}
	r := res.String()

	if r != "END\r\n" {
		t.Errorf("%v", r)
	}
}

func TestRespValueEnd(t *testing.T) {
	res := Response{
		"END",
		[]Value{
			Value{"k1", "f1", []byte("123"), ""},
		},
	}
	r := res.String()

	if r != "VALUE k1 f1 3\r\n123\r\nEND\r\n" {
		t.Errorf("%v", r)
	}
}

func TestRespMultipleValue(t *testing.T) {
	res := Response{
		"END",
		[]Value{
			Value{"k1", "f1", []byte("123"), ""},
			Value{"k2", "f2", []byte("456"), ""},
		},
	}
	r := res.String()

	if r != "VALUE k1 f1 3\r\n123\r\nVALUE k2 f2 3\r\n456\r\nEND\r\n" {
		t.Errorf("%v", r)
	}
}
