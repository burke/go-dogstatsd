// Copyright 2013 Ooyala, Inc.

package dogstatsd

import (
	"net"
	"reflect"
	"testing"
	"time"
)

var dogstatsdTests = []struct {
	GlobalNamespace string
	GlobalTags      []string
	Method          string
	Metric          string
	Value           interface{}
	Tags            []string
	Rate            float64
	Expected        string
}{
	{"", nil, "Gauge", "test.gauge", 1.0, nil, 1.0, "test.gauge:1|g"},
	{"", nil, "Gauge", "test.gauge", 1.0, nil, 0.999999, "test.gauge:1|g|@0.999999"},
	{"", nil, "Gauge", "test.gauge", 1.0, []string{"tagA"}, 1.0, "test.gauge:1|g|#tagA"},
	{"", nil, "Gauge", "test.gauge", 1.0, []string{"tagA", "tagB"}, 1.0, "test.gauge:1|g|#tagA,tagB"},
	{"", nil, "Gauge", "test.gauge", 1.0, []string{"tagA"}, 0.999999, "test.gauge:1|g|@0.999999|#tagA"},
	{"", nil, "Count", "test.count", int64(1), []string{"tagA"}, 1.0, "test.count:1|c|#tagA"},
	{"", nil, "Count", "test.count", int64(-1), []string{"tagA"}, 1.0, "test.count:-1|c|#tagA"},
	{"", nil, "Histogram", "test.histogram", 2.3, []string{"tagA"}, 1.0, "test.histogram:2.3|h|#tagA"},
	{"", nil, "Set", "test.set", "uuid", []string{"tagA"}, 1.0, "test.set:uuid|s|#tagA"},
	{"", nil, "Timer", "test.timer", 44876 * time.Microsecond, []string{"tagA"}, 1.0, "test.timer:44.876|ms|#tagA"},
	{"flubber.", nil, "Set", "test.set", "uuid", []string{"tagA"}, 1.0, "flubber.test.set:uuid|s|#tagA"},
	{"", []string{"tagC"}, "Set", "test.set", "uuid", []string{"tagA"}, 1.0, "test.set:uuid|s|#tagC,tagA"},
}

func TestClient(t *testing.T) {
	addr := "localhost:1201"
	server := newServer(t, addr)
	defer server.Close()

	client, err := New(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	for _, tt := range dogstatsdTests {
		client.SetNamespace(tt.GlobalNamespace)
		client.SetTags(tt.GlobalTags)

		method := reflect.ValueOf(client).MethodByName(tt.Method)
		e := method.Call([]reflect.Value{
			reflect.ValueOf(tt.Metric),
			reflect.ValueOf(tt.Value),
			reflect.ValueOf(tt.Tags),
			reflect.ValueOf(tt.Rate)})[0]
		errInter := e.Interface()
		if errInter != nil {
			t.Fatal(errInter.(error))
		}

		message := serverRead(t, server)
		if message != tt.Expected {
			t.Errorf("Expected: %s. Actual: %s", tt.Expected, message)
		}
	}

}

func TestEvent(t *testing.T) {
	addr := "localhost:1201"
	server := newServer(t, addr)
	defer server.Close()
	client := newClient(t, addr)

	err := client.Event("title", "text", []string{"tag1", "tag2"})
	if err != nil {
		t.Fatal(err)
	}

	message := serverRead(t, server)
	expected := "_e{5,4}:title|text|#tag1,tag2"
	if message != expected {
		t.Errorf("Expected: %s. Actual: %s", expected, message)
	}
}

func serverRead(t *testing.T, server *net.UDPConn) string {
	bytes := make([]byte, 1024)
	n, _, err := server.ReadFrom(bytes)
	if err != nil {
		t.Fatal(err)
	}
	return string(bytes[:n])
}

func newClient(t *testing.T, addr string) *Client {
	client, err := New(addr)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func newServer(t *testing.T, addr string) *net.UDPConn {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		t.Fatal(err)
	}

	server, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatal(err)
	}
	return server
}
