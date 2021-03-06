// Copyright 2013 Ooyala, Inc.

/*
Package dogstatsd provides a Go DogStatsD client. DogStatsD extends StatsD - adding tags and
histograms. Refer to http://docs.datadoghq.com/guides/dogstatsd/ for information about DogStatsD.

Example Usage:
		// Create the client
		c, err := dogstatsd.New("127.0.0.1:8125")
		defer c.Close()
		if err != nil {
			log.Fatal(err)
		}
		// Prefix every metric with the app name
		c.Namespace = "flubber."
		// Send the EC2 availability zone as a tag with every metric
		append(c.Tags, "us-east-1a")
		err = c.Gauge("request.duration", 1.2, nil, 1)

dogstatsd is based on go-statsd-client.
*/
package dogstatsd

import (
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"strings"
)

// Client holds onto a connection and the other context necessary for every stasd packet.
type Client struct {
	conn net.Conn
	// Namespace to prepend to all statsd calls
	Namespace string
	// Global tags to be added to every statsd call
	Tags []string
}

// New returns a pointer to a new Client and an error.
// addr must have the format "hostname:port"
func New(addr string) (*Client, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	client := &Client{conn: conn}
	return client, nil
}

// Close closes the connection to the DogStatsD agent
func (c *Client) Close() error {
	return c.conn.Close()
}

// send handles sampling and sends the message over UDP. It also adds global namespace prefixes and tags.
func (c *Client) send(name string, value string, tags []string, rate float64) error {
	if rate < 1 {
		if rand.Float64() < rate {
			value = fmt.Sprintf("%s|@%f", value, rate)
		} else {
			return nil
		}
	}

	if c.Namespace != "" {
		name = fmt.Sprintf("%s%s", c.Namespace, name)
	}

	tags = append(c.Tags, tags...)
	if len(tags) > 0 {
		value = fmt.Sprintf("%s|#%s", value, strings.Join(tags, ","))
	}

	data := fmt.Sprintf("%s:%s", name, value)
	_, err := c.conn.Write([]byte(data))
	return err
}

// Event posts to the Datadog event stream.
func (c *Client) Event(title string, text string, tags []string) error {
	var b bytes.Buffer

	fmt.Fprintf(&b, "_e{%d,%d}:%s|%s", len(title), len(text), title, text)
	tags = append(c.Tags, tags...)
	format := "|#%s"
	for _, t := range tags {
		fmt.Fprintf(&b, format, t)
		format = ",%s"
	}

	_, err := c.conn.Write(b.Bytes())
	return err
}

// Gauge measures the value of a metric at a particular time
func (c *Client) Gauge(name string, value float64, tags []string, rate float64) error {
	stat := fmt.Sprintf("%f|g", value)
	return c.send(name, stat, tags, rate)
}

// Count tracks how many times something happened per second
func (c *Client) Count(name string, value int64, tags []string, rate float64) error {
	stat := fmt.Sprintf("%d|c", value)
	return c.send(name, stat, tags, rate)
}

// Histogram tracks the statistical distribution of a set of values
func (c *Client) Histogram(name string, value float64, tags []string, rate float64) error {
	stat := fmt.Sprintf("%f|h", value)
	return c.send(name, stat, tags, rate)
}

// Timer tracks the statistical distribution of a set of durations
func (c *Client) Timer(name string, value float64, tags []string, rate float64) error {
	stat := fmt.Sprintf("%f|ms", value)
	return c.send(name, stat, tags, rate)
}

// Set counts the number of unique elements in a group
func (c *Client) Set(name string, value string, tags []string, rate float64) error {
	stat := fmt.Sprintf("%s|s", value)
	return c.send(name, stat, tags, rate)
}
