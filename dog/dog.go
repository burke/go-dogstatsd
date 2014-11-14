package dog

import "github.com/Shopify/go-dogstatsd"

var Client *dogstatsd.Client

// Configure instantiates the global client.
func Configure(addr string, namespace string, tags []string) (err error) {
	if Client, err = dogstatsd.New(addr); err != nil {
		return
	}
	Client.Namespace = namespace
	Client.Tags = tags
	return
}

func Event(title string, text string, tags []string) error {
	return Client.Event(title, text, tags)
}
func Gauge(name string, value float64, tags []string, rate float64) error {
	return Client.Gauge(name, value, tags, rate)
}
func Count(name string, value int64, tags []string, rate float64) error {
	return Client.Count(name, value, tags, rate)
}
func Histogram(name string, value float64, tags []string, rate float64) error {
	return Client.Histogram(name, value, tags, rate)
}
func Timer(name string, value float64, tags []string, rate float64) error {
	return Client.Timer(name, value, tags, rate)
}
func Set(name string, value string, tags []string, rate float64) error {
	return Client.Set(name, value, tags, rate)
}
