package api

import (
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	consul "github.com/hashicorp/consul/api"
)

type RoundRobinConfig struct {
	Region                string
	Addrs                 []string
	DialTimeout           time.Duration
	ResponseHeaderTimeout time.Duration
}

func (c RoundRobinConfig) validate() bool {
	if c.Region == "" ||
		len(c.Addrs) == 0 ||
		c.DialTimeout == 0 ||
		c.ResponseHeaderTimeout == 0 {
		return false
	}
	return true
}

type consulClient struct {
	clients []*consul.Client
	count   uint32
	index   uint32
	region  string
}

func newConsulClient(config RoundRobinConfig) *consulClient {
	if !config.validate() {
		panic(fmt.Sprintf("invalid config"))
	}
	clients := make([]*consul.Client, len(config.Addrs), len(config.Addrs))
	for i, addr := range config.Addrs {
		config := consul.Config{
			Address: addr,
			Scheme:  "http",
			HttpClient: &http.Client{
				Transport: &http.Transport{
					Proxy: http.ProxyFromEnvironment,
					Dial: (&net.Dialer{
						Timeout:   config.DialTimeout,
						KeepAlive: 30 * time.Second,
					}).Dial,
					DisableKeepAlives:     false,
					MaxIdleConnsPerHost:   1,
					ResponseHeaderTimeout: config.ResponseHeaderTimeout,
				},
			},
		}
		clients[i], _ = consul.NewClient(&config)
	}
	return &consulClient{
		clients: clients,
		count:   uint32(len(clients)),
		index:   0, // TODO: random
	}
}

func (c *consulClient) next() *consul.Client {
	index := atomic.AddUint32(&c.index, 1)
	return c.clients[index%c.count]
}

func (c *consulClient) listServices() ([]string, uint64, error) {
	client := c.next()
	services, meta, err :=
		client.Catalog().Services(&consul.QueryOptions{
			Datacenter:        c.region,
			AllowStale:        true,
			RequireConsistent: false,
		})
	if err != nil {
		return []string{}, 0, err
	}
	names := make([]string, 0, len(services))
	for name := range services {
		names = append(names, name)
	}
	return names, meta.LastIndex, nil
}

func (c *consulClient) listServiceEndpoints(name string) ([]Endpoint, uint64, error) {
	client := c.next()
	entries, meta, err :=
		client.Health().Service(name, "HTTP", true, &consul.QueryOptions{
			Datacenter:        c.region,
			AllowStale:        true,
			RequireConsistent: false,
		})
	if err != nil {
		return []Endpoint{}, 0, err
	}
	endpoints := make([]Endpoint, len(entries), len(entries))
	for i, entry := range entries {
		addr := entry.Service.Address
		if addr == "" {
			addr = entry.Node.Address
		}
		endpoints[i] = Endpoint{Addr: addr, Port: uint32(entry.Service.Port)}
	}
	return endpoints, meta.LastIndex, nil
}
