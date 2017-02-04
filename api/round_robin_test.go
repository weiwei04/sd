package api

import (
	"testing"
	"time"
)

func Test_syncOnce(t *testing.T) {
	stopCh := make(chan struct{})

	balancer := &roundRobin{
		stopCh:   stopCh,
		services: make(map[string]*service),
		client:   newConsulClient(RoundRobinConfig{}),
	}

	var namesIndex uint64 = 0
	balancer.listServicesFn = func() ([]string, uint64, error) {
		namesIndex++
		return []string{"service0"}, namesIndex, nil
	}
	var endpointsIndex uint64 = 0
	balancer.listServiceEndpointsFn = func(name string) ([]Endpoint, uint64, error) {
		endpointsIndex++
		return []Endpoint{Endpoint{Addr: "10.8.0.1", Port: 5600}, Endpoint{Addr: "10.8.0.2", Port: 5600}},
			endpointsIndex, nil
	}

	exist := balancer.Exist("service0")
	if exist {
		t.Errorf("invalid result, expected[endpoint not found]")
	}

	_, err := balancer.Next("service0")
	if err == nil {
		t.Errorf("invalid result, expected[endpoint not found]")
	}

	go balancer.syncLoop()
	time.Sleep(3 * time.Second)

	exist = balancer.Exist("service0")
	if !exist {
		t.Errorf("endpoint not found")
	}
	endpoint0, err := balancer.Next("service0")
	if err != nil {
		t.Errorf("endpoint not found")
	}
	endpoint1, err := balancer.Next("service0")
	if err != nil {
		t.Errorf("endpoint not found")
	}
	if endpoint0.Addr == endpoint1.Addr {
		t.Errorf("invalid balancer result")
	}

	counts := make(map[string]int)
	for i := 0; i < 100; i++ {
		e, err := balancer.Next("service0")
		if err != nil {
			t.Errorf("endpoint not found")
		}
		counts[e.String()] = counts[e.String()] + 1
	}

	close(stopCh)

	if counts[endpoint0.String()] == 0 {
		t.Errorf("endpoint not found")
	}
	if counts[endpoint1.String()] == 0 {
		t.Errorf("endpoint not found")
	}
	if counts[endpoint0.String()]-counts[endpoint1.String()] > 1 {
		t.Errorf("unexpected balance result")
	}
}
