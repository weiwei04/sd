package api

import (
	"testing"
	"time"

	"github.com/hashicorp/consul/consul/structs"
	"github.com/hashicorp/consul/testutil"
)

func Test_syncOnce(t *testing.T) {
	stopCh := make(chan struct{})

	balancer := &roundRobin{
		stopCh:   stopCh,
		services: make(map[string]*service),
		client: newConsulClient(RoundRobinConfig{
			Region: "dc1",
			Addrs:  []string{"127.0.0.1:8500"},
		}),
		syncInterval: 1 * time.Second,
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

func Test_RoundRobin(t *testing.T) {
	srv := testutil.NewTestServer(t)
	defer srv.Stop()

	// populate test data
	//for i := 0; i < 10; i++ {
	//id := fmt.Sprintf("service0:%d", i)
	srv.AddService("service0", structs.HealthPassing, []string{"HTTP"})
	//}

	stopCh := make(chan struct{})
	balancer := NewRoundRobin(RoundRobinConfig{
		Region:       "dc1",
		Addrs:        []string{srv.HTTPAddr},
		SyncInterval: 1 * time.Second,
	}, stopCh)

	// wait for first sync
	time.Sleep(3 * time.Second)

	exist := balancer.Exist("service0")
	if !exist {
		t.Errorf("endpoint not found")
	}

	counts := make(map[string]int)
	for i := 0; i < 1000; i++ {
		endpoint, err := balancer.Next("service0")
		if err != nil {
			t.Errorf("endpoint not found")
		} else {
			counts[endpoint.String()] = counts[endpoint.String()] + 1
		}
	}
	close(stopCh)

	if len(counts) != 1 {
		t.Errorf("invalid balance result, expected 10 endpoints, got %d", len(counts))
	}
	for _, v := range counts {
		if v-1000 > 0 {
			t.Errorf("invalid balance result")
		}
	}
}
