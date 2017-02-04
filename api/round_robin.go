package api

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func NewRoundRobin(config RoundRobinConfig, stopCh <-chan struct{}) Balancer {
	balancer := &roundRobin{
		stopCh:   stopCh,
		services: make(map[string]*service),
		client:   newConsulClient(config),
	}

	balancer.listServicesFn = balancer.client.listServices
	balancer.listServiceEndpointsFn = balancer.client.listServiceEndpoints

	go balancer.syncLoop()
	return balancer
}

type service struct {
	endpoints []Endpoint
	index     uint32
	lastIndex uint64
}

//func (s *service) Next() (Endpoint, error) {
//	if len(s.endpoints) == 0 {
//		return Endpoint{}, ErrNotFound
//	}
//	idx := atomic.AddUint32(&s.idx, 1) % uint32(len(s.endpoints))
//	return s.endpoints[s.idx], nil
//}

type roundRobin struct {
	stopCh                 <-chan struct{}
	listServicesFn         func() ([]string, uint64, error)
	listServiceEndpointsFn func(service string) ([]Endpoint, uint64, error)

	client *consulClient

	services map[string]*service
	mutex    sync.RWMutex
}

func (r *roundRobin) Exist(name string) bool {
	r.mutex.RLock()
	_, ok := r.services[name]
	r.mutex.RUnlock()
	return ok
}

func (r *roundRobin) Next(name string) (Endpoint, error) {
	r.mutex.RLock()
	service, ok := r.services[name]
	r.mutex.RUnlock()
	if !ok || len(service.endpoints) == 0 {
		return Endpoint{}, ErrNotFound
	}
	index := atomic.AddUint32(&service.index, 1) % uint32(len(service.endpoints))
	return service.endpoints[index], nil
}

func (r *roundRobin) syncOnce(lastIndex uint64) uint64 {
	names, newIndex, err := r.listServicesFn()
	if err != nil {
		return lastIndex
	}

	changed := false
	if lastIndex != newIndex {
		changed = true
	}

	services := make(map[string]struct{})
	for _, name := range names {
		if changed {
			services[name] = struct{}{}
		}
		endpoints, eIndex, err := r.listServiceEndpointsFn(name)
		if err != nil {
			continue
		}
		old, ok := r.services[name]
		if !ok {
			// TODO: random a index
			old = &service{}
		}
		if eIndex == old.lastIndex {
			continue
		}
		// TODO: if len(endpoints) == 0 retry asap
		sort.Sort(sortable(endpoints))
		index := atomic.LoadUint32(&old.index)
		new := &service{
			endpoints: endpoints,
			index:     index,
			lastIndex: eIndex,
		}
		r.mutex.Lock()
		r.services[name] = new
		r.mutex.Unlock()
	}

	if !changed {
		return newIndex
	}

	for name := range r.services {
		_, ok := services[name]
		if !ok {
			r.mutex.Lock()
			delete(r.services, name)
			r.mutex.Unlock()
		}
	}

	return newIndex
}

func (r *roundRobin) syncLoop() {
	var lastIndex uint64

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			break
		}
		lastIndex = r.syncOnce(lastIndex)
	}
}
