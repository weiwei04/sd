package api

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func NewRoundRobin(config RoundRobinConfig, stopCh <-chan struct{}) Balancer {
	err := config.normalize()
	if err != nil {
		panic(fmt.Sprintf("invalid config"))
	}
	balancer := &roundRobin{
		stopCh:       stopCh,
		services:     make(map[string]*service),
		client:       newConsulClient(config),
		syncInterval: config.SyncInterval,
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

//type set map[string]struct{}

type roundRobin struct {
	stopCh                 <-chan struct{}
	listServicesFn         func() (map[string]struct{}, uint64, error)
	listServiceEndpointsFn func(service string) ([]Endpoint, uint64, error)

	client *consulClient

	syncInterval time.Duration

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

func (r *roundRobin) syncOnce(lastNames map[string]struct{}, lastIndex uint64) (map[string]struct{}, uint64) {
	newNames, newIndex, err := r.listServicesFn()
	if err != nil {
		return lastNames, lastIndex
	}

	changed := false
	if lastIndex < newIndex {
		changed = true
	} else {
		newNames = lastNames
	}

	for name := range newNames {
		endpoints, eIndex, err := r.listServiceEndpointsFn(name)
		if err != nil {
			continue
		}
		if len(endpoints) == 0 {
			// TODO: if len(endpoints) == 0 queue a async listServiceEndpoints call
			continue
		}
		old, ok := r.services[name]
		if !ok {
			// TODO: random a index
			old = &service{}
		}
		if eIndex <= old.lastIndex {
			continue
		}
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
		return newNames, newIndex
	}

	for name := range r.services {
		_, ok := newNames[name]
		if !ok {
			r.mutex.Lock()
			delete(r.services, name)
			r.mutex.Unlock()
		}
	}

	return newNames, newIndex
}

func (r *roundRobin) syncLoop() {
	var lastIndex uint64
	lastNames := make(map[string]struct{})

	ticker := time.NewTicker(r.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			break
		}
		lastNames, lastIndex = r.syncOnce(lastNames, lastIndex)
	}
}
