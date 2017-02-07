package api

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func NewRoundRobin(config RoundRobinConfig) Balancer {
	err := config.normalize()
	if err != nil {
		panic(fmt.Sprintf("invalid config"))
	}
	balancer := &roundRobin{
		stopCh:       make(chan struct{}),
		services:     make(map[string]*service),
		client:       newConsulClient(config),
		syncInterval: config.SyncInterval,
		logger:       config.Logger,
	}

	balancer.listServicesFn = balancer.client.listServices
	balancer.listServiceEndpointsFn = balancer.client.listServiceEndpoints

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
	stopCh chan struct{}
	wg     sync.WaitGroup

	listServicesFn         func() (map[string]struct{}, uint64, error)
	listServiceEndpointsFn func(service string) ([]Endpoint, uint64, error)

	client *consulClient

	syncInterval time.Duration

	services map[string]*service
	mutex    sync.RWMutex

	logger Logger
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

func (r *roundRobin) Start() {
	r.wg.Add(1)
	go r.syncLoop()
	r.logger.Infof("RoundRobin Balancer Background Sync Started, SyncInterval[%s]", r.syncInterval.String())
}

func (r *roundRobin) Stop() {
	close(r.stopCh)
	r.wg.Wait()
	r.logger.Infof("RoundRobin Balancer Background Sync Stopped")
}

func (r *roundRobin) syncOnce(lastNames map[string]struct{}, lastIndex uint64) (map[string]struct{}, uint64) {
	newNames, newIndex, err := r.listServicesFn()
	if err != nil {
		r.logger.Warningf("RoundRobin Balancer ListServices failed, err[%s]", err.Error())
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
			r.logger.Warningf("RoundRobin Balancer ListServiceEndpoints(%s) failed, err[%s]", name, err.Error())
			continue
		}
		if len(endpoints) == 0 {
			r.logger.Debugf("RoundRobin Balancer ignore empty endpoints set")
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
	defer r.wg.Done()

	var lastIndex uint64
	lastNames := make(map[string]struct{})

	ticker := time.NewTicker(r.syncInterval)
	defer ticker.Stop()

	var current time.Time
	for {
		select {
		case <-r.stopCh:
			return
		case current = <-ticker.C:
			break
		}
		lastNames, lastIndex = r.syncOnce(lastNames, lastIndex)
		r.logger.Debugf("RoundRobin Balancer Background Sync at %s, index[%d]", current.Format(time.UnixDate), lastIndex)
	}
}
