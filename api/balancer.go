package api

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrNotFound = errors.New("endpoints not found")
)

type Endpoint struct {
	Addr string
	Port uint32
}

func (e Endpoint) String() string {
	return fmt.Sprintf("%s:%d", e.Addr, e.Port)
}

type Balancer interface {
	Exist(service string) bool
	Next(service string) (Endpoint, error)
}

type sortable []Endpoint

func (s sortable) Less(i, j int) bool {
	less := strings.Compare(s[i].Addr, s[j].Addr)
	if less < 0 {
		return true
	} else if less == 0 {
		return s[i].Port < s[j].Port
	}
	return false
}

func (s sortable) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortable) Len() int {
	return len(s)
}
