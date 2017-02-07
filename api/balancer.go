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

type Logger interface {
	Debugf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Warningf(format string, v ...interface{})
	Errorf(format string, v ...interface{})
	Fatalf(format string, v ...interface{})
}

type logger struct{}

func (l logger) printf(level string, format string, v ...interface{}) {
	log := fmt.Sprintf(format, v...)
	fmt.Printf("[%s] %s\n", level, log)
}

func (l logger) Debugf(format string, v ...interface{}) {
	l.printf("DEBUG", format, v...)
}

func (l logger) Infof(format string, v ...interface{}) {
	l.printf("INFO", format, v...)
}

func (l logger) Warningf(format string, v ...interface{}) {
	l.printf("WARN", format, v...)
}

func (l logger) Errorf(format string, v ...interface{}) {
	l.printf("ERR", format, v...)
}

func (l logger) Fatalf(format string, v ...interface{}) {
	panic(fmt.Sprintf(format, v...))
}

type Balancer interface {
	Exist(service string) bool
	Next(service string) (Endpoint, error)
	Start()
	Stop()
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
