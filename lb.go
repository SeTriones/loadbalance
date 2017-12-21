package loadbalance

import (
	"errors"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
)

var (
	ErrEmpty   = errors.New("not backend addresses")
	ErrAllDown = errors.New("all backend servers down")
)

type Updater interface {
	// update backend server addresses
	Update() ([]string, error)
}

type BackendServer struct {
	Addr     string
	FailCnt  int
	Down     bool
	DownTime time.Time
}

type LoadBalancer struct {
	mu             sync.Mutex
	serverList     []*BackendServer
	serverIndex    map[string]*BackendServer
	next           int
	cnt            int
	lastUpdate     time.Time
	updateInterval time.Duration
	updater        Updater
	updating       bool
	failThreshold  int
	activeCnt      int
}

func NewLoadBalancer(u Updater, interval time.Duration, maxErr int) *LoadBalancer {
	addrs, err := u.Update()
	if err != nil {
		log.Errorf("init backend addresses fail")
		return nil
	}
	l := len(addrs)
	index := make(map[string]*BackendServer)
	servers := make([]*BackendServer, l, l)
	for i := 0; i < l; i++ {
		s := &BackendServer{
			Addr:    addrs[i],
			FailCnt: 0,
			Down:    false,
		}
		servers[i] = s
		index[addrs[i]] = s
	}
	lb := &LoadBalancer{
		mu:             sync.Mutex{},
		serverList:     servers,
		serverIndex:    index,
		next:           0,
		cnt:            l,
		lastUpdate:     time.Now(),
		updateInterval: interval,
		updater:        u,
		updating:       false,
		failThreshold:  maxErr,
		activeCnt:      l,
	}
	return lb
}

func (lb *LoadBalancer) Get() (string, error) {
	t := time.Now()
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if t.Sub(lb.lastUpdate) > lb.updateInterval && !lb.updating {
		lb.updating = true
		go func() {
			addrs, err := lb.updater.Update()
			if err != nil {
				log.Errorf("update fail,err=%v", err)
				lb.updating = false
			} else {
				lb.Set(addrs)
			}
			lb.lastUpdate = time.Now()
			lb.updating = false
		}()
	}
	if lb.cnt == 0 {
		return "", ErrEmpty
	}
	if lb.activeCnt == 0 {
		return "", ErrAllDown
	}
	if lb.next >= lb.cnt {
		lb.next = 0
	}
	start := lb.next
	for {
		s := lb.serverList[lb.next]
		lb.next = (lb.next + 1) % lb.cnt
		if lb.next == start && lb.cnt > 1 {
			return s.Addr, errors.New("no active server")
		}
		if !s.Down {
			return s.Addr, nil
		} else {
			if t.Sub(s.DownTime) > lb.updateInterval*time.Duration(10) {
				log.Infof("%s is mark as active", s.Addr)
				s.Down = false
			}
			return s.Addr, nil
		}
	}
}

func (lb *LoadBalancer) Set(addrs []string) {
	if len(addrs) == 0 {
		log.Errorf("do not set zero length addrs")
	}
	m := make(map[string]int)
	for _, addr := range addrs {
		m[addr] = 1
	}
	lb.mu.Lock()
	defer lb.mu.Unlock()
	newList := []*BackendServer{}
	for _, server := range lb.serverList {
		_, ok := m[server.Addr]
		if ok {
			newList = append(newList, server)
			delete(m, server.Addr)
		} else {
			delete(lb.serverIndex, server.Addr)
		}
	}
	for k, _ := range m {
		s := &BackendServer{
			Addr:    k,
			FailCnt: 0,
			Down:    false,
		}
		newList = append(newList, s)
		lb.serverIndex[k] = s
	}
	lb.cnt = len(newList)
	lb.serverList = newList
	if lb.next >= lb.cnt {
		lb.next = 0
	}
}

func (lb *LoadBalancer) MarkFail(addr string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	s, ok := lb.serverIndex[addr]
	if ok {
		s.FailCnt = s.FailCnt + 1
		if s.FailCnt >= lb.failThreshold {
			log.Infof("%s is marked as down", addr)
			s.Down = true
			s.DownTime = time.Now()
			s.FailCnt = 0
			lb.activeCnt = lb.activeCnt - 1
		}
	}
}
