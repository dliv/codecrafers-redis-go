package storage

import (
	"fmt"
	"sync"
	"time"
)

type StorageVal struct {
	Payload string
	Exp     int64
}

type Storage struct {
	mu    sync.RWMutex
	Items map[string]StorageVal
}

func NewStorage() *Storage {
	return &Storage{
		Items: make(map[string]StorageVal),
	}
}

func (s *Storage) Get(key string) (StorageVal, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.Items[key]
	fmt.Println("Get key: ", key, ", val: ", val.Payload, ", exp: ", val.Exp, ", ok: ", ok)
	if !ok {
		fmt.Println("missing")
		return StorageVal{}, false
	}
	if val.Exp == int64(0) {
		fmt.Println("no exp")
		return val, true
	}
	now := time.Now().Unix()
	fmt.Println("now: ", now)
	if now > val.Exp {
		fmt.Println("expired")
		delete(s.Items, key)
		return StorageVal{}, false
	}
	fmt.Println("current")
	return val, true
}

func (s *Storage) Set(key, payload string, expiresIn int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	exp := int64(0)
	now := time.Now().Unix()
	fmt.Println("expiresIn: ", expiresIn)
	fmt.Println("now: ", now)
	if expiresIn > int64(0) {
		exp = now + expiresIn
	}
	fmt.Println("Set key: ", key, ", payload: ", payload, ", exp: ", exp)
	s.Items[key] = StorageVal{
		payload,
		exp,
	}
}

func (s *Storage) Del(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.Items, key)
}
