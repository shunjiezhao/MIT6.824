package kvraft

import "errors"

type store struct {
	store map[string]string
}

func newStore() *store {
	return &store{
		map[string]string{},
	}
}
func (s store) Get(key string) (string, error) {
	value, ok := s.store[key]
	if !ok {
		return "", errors.New(ErrNoKey)
	}
	return value, nil
}

func (s store) Put(key string, value string) error {
	s.store[key] = value
	return nil
}

func (s store) Append(key string, value string) error {
	if _, ok := s.store[key]; !ok {
		s.store[key] = ""
	}
	s.store[key] += value
	return nil
}

type Store interface {
	Get(key string) (string, error)
	Put(key string, value string) error
	Append(key string, value string) error
}
