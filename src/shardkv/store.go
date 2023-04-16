package shardkv

import (
	"errors"
)

type store struct {
	Map map[string]string
}

func newStore() *store {
	return &store{
		map[string]string{},
	}
}
func (s store) Get(key string) (string, error) {
	value, ok := s.Map[key]
	if !ok {
		return "", errors.New(ErrNoKey)
	}
	return value, nil
}

func (s store) Put(key string, value string) error {
	s.Map[key] = value
	return nil
}

func (s store) Append(key string, value string) error {
	if _, ok := s.Map[key]; !ok {
		s.Map[key] = ""
	}
	s.Map[key] += value
	return nil
}

func (s store) Clone() Store {
	newMap := make(map[string]string)
	for k, v := range s.Map {
		newMap[k] = v
	}
	return store{
		newMap,
	}
}

type Store interface {
	Get(key string) (string, error)
	Put(key string, value string) error
	Clone() Store
	Append(key string, value string) error
}
