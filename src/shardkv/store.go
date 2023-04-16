package shardkv

import (
	"errors"
)

type store map[string]string

func newStore() store {
	return map[string]string{}
}
func (s store) Get(key string) (string, error) {
	value, ok := s[key]
	if !ok {
		return "", errors.New(ErrNoKey)
	}
	return value, nil
}

func (s store) Put(key string, value string) error {
	s[key] = value
	return nil
}

func (s store) Append(key string, value string) error {
	if _, ok := s[key]; !ok {
		s[key] = ""
	}
	s[key] += value
	return nil
}

func (s store) Clone() store {
	newMap := make(map[string]string)
	for k, v := range s {
		newMap[k] = v
	}
	return newMap
}
