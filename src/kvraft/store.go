package kvraft

import (
	"6.5840/labgob"
	"bytes"
	"errors"
	"log"
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

type Store interface {
	Get(key string) (string, error)
	Put(key string, value string) error
	Append(key string, value string) error
}

func (kv *KVServer) GetStoreBytes() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.Store) != nil ||
		e.Encode(kv.IsExec) != nil {
		panic("encode error")
	}

	Debug(kv, dInfo, "encode snap shot success")
	return w.Bytes()
}

func (kv *KVServer) InstallStoreBytes(b []byte) {
	if len(b) == 0 {
		return
	}
	r := bytes.NewBuffer(b)
	d := labgob.NewDecoder(r)
	var s store
	var isExec map[int]int64

	err1 := d.Decode(&s)
	err2 := d.Decode(&isExec)
	if err1 != nil || err2 != nil {
		log.Println(err1, err2)
		panic("")
	}

	kv.Store = s
	kv.IsExec = isExec
	Debug(kv, dInfo, "instsall snap shot success")
}
