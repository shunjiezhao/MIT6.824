package shardkv

import (
	"github.com/panjf2000/ants/v2"
)

type Pool struct {
	*ants.Pool
}

func NewPool() *Pool {
	pool, err := ants.NewPool(300)
	if err != nil {
		panic(err)
	}
	return &Pool{
		Pool: pool,
	}
}

func (p *Pool) Do(f func()) {
	err := p.Pool.Submit(f)
	if err != nil {
		panic(err)
	}
}
