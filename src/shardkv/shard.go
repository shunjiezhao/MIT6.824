package shardkv

type ShardStatus int

const (
	Serveing = iota // 说明现在这个分片没有其余动作
	Pulling         // 还需要拉
	Delete          // 需要删除
	GC              // 拉完了，需要发送确认收到
)

func (s ShardStatus) String() string {
	switch s {

	case Serveing:
		return "Serveing"
	case Pulling:
		return "Pulling"
	case Delete:

		return "Delete"
	case GC:
		return "GC"
	default:
		return "Unknown"
	}
}

type Shard struct {
	Store  store
	Status ShardStatus
}

func NewShard() *Shard {
	return &Shard{
		Store:  newStore(),
		Status: Serveing,
	}
}

func (s *Shard) Get(key string) (string, error) {
	return s.Store.Get(key)

}
func (s *Shard) Put(key, value string) error {
	return s.Store.Put(key, value)

}
func (s *Shard) Append(key, value string) error {
	return s.Store.Append(key, value)
}

func (s *Shard) Clone() Shard {
	return Shard{
		Store:  s.Store.Clone(),
		Status: s.Status,
	}
}
