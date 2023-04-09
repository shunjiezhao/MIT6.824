package shardkv

type SingleExec interface {
	IxExec(args any) bool
	Exec(args any, hook func() any)
	GetIfExec(args any) (any, bool)
}

type singleExec struct {
	Result map[ClientID]map[int64]OpReply  // id, seq reply
	IsExec map[ClientID]map[int64]struct{} // id, seq isExec
}

func (s *singleExec) GetIfExec(args any) (any, bool) {
	if s.IxExec(args) {
		id := args.(OpArgs).ClientID
		seqNum := args.(OpArgs).SeqNum
		return s.Result[id][seqNum], true
	}
	return nil, false
}

func (s *singleExec) IxExec(args any) bool {
	id := args.(OpArgs).ClientID
	seqNum := args.(OpArgs).SeqNum
	if cExec, ok := s.IsExec[id]; ok {
		if _, ok = cExec[seqNum]; ok {
			return true
		}
	} else {
		s.IsExec[id] = map[int64]struct{}{}
	}

	return false
}

func (s *singleExec) Exec(args any, op func() any) {
	id := args.(OpArgs).ClientID
	seqNum := args.(OpArgs).SeqNum
	if s.IxExec(args) {
		return
	}
	data := op()

	s.IsExec[id][seqNum] = struct{}{}

	if s.Result[id] == nil {
		s.Result[id] = map[int64]OpReply{}
	}
	s.Result[id][seqNum] = data.(OpReply)
}

func NewSingleExec() SingleExec {
	return &singleExec{
		Result: map[ClientID]map[int64]OpReply{},
		IsExec: map[ClientID]map[int64]struct{}{},
	}
}
