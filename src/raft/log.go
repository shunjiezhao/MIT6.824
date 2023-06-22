package raft

import (
	"fmt"
	"sort"
)

type (
	Log struct {
		Logs  []LogEntry
		Start int
	}
	LogEntry struct {
		Term    int
		Command interface{}
		Index   int
	}
)

func (l *Log) start() int {
	return l.Start + 1
}
func (l *Log) append(e LogEntry) {
	l.Logs = append(l.Logs, e)
}
func (l *Log) entryAt(i int) LogEntry {
	return l.Logs[i-l.Start]
}

func (l *Log) startLog() LogEntry {
	return l.entryAt(l.start())
}
func (l *Log) lastLogIndex() int {
	return len(l.Logs) - 1 + l.Start
}
func (l *Log) lastLog() LogEntry {
	return l.entryAt(l.lastLogIndex())
}

func (l *Log) nextLogIndex() int {
	return len(l.Logs) + l.Start
}
func mkLog(start int, term int) Log {
	log := Log{Logs: make([]LogEntry, 1), Start: start}
	log.Logs[0] = LogEntry{Term: term}
	return log
}

// copy [start + 1,end]
func (l *Log) setStart(start int, lastTerm int) {
	panicIf(start <= l.Start, "start <= l.Start 说明安装 已经安装的") // 这说明我们早已经持久化过了这个
	// 这种情况 出现在 rpc 延迟
	// 正常情况下是不会出现的，因为快照只会压缩在 commit Index 的日志
	// commit Index 是大多数人的心愿
	// 所以发送这个的leader是不会被选中的

	prevLastIndex := l.lastLogIndex()
	// copy [start + 1, end]
	diff := start - l.Start
	clone := make([]LogEntry, len(l.Logs)-diff)
	copy(clone, l.Logs[diff:])
	l.Start = start
	l.Logs = clone
	panicIf(prevLastIndex != l.lastLogIndex(), fmt.Sprintf("set start error: lastindex want: %d but: %d", prevLastIndex, l.lastLogIndex()))
}

// return Logs[x:y]
func (l *Log) cloneRange(x, y int) []LogEntry {
	x, y = x-l.Start, y-l.Start
	if x > y {
		return nil
	}
	cone := make([]LogEntry, y-x+1)
	copy(cone, l.Logs[x:y+1])
	return cone
}

// delete x + 1 -> end
func (l *Log) cut2end(x int) {
	l.Logs = l.cloneRange(l.Start, x)
}

// return the first index >= term
func (l *Log) search(term int) int {
	idx := sort.Search(len(l.Logs), func(i int) bool {
		return l.Logs[i].Term >= term
	}) + l.Start
	if idx == len(l.Logs)+l.Start || l.entryAt(idx).Term != term {
		return -1
	}
	return idx
}

func (l *Log) TermLastIndex(term int) int {
	idx := sort.Search(len(l.Logs), func(i int) bool {
		return l.Logs[i].Term > term
	}) - 1 + l.Start
	return idx
}

// return the first index >= term
func (l *Log) contain(term int) bool {
	idx := l.search(term)
	if idx == -1 {
		return false
	}
	return l.entryAt(idx).Term == term
}
