package raft

type (
	Log struct {
		logs   []LogEntry
		index0 int
	}
	LogEntry struct {
		Term    int
		Command interface{}
	}
)

func (l *Log) start() int {
	return l.index0 + 1
}
func (l *Log) append(e LogEntry) {
	l.logs = append(l.logs, e)
}
func (l *Log) entryAt(i int) LogEntry {
	return l.logs[i-l.index0]
}
func (l *Log) lastLogIndex() int {
	return len(l.logs) - 1
}
func (l *Log) nextLogIndex() int {
	return len(l.logs)
}
func mkLog() Log {
	log := Log{logs: make([]LogEntry, 1), index0: 0}
	log.logs[0] = LogEntry{Term: 0}
	return log
}

// return logs[x:y]
func (l *Log) cloneRange(x, y int) []LogEntry {
	if x > y {
		return nil
	}
	cone := make([]LogEntry, y-x+1)
	copy(cone, l.logs[x:y+1])
	return cone
}

// delete x + 1 -> end
func (l *Log) cut2end(x int) {
	l.logs = l.cloneRange(0, x)
}
