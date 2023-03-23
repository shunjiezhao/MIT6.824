package raft

type (
	Log struct {
		Logs  []LogEntry
		Start int
	}
	LogEntry struct {
		Term    int
		Command interface{}
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
func (l *Log) lastLogIndex() int {
	return len(l.Logs) - 1
}
func (l *Log) lastLog() LogEntry {
	return l.entryAt(l.lastLogIndex())
}

func (l *Log) nextLogIndex() int {
	return len(l.Logs)
}
func mkLog() Log {
	log := Log{Logs: make([]LogEntry, 1), Start: 0}
	log.Logs[0] = LogEntry{Term: 0}
	return log
}

// return Logs[x:y]
func (l *Log) cloneRange(x, y int) []LogEntry {
	if x > y {
		return nil
	}
	cone := make([]LogEntry, y-x+1)
	copy(cone, l.Logs[x:y+1])
	return cone
}

// delete x + 1 -> end
func (l *Log) cut2end(x int) {
	l.Logs = l.cloneRange(0, x)
}
