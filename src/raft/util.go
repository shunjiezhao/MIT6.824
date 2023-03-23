package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	DHeart   logTopic = "Heart"
	DIndex   logTopic = "Index"
	DSys     logTopic = "SYS"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

// Debugging
var _debug = true

var debugStart time.Time
var debugVerbosity int

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}
func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(rf *Raft, topic logTopic, format string, a ...interface{}) {
	if topic == DHeart {
		return
	}
	if _debug {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		if rf == nil || rf.killed() == false {
			log.Printf(format, a...)
		}
	}
}
func DebugT(t time.Time, topic logTopic, format string, a ...interface{}) {
	if _debug {
		time := t.Sub(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func getServerName(me int) string {
	return fmt.Sprintf("S%d", me)
}
