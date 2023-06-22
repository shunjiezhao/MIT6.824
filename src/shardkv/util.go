package shardkv

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	dLock   logTopic = "LOCK"
	dRpc    logTopic = "RPC"
	dApply  logTopic = "APPLY"
	dChan   logTopic = "CHAN"
	dInfo   logTopic = "INFO"
	dTOut   logTopic = "TIMEOUT"
	dErr    logTopic = "ERROR"
	dConfig logTopic = "CONFIG"
	dShard  logTopic = "SHARD"
	dGC     logTopic = "GC"
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

func Debug(sc *ShardKV, topic logTopic, format string, a ...interface{}) {
	if topic == dLock {
		return
	}
	if _debug {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		if sc != nil {
			prefix += fmt.Sprintf("SKV-%d gid: %v ", sc.me, sc.gid)
		}
		format = prefix + format
		log.Printf(format, a...)
	}
}

func panicIf(cond bool, format string, a ...interface{}) {
	if cond {
		panic(fmt.Sprintf(format, a...))
	}
}
