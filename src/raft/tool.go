package raft

import (
	"log"
	"math/rand"
	"time"
)

func (rf *Raft) getLogEntry(index int) *LogEntry {
	logs := rf.log

	if index < 0 {
		log.Panicf("S%d Fail to get LogEntry, index < 0.", rf.me)
	}

	if index == 0 {
		return &LogEntry{
			Command: nil,
			Term:    0,
		}
	}

	if index > len(logs) {
		return &LogEntry{
			Command: nil,
			Term:    -1,
		}
	}
	return &logs[index-1]
}

func (rf *Raft) getLastLogInfo() (lastLogIndex int, lastLogTerm int) {
	logs := rf.log
	lastLogIndex = len(logs)
	lastLogTerm = rf.getLogEntry(lastLogIndex).Term
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) getLogSlice(start, end int) []LogEntry {
	logs := rf.log
	return logs[start-1 : end-1]
}

func (rf *Raft) setElectionTime() {
	t := time.Now()
	extra := rand.Int63() % int64(ELECTIONTIMEOUT)
	electionTimeout := extra + ELECTIONTIMEOUT
	t = t.Add(time.Duration(electionTimeout) * time.Millisecond)
	rf.electionTime = t
}

func (rf *Raft) setHeartbeatTime() {
	t := time.Now()
	t = t.Add(time.Duration(HEARTBEATINTERVAL) * time.Millisecond)
	rf.heartbeatTime = t
}

// Rules for Servers: All Servers
// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
func (rf *Raft) checkTerm(term int) bool {
	if term > rf.currentTerm {
		Debug(dTerm, "S%d Term is lower, update term to T%d, and convert to follower. (%d < %d)", rf.me, term, rf.currentTerm, term)
		rf.state = FOLLOWER
		rf.currentTerm = term
		rf.votedFor = -1
		return true
	}
	return false
}

func max(a, b int) int {
	if a < b {
		return b
	} else {
		return a
	}
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
