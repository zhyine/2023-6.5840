package raft

import (
	"log"
	"math"
	"math/rand"
	"time"
)

func (rf *Raft) logicIndexToRealIndex(logicIndex int) int {
	return logicIndex - rf.lastIncludedIndex
}

// Get the LogEntry with index
// index and term of the LogEntry start from 1
func (rf *Raft) getLogEntry(index int) *LogEntry {
	logs := rf.log

	realIndex := rf.logicIndexToRealIndex(index)
	// realIndex := index - rf.lastIncludedIndex

	if realIndex < 0 {
		log.Panicf("S%d Fail to get LogEntry, index < 0.", rf.me)
	}

	if realIndex == 0 {
		return &LogEntry{
			Command: nil,
			Term:    rf.lastIncludedTerm,
		}
	}

	if realIndex > len(logs) {
		return &LogEntry{
			Command: nil,
			Term:    -1,
		}
	}
	return &logs[realIndex-1]
}

func (rf *Raft) getLastLogInfo() (lastLogIndex int, lastLogTerm int) {
	logs := rf.log
	lastLogIndex = len(logs) + rf.lastIncludedIndex
	lastLogTerm = rf.getLogEntry(lastLogIndex).Term
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) getLogSlice(start, end int) []LogEntry {
	logs := rf.log
	realStart, realEnd := rf.logicIndexToRealIndex(start), rf.logicIndexToRealIndex(end)
	// realStart, realEnd := start-rf.lastIncludedIndex, end-rf.lastIncludedIndex
	if realStart <= 0 {
		Debug(dError, "LogEntries.getSlice: startIndex out of range. startIndex: %d, len: %d.",
			realStart, len(logs))
		log.Panicf("LogEntries.getSlice: startIndex out of range. (%d < %d)", realStart, rf.lastIncludedIndex)
	}
	if realEnd > len(logs)+1 {
		Debug(dError, "LogEntries.getSlice: endIndex out of range. endIndex: %d, len: %d.",
			realEnd, len(logs))
		log.Panicf("LogEntries.getSlice: endIndex out of range. (%d > %d)", realEnd, len(logs)+1+rf.lastIncludedIndex)
	}
	if realStart > realEnd {
		Debug(dError, "LogEntries.getSlice: startIndex > endIndex. (%d > %d)", realStart, realEnd)
		log.Panicf("LogEntries.getSlice: startIndex > endIndex. (%d > %d)", realStart, realEnd)
	}
	return append([]LogEntry(nil), logs[realStart-1:realEnd-1]...)
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
		rf.leaderId = -1
		rf.persist()
		return true
	}
	return false
}

func (rf *Raft) getIndexBoundaryWithTerm(term int) (int, int) {
	logs := rf.log
	if term == 0 {
		return 0, 0
	}
	start, end := math.MaxInt, -1
	for i := 1 + rf.lastIncludedIndex; i <= len(logs)+rf.lastIncludedIndex; i++ {
		if rf.getLogEntry(i).Term == term {
			start = min(start, i)
			end = max(end, i)
		}
	}
	if end == -1 {
		return -1, -1
	}
	return start, end
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
