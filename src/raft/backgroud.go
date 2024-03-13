package raft

import (
	"time"
)

func (rf *Raft) ticker() {
	for rf.killed() == false {

		rf.mu.Lock()
		// Your code here (2A)
		// Check if a leader election should be started.
		if rf.state == LEADER && time.Now().After(rf.heartbeatTime) {
			Debug(dTimer, "S%d Heartbeat timeout, boardcast heartbeat!", rf.me)
			rf.sendEntries(true)
		}

		if time.Now().After(rf.electionTime) {
			Debug(dTimer, "S%d Election timeout, start a new round election.", rf.me)
			rf.startElection()
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		time.Sleep(time.Duration(TICKINTERVAL) * time.Millisecond)
	}
}

func (rf *Raft) applyLogsLoop() {
	for rf.killed() == false {
		rf.mu.Lock()

		applyMsg := []ApplyMsg{}
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			applyMsg = append(applyMsg, ApplyMsg{
				CommandValid: true,
				Command:      rf.getLogEntry(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			})
			Debug(dLog2, "S%d Apply log at T%d. lastApplied: %d, commitIndex: %d", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
		}

		rf.mu.Unlock()

		for _, msg := range applyMsg {
			rf.applyCh <- msg
		}

		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}
