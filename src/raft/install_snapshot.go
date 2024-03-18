package raft

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

// InstallSnapshot RPC: Receiver implementation
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dSnap, "S%d Receive InstallSnapshot request from S%d at T%d", rf.me, args.LeaderId, rf.currentTerm)

	// Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		Debug(dSnap, "S%d Reject the InstallSnapshot from S%d at T%d, since leader's term is lower.", rf.me, args.LeaderId, rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	}

	// Rules for Servers: All Servers-Rule2
	rf.checkTerm(args.Term)

	reply.Term = rf.currentTerm

	rf.leaderId = args.LeaderId

	Debug(dTimer, "S%d Reset Election Timeout.")
	rf.setElectionTime()

	if rf.waitingIndex >= args.LastIncludedIndex {
		Debug(dSnap, "S%d Reject the InstallSnapshot request from S%d at T%d, since a newer snapshot already exists.", rf.me, args.LeaderId, rf.currentTerm)
		return
	}

	lastLogIndex, _ := rf.getLastLogInfo()
	// if lastLogIndex >= args.LastIncludedIndex {
	// 	rf.log = rf.getLogSlice(args.LastIncludedIndex+1, lastLogIndex+1)
	// } else {
	// 	rf.log = []LogEntry{}
	// }

	// if existing log entry has same index and term as snapshot's last included entry, retain log entries following it and reply
	if rf.getLogEntry(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		rf.log = rf.getLogSlice(args.LastIncludedIndex+1, lastLogIndex+1)
	} else {
		// Discard the entire log
		rf.log = []LogEntry{}
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.snapshot = args.Data

	// Save snapshot file
	rf.persist()

	// Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	rf.waitingIndex = args.LastIncludedIndex
	rf.waitingTerm = args.LastIncludedTerm
	rf.waitingSnapshot = args.Data
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendSnapshot(server int) {
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapshot,
	}

	Debug(dSnap, "S%d Send InstallSnapshot request to S%d at T%d.", rf.me, server, rf.currentTerm)
	go rf.leaderSendInstallSnapshot(server, args)

}

func (rf *Raft) leaderSendInstallSnapshot(server int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	if rf.sendInstallSnapshot(server, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		Debug(dSnap, "S%d Receive InstallSnapshot reply from S%d at T%d.", rf.me, server, rf.currentTerm)

		if rf.currentTerm != args.Term {
			Debug(dWarn, "S%d Term has changed after the InstallSnapshot, reply was discarded."+"args.Term: %d, rf.currentTerm: %d", rf.me, args.Term, rf.currentTerm)
			return
		}

		// Rules for Servers: All Servers-Rule2
		rf.checkTerm(reply.Term)

		newNextIndex := args.LastIncludedIndex + 1
		newMatchIndex := args.LastIncludedIndex
		rf.nextIndex[server] = max(newNextIndex, rf.nextIndex[server])
		rf.matchIndex[server] = max(newMatchIndex, rf.matchIndex[server])
	}
}
