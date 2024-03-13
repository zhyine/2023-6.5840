package raft

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries RPC: Receiver implementation
//  1. Reply false if term < currentTerm
//  2. Reply false if log doesn't contain an entry at prevLogIndx whose term matches prevLogTerm
//  3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
//  4. Append any new entries not already in the log
//  5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(args.Entries) == 0 {
		Debug(dLog2, "S%d Receive Heartbeat from S%d at T%d.", rf.me, args.LeaderId, rf.currentTerm)
	} else {
		Debug(dLog2, "S%d Receive AppendEntries from S%d at T%d.", rf.me, args.LeaderId, rf.currentTerm)
	}

	reply.Term = rf.currentTerm

	Debug(dTimer, "S%d Reset election timeout.", rf.me)
	rf.setElectionTime()

	// Rules for Servers: All Servers-Rule2
	rf.checkTerm(args.Term)

	// Rules for Servers: Candidates-Rule3
	if rf.state == CANDIDATE && rf.currentTerm < args.Term {
		Debug(dLog2, "S%d Convert from candidate to follower at T%d.", rf.me, rf.currentTerm)
		rf.state = FOLLOWER
	}

	// AppendEntries RPC: Receiver implementation-Rule1
	if args.Term < rf.currentTerm {
		Debug(dTerm, "S%d Reject the AppendEntries from S%d at T%d, since leader's term is lower.", rf.me, args.LeaderId, rf.currentTerm)
		reply.Success = false
		return
	}

	// AppendEntries RPC: Receiver implementation-Rule2
	if rf.getLogEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
		Debug(dLog2, "S%d PrevLogEntries do not match. Ask leader to retry.", rf.me)
		reply.Success = false
		return
	}

	// AppendEntries RPC: Receiver implementation-Rule3
	for i, entry := range args.Entries {
		if rf.getLogEntry(args.PrevLogIndex+i+1).Term != entry.Term {
			rf.log = append(rf.getLogSlice(1, i+1+args.PrevLogIndex), args.Entries...)
			break
		}
	}

	// AppendEntries RPC: Receiver implementation-Rule5
	if args.LeaderCommit > rf.commitIndex {
		Debug(dCommit, "S%d Get higher commitIndex from S%d at T%d, updating commitIndex. commitIndex: %d, LeaderCommit: %d",
			rf.me, args.LeaderId, rf.currentTerm, rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		Debug(dCommit, "S%d Updated commitIndex at T%d. CI: %d.", rf.me, rf.currentTerm, rf.commitIndex)
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendEntries(isHeartbeat bool) {
	Debug(dTimer, "S%d Reset heartbeat timeout.")
	rf.setHeartbeatTime()

	Debug(dTimer, "S%d Reset election timeout.")
	rf.setElectionTime()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		nextIndex := rf.nextIndex[peer]

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  rf.getLogEntry(nextIndex - 1).Term,
			LeaderCommit: rf.commitIndex,
		}

		if isHeartbeat {
			args.Entries = make([]LogEntry, 0)
			Debug(dLog, "S%d Send heartbeat to S%d at T%d. PrevLogIndex: %d, PrevLogTerm: %d, LeaderCommit: %d.", rf.me, peer, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
			go rf.leaderSendAppendEntries(peer, args)
		}
	}
}

func (rf *Raft) leaderSendAppendEntries(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	if rf.sendAppendEntries(server, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		Debug(dLog, "S%d Receive AppendEntries reply from S%d at T%d.", rf.me, server, rf.currentTerm)

		// Rules for Servers: All Servers-Rule2
		if rf.checkTerm(reply.Term) {
			return
		}

		if args.Term != rf.currentTerm {
			Debug(dWarn, "S%d Term has changed after the AppendEntries, reply was discarded."+"args.Term: %d, rf.currentTerm: %d", rf.me, args.Term, rf.currentTerm)
			return
		}

		// All for Servers: Leaders-Rule3
		// If successful: update nextIndex and matchIndex for follower
		// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
		if reply.Success {
			newNextIndex := args.PrevLogIndex + len(args.Entries) + 1
			newMatchIndex := args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = max(newNextIndex, rf.nextIndex[server])
			rf.matchIndex[server] = max(newMatchIndex, rf.matchIndex[server])
		} else {
			if rf.nextIndex[server] > 1 {
				rf.nextIndex[server]--
			}

			lastLogIndex, _ := rf.getLastLogInfo()
			nextIndex := rf.nextIndex[server]
			newArgs := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: nextIndex - 1,
				PrevLogTerm:  rf.getLogEntry(nextIndex - 1).Term,
				Entries:      rf.getLogSlice(nextIndex, lastLogIndex+1),
				LeaderCommit: rf.commitIndex,
			}

			go rf.leaderSendAppendEntries(server, newArgs)
		}
	}
}
