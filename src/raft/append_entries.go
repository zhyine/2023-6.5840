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

	// 2C
	XTerm  int // term in the conflicting entry
	XIndex int // index of first entry with that term
	XLen   int // log length
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

	// AppendEntries RPC: Receiver implementation-Rule1
	if args.Term < rf.currentTerm {
		Debug(dTerm, "S%d Reject the AppendEntries from S%d at T%d, since leader's term is lower.", rf.me, args.LeaderId, rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// Rules for Servers: All Servers-Rule2
	rf.checkTerm(args.Term)

	// Rules for Servers: Candidates-Rule3
	if rf.state == CANDIDATE && rf.currentTerm == args.Term {
		Debug(dLog2, "S%d Convert from candidate to follower at T%d.", rf.me, rf.currentTerm)
		rf.state = FOLLOWER
	}

	Debug(dTimer, "S%d Reset Election Timeout.", rf.me)
	rf.setElectionTime()

	reply.Term = rf.currentTerm

	// AppendEntries RPC: Receiver implementation-Rule2
	if args.PrevLogTerm == -1 || rf.getLogEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
		Debug(dLog2, "S%d PrevLogEntries do not match. Ask leader to retry.", rf.me)
		reply.Success = false
		reply.XTerm = rf.getLogEntry(args.PrevLogIndex).Term
		reply.XIndex, _, _ = rf.getIndexBoundaryWithTerm(reply.XTerm)
		reply.XLen = len(rf.log)
		return
	}

	// AppendEntries RPC: Receiver implementation-Rule3
	for i, entry := range args.Entries {
		if rf.getLogEntry(args.PrevLogIndex+i+1).Term != entry.Term {
			rf.log = append(rf.getLogSlice(1, i+1+args.PrevLogIndex), args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	// AppendEntries RPC: Receiver implementation-Rule5
	if args.LeaderCommit > rf.commitIndex {
		Debug(dCommit, "S%d Get higher commitIndex from S%d at T%d, updating commitIndex. rf.commitIndex: %d, args.LeaderCommit: %d",
			rf.me, args.LeaderId, rf.currentTerm, rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		Debug(dCommit, "S%d Update commitIndex at T%d. rf.commitIndex: %d.", rf.me, rf.currentTerm, rf.commitIndex)
	}

	Debug(dLog2, "S%d Append entries success. Entries: %v", rf.me, args.Entries)

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendEntries(isHeartbeat bool) {
	Debug(dTimer, "S%d Reset Heartbeat Timeout.", rf.me)
	rf.setHeartbeatTime()

	Debug(dTimer, "S%d Reset Election Timeout.", rf.me)
	rf.setElectionTime()

	lastLogIndex, _ := rf.getLastLogInfo()
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

		// Rules for Servers: Leaders-Rule3
		// If last log index >= nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
		if lastLogIndex >= nextIndex {
			args.Entries = rf.getLogSlice(nextIndex, lastLogIndex+1)
			Debug(dLog, "S%d Send AppendEntries to S%d at T%d. args.PrevLogIndex: %d, args.PrevLogTerm: %d, args.LeaderCommit: %d, args.Entries: %v.", rf.me, peer, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)
			go rf.leaderSendAppendEntries(peer, args)
		} else if isHeartbeat {
			args.Entries = make([]LogEntry, 0)
			Debug(dLog, "S%d Send Heartbeat to S%d at T%d. args.PrevLogIndex: %d, args.PrevLogTerm: %d, args.LeaderCommit: %d.", rf.me, peer, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
			go rf.leaderSendAppendEntries(peer, args)
		}
	}
}

func (rf *Raft) leaderSendAppendEntries(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		Debug(dLog, "S%d Receive AppendEntries reply from S%d at T%d.", rf.me, server, rf.currentTerm)

		if args.Term != rf.currentTerm {
			Debug(dWarn, "S%d Term has changed after the AppendEntries, reply was discarded."+"args.Term: %d, rf.currentTerm: %d", rf.me, args.Term, rf.currentTerm)
			return
		}

		// Rules for Servers: All Servers-Rule2
		if rf.checkTerm(reply.Term) {
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

			// All for Servers: Leaders-Rule4
			// If there exits an N such that N > commitIndex, a majority of mathchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex
			for N := len(rf.log); N > rf.commitIndex && rf.getLogEntry(N).Term == rf.currentTerm; N-- {
				count := 1
				for peer, matchIndex := range rf.matchIndex {
					if peer == rf.me {
						continue
					}

					if matchIndex >= N {
						count++
					}
				}

				if count > len(rf.peers)/2 {
					rf.commitIndex = N
					Debug(dCommit, "S%d Update commitIndex at T%d for majority consensus. rf.commitIndex: %d.", rf.me, rf.currentTerm, rf.commitIndex)
					break
				}
			}
		} else {
			// if rf.nextIndex[server] > 1 {
			// 	rf.nextIndex[server]--
			// }

			if reply.XTerm == -1 {
				// follower's log is too short
				rf.nextIndex[server] = reply.XLen + 1
			} else {
				_, end, ok := rf.getIndexBoundaryWithTerm(reply.XTerm)
				if ok {
					// leader has XTerm
					rf.nextIndex[server] = end
				} else {
					// leader doesn't have XTerm
					rf.nextIndex[server] = reply.XIndex
				}
			}

			lastLogIndex, _ := rf.getLastLogInfo()
			nextIndex := rf.nextIndex[server]
			if lastLogIndex >= nextIndex {
				Debug(dLog, "S%d Inconsistent log, retrying.", rf.me)
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
}
