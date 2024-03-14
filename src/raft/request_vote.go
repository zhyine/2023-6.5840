package raft

import "sync"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// RequestVote RPC: Receiver implementation
//  1. Reply false if term < currentTerm (§5.1)
//  2. If votedFor is null or candidated, and candidate's log is at least up-to-date receiver's log, grant vote (§5.2,§5.4)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dVote, "S%d Receive RequestVote from S%d at T%d.", rf.me, args.CandidateId, rf.currentTerm)

	// RequestVote RPC: Receiver implementation-Rule1
	if args.Term < rf.currentTerm {
		Debug(dTerm, "S%d Reject the RequestVote from S%d at T%d, since Candidate's term is lower.", rf.me, args.CandidateId, rf.currentTerm)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// Rules for Servers: All Servers-Rule2
	rf.checkTerm(args.Term)

	reply.Term = rf.currentTerm

	// RequestVote RPC: Receiver implementation-Rule2
	lastLogIndex, lastLogTerm := rf.getLastLogInfo()
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// Raft determines which of tow logs is more up-to-date by comparing the index and term of the last entries in the logs (§5.4)
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			Debug(dVote, "S%d Grant vote to S%d at T%d.", rf.me, args.CandidateId, rf.currentTerm)

			reply.VoteGranted = true
			rf.votedFor = args.CandidateId

			Debug(dTerm, "S%d Reset election timeout.", rf.me)
			rf.setElectionTime()
		} else {
			Debug(dVote, "S%d Reject the RequestVote from S%d at T%d, since the Candidate's log is not up-to-date.", rf.me, args.CandidateId, rf.currentTerm)
		}
	} else {
		Debug(dVote, "S%d Already vote for S%d, reject the RequestVote.", rf.me, rf.votedFor)
	}

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Rules for Servers: Candidates (§5.2)
// On conversion to candidate, start election
//   - Increment currentTerm
//   - Vote for self
//   - Reset election timer
//   - Send RequestVote RPCs to all others servers
func (rf *Raft) startElection() {
	rf.currentTerm++
	Debug(dTerm, "S%d Start a new term, which is T%d", rf.me, rf.currentTerm)
	rf.state = CANDIDATE
	rf.votedFor = rf.me
	rf.setElectionTime()
	Debug(dTimer, "S%d Reset election timeout.", rf.me)

	lastLogIndex, lastLogTerm := rf.getLastLogInfo()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	voteCount := 1
	var once sync.Once

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		Debug(dVote, "S%d Send RequestVote to S%d at T%d", rf.me, peer, rf.currentTerm)
		go rf.candidateSendRequsetVotes(peer, args, &voteCount, &once)
	}

}

func (rf *Raft) candidateSendRequsetVotes(server int, args *RequestVoteArgs, voteCount *int, once *sync.Once) {
	reply := &RequestVoteReply{}
	if rf.sendRequestVote(server, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		Debug(dVote, "S%d Receive RequestVote reply from S%d at T%d", rf.me, server, rf.currentTerm)

		// if reply.Term < rf.currentTerm {
		// 	Debug(dLog, "S%d Term lower, invalid RequestVote reply. reply.Term: %d, rf.currentTerm: %d", rf.me, reply.Term, rf.currentTerm)
		// 	return
		// }

		if args.Term != rf.currentTerm {
			Debug(dWarn, "S%d Term has changed after sending RequestVote, reply was discarded."+"args.Term: %d, rf.currentTerm: %d", rf.me, args.Term, rf.currentTerm)
			return
		}

		// Rules for Servers: All Servers-Rule2
		rf.checkTerm(reply.Term)

		if reply.VoteGranted {
			Debug(dVote, "S%d Get a yes vote from S%d at T%d.", rf.me, server, rf.currentTerm)
			*voteCount++
			// If votes received from majority of servers: become leader
			if *voteCount > len(rf.peers)/2 {
				once.Do(func() {
					Debug(dLeader, "S%d Receive majority votes at T%d, become leader.", rf.me, rf.currentTerm)

					rf.state = LEADER
					lastLogIndex, _ := rf.getLastLogInfo()
					for peer := range rf.peers {
						rf.nextIndex[peer] = lastLogIndex + 1
						rf.matchIndex[peer] = 0
					}

					// Rules for Servers: Leaders-Rule1
					// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
					rf.sendEntries(true)
				})
			}
		} else {
			Debug(dVote, "S%d Get a reject vote from S%d at T%d.", rf.me, server, rf.currentTerm)
		}
	}
}
