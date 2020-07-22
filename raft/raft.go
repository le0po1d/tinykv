// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sort"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	// including itself
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	step stepFunc

	tick func()

	rand *rand.Rand

	randomElectionTimeout int

	//hs pb.HardState
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	hardState, _, _ := c.Storage.InitialState()
	// Your Code Here (2A).
	raft := Raft{
		Term: hardState.Term,
		Vote: hardState.Vote,
		id: c.ID,
		electionTimeout: c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
	}
	// rand to generate random election timeout
	raft.rand = rand.New(rand.NewSource(int64(raft.id)))
	// Next is the index of the next log entry to send to that server
	raft.Prs = make(map[uint64]*Progress)
	for _, peer := range c.peers{
		raft.Prs[peer] = &Progress{Next: 1}
	}
	// initialize log
	raft.RaftLog = newLog(c.Storage)
	// convert to follower
	raft.RaftLog.committed = hardState.Commit
	raft.becomeFollower(raft.Term, None)
	return &raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to]
	prevLogIndex := pr.Next - 1
	// index of log entry immediately preceding new ones
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)
	// log entries to send
	ents := r.RaftLog.Entries(pr.Next)
	entries := make([]*pb.Entry, 0, len(ents))
	for i := range ents{
		entries = append(entries, &ents[i])
	}
	msg := &pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To: to,
		LogTerm: prevLogTerm,
		Index: prevLogIndex,
		Entries: entries,
		Commit: r.RaftLog.committed,
	}
	r.send(msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.send(&pb.Message{To: to, MsgType: pb.MessageType_MsgHeartbeat})
}

// 上层应用调用tickXXX()接口来advance 两个logical clock
// 心跳的timeout时间固定
// 当Leader的心跳timeout时 Leader将发送MessageType_MsgBeat这个本地消息给Step方法
//
func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.resetHeartbeatTimer()
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat})
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.isElectionTimeout() {
		r.resetElectionTimer()
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.step = stepLeader
	r.tick = r.tickHeartbeat
	r.State = StateLeader
	r.Lead = r.id
	r.appendEntry([]*pb.Entry{{Data: nil}})
	log.Infof("%d becomes leader at term %d", r.id, r.Term)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.step = stepFollower
	r.tick = r.tickElection
	r.Reset(term)
	r.Lead = lead
	r.State = StateFollower
	log.Infof("%d becomes follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
// Beginning of a election
// 1.increment its current term by 1
// 2.transition to candidate state
// 3.vote for itself
// 4.issue RequestVote RPC in parallel to other nodes in cluster
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		return
	}
	r.tick = r.tickElection
	r.step = stepCandidate
	// increment current term
	r.Reset(r.Term + 1)
	// change state
	r.State = StateCandidate
	// vote for itself
	r.Vote = r.id
	log.Infof("%d become candidate at term %d", r.id, r.Term)
}

// request vote from other nodes in cluster
func (r *Raft) requestVote() {
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm := r.RaftLog.LastTerm()
	for peer_id := range r.Prs {
		if peer_id == r.id {
			continue
		}
		msg := pb.Message{
			MsgType:              pb.MessageType_MsgRequestVote,
			To:                   peer_id,
			Term:                 r.Term,
			LogTerm:              lastLogTerm,
			Index:                lastLogIndex,
		}
		r.send(&msg)
	}
}

func (r *Raft) Reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None
	r.resetElectionTimer()
	r.resetHeartbeatTimer()
	r.votes = make(map[uint64]bool)
}

func (r *Raft) send(m *pb.Message) {
	m.From = r.id
	m.Term = r.Term
	r.msgs = append(r.msgs, *m)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	//election timeout should campaign
	if m.MsgType == pb.MessageType_MsgHup {
		log.Infof("%d start campaign at term %d", r.id, r.Term)
		r.campaign()
		return nil
	}
	//check term
	switch {
	case m.Term == 0: 		// stands for local messsage
	case m.Term > r.Term:	// receive message with higher term should become follower
		lead := m.From
		if m.MsgType == pb.MessageType_MsgRequestVote {
			lead = None
		}
		r.becomeFollower(m.Term, lead)
	case m.Term < r.Term:	// receive message with stale term should reject
		r.send(&pb.Message{To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
		return nil
	}
	//step
	return r.step(r, m)
}

func (r *Raft) poll(id uint64, vote bool) (granted int) {
	if vote {
		log.Infof("%d received vote from %d at term %d", r.id, id, r.Term)
	} else {
		log.Infof("%d received rejection from %d at term %d", r.id, id ,r.Term)
	}
	if _, ok := r.votes[id]; !ok {
		r.votes[id] = vote
	}
	for _, voted := range r.votes{
		if voted {
			granted++
		}
	}
	return granted
}

func (r *Raft) quorum() int {
	return len(r.Prs) / 2 + 1
}

func (pr *Progress) update(index uint64) {
	if pr.Match < index {
		pr.Match = index
	}
	if pr.Next < index+1 {
		pr.Next = index + 1
	}
}

type stepFunc func(r *Raft, m pb.Message) error

func stepCandidate(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVote:	// candidate receive request vote should reject
		log.Infof("%d reject vote from %d", r.id, m.From)
		r.send(&pb.Message{To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
	case pb.MessageType_MsgRequestVoteResponse:	// candidate receive request vote response should check  for quorum
		granted := r.poll(m.From, !m.Reject)
		switch r.quorum() {
		case granted: // 半数以上的节点投了票
			r.becomeLeader()
			r.bcastAppend()
		case len(r.votes) - granted: // 半数以上的节点拒绝投票
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgHeartbeat: // candidate receive heartbeat should become follower
		r.becomeFollower(m.Term, m.From)
	case pb.MessageType_MsgAppend: //candidate receive append should become follower
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	}
	return nil
}

func stepLeader(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVote: // leader receive request vote should reject
		log.Infof("%d reject vote from %d", r.id, m.From)
		r.send(&pb.Message{To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
	case pb.MessageType_MsgBeat: // leader receive local message for heartbeat should broadcast heartbeat
		r.bcastHeartbeat()
	case pb.MessageType_MsgPropose: // leader receive propose should append entry then broadcast
		if len(m.Entries) == 0 {
			log.Errorf("%d stepped empty propose", r.id)
		}
		r.appendEntry(m.Entries)
		r.bcastAppend()
	case pb.MessageType_MsgAppendResponse: // leader receive append response
		if m.Reject { // reject append should decrement nextIndex and try again
			r.Prs[m.From].Next--
			r.sendAppend(m.From)
		} else {
			r.Prs[m.From].update(m.Index)
			if r.maybeCommit() {
				r.bcastAppend()
			}
		}
	}
	return nil
}

func stepFollower(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVote: // follower receive request vote should vote or reject
		if (r.Vote == None || r.Vote == m.From) && r.RaftLog.isLogUpToDate(m.LogTerm, m.Index) {
			log.Infof("%d voted for %d at term %d", r.id, m.From, r.Term)
			r.Vote = m.From
			r.send(&pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From})
		} else {
			log.Infof("%d reject vote from %d", r.id, m.From)
			r.send(&pb.Message{To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
		}
	case pb.MessageType_MsgPropose: // follower receive propose should redirct to leader
		r.send(&m)
	case pb.MessageType_MsgAppend:
		r.resetElectionTimer()
		r.Lead = m.From
		r.handleAppendEntries(m)
	}
	return nil
}

func (r *Raft) appendEntry(ents []*pb.Entry) {
	es := make([]pb.Entry, 0, len(ents))
	lastIndex := r.RaftLog.LastIndex()
	for i, ent := range ents{
		ent.Index = lastIndex + 1 + uint64(i)
		ent.Term = r.Term
		es = append(es, *ent)
	}
	r.RaftLog.append(es...)
	r.Prs[r.id].update(r.RaftLog.LastIndex())
	r.maybeCommit()
}

func (r *Raft) maybeCommit() bool {
	matches := make(uint64Slice, 0, len(r.Prs))
	for _, pr := range r.Prs{
		matches = append(matches, pr.Match)
	}
	sort.Sort(sort.Reverse(matches))
	maybeCommitIndex := matches[r.quorum()-1]
	return r.RaftLog.maybeCommit(maybeCommitIndex, r.Term)
}

func (r *Raft) campaign() {
	r.becomeCandidate()
	// vote for itself may have only one node
	if r.quorum() == r.poll(r.id, true) {
		r.becomeLeader()
		return
	}
	r.requestVote()
}

func (r *Raft) resetElectionTimer() {
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + r.rand.Intn(r.electionTimeout)
}

func (r *Raft) resetHeartbeatTimer() {
	r.heartbeatElapsed = 0
}

func (r *Raft) isElectionTimeout() bool {
	if r.electionElapsed < r.randomElectionTimeout {
		return false
	}else{
		return true
	}
}

// handleAppendEntries handle AppendEntries RPC request
//
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if indexLastNew, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries); ok {
		r.send(&pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, Index: indexLastNew})
		//log.Infof()
	}else{
		r.send(&pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, Reject: true, Index: m.Index})
		//log.Infof()
	}
}

func (r *Raft) bcastAppend() {
	for peer_id := range r.Prs{
		if peer_id == r.id {
			continue
		}
		r.sendAppend(peer_id)
	}
}

func (r *Raft) bcastHeartbeat() {
	for peer_id := range r.Prs{
		if peer_id == r.id {
			continue
		}
		r.sendHeartbeat(peer_id)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:                 r.Term,
		Vote:                 r.Vote,
		Commit:               r.RaftLog.committed,
	}
}
