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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	raftLog := &RaftLog{
		storage: storage,
	}
	first, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	last, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	entrys, err := storage.Entries(first, last+1)
	if err != nil {
		panic(err)
	}
	raftLog.entries = entrys
	raftLog.stabled = last
	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.stabled < l.LastIndex() {
		return l.entries[l.stabled-l.firstIndex()+1:]
	}
	return []pb.Entry{}
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return l.entries[l.applied-l.firstIndex()+1:l.committed]
}

func (l *RaftLog) firstIndex() uint64 {
	if len(l.entries) > 0 {
		return l.entries[0].Index
	}
	return 0
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	return 0
}

func (l *RaftLog) LastTerm() uint64 {
	if len(l.entries) > 0{
		lastTerm, _ := l.Term(l.LastIndex())
		return lastTerm
	}
	return 0
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) == 0 || i < l.firstIndex()  || i > l.LastIndex(){
		return 0, nil
	}
	offset := l.firstIndex()
	return l.entries[i - offset].Term, nil
}

func (l *RaftLog) isLogUpToDate(logTerm, logIndex uint64) bool {
	return logTerm > l.LastTerm() || (logTerm == l.LastTerm() && logIndex >= l.LastIndex())
}

func (l *RaftLog) append(ents ...pb.Entry) {
	l.entries = append(l.entries, ents...)
}

// returns log entry from i to lastIndex
func (l *RaftLog) Entries(i uint64) []pb.Entry {
	if i > l.LastIndex() {
		return nil
	}
	return l.entries[i - l.firstIndex():]
}

// check if can append
func (l * RaftLog) maybeAppend(prevLogIndex uint64, prevLogTerm uint64, committed uint64, ents []*pb.Entry) (uint64, bool) {
	if l.matchTerm(prevLogIndex, prevLogTerm) { // leader and follower match in prevLogIndex
		conflictIndex := l.findConflict(ents)
		switch {
		case conflictIndex == 0: // no conflict
		default:
			if conflictIndex <= l.LastIndex() {
				l.entries = l.entries[0:conflictIndex-l.firstIndex()]
				l.stabled = l.LastIndex()
			}
			offset := prevLogIndex + 1
			for _, e := range ents[conflictIndex-offset:]{
				ent := *e
				l.entries = append(l.entries, ent)
			}
		}
		indexLastNew := prevLogIndex + uint64(len(ents))
		//l.stabled = indexLastNew
		if committed > l.committed {
			l.committed = min(committed, indexLastNew)
		}
		return indexLastNew, true
	}
	return 0, false
}

// findConflict find Index of the conflict
// It returns the first conflicting entry between
// the existing entries and the given entries
// If there's no conflicting entries return 0
// if the existing entries and the given entries conflict
// at ent.Index or
func (l * RaftLog) findConflict(ents []*pb.Entry) uint64 {
	// iterating ents
	for _, ent := range ents{
		// find a conflict entry or RaftLog doesn't contain an entry at ent.Index
		if !l.matchTerm(ent.Index, ent.Term) {
			// find a conflict entry
			if ent.Index <= l.LastIndex() {
				log.Infof("find conflict log entry at index %d", ent.Index)
			}
			return ent.Index
		}
	}
	return 0
}

func (l *RaftLog) matchTerm(prevLogIndex, prevLogTerm uint64) bool {
	term, _ := l.Term(prevLogIndex)
	return term == prevLogTerm
}

func (l *RaftLog) maybeCommit(index, term uint64) bool{
	// note that only log entries from the leader's current term are committed by counting replicas
	iterm, _ := l.Term(index)
	if index > l.committed && term == iterm{
		l.committed = index
		return true
	}
	return false
}