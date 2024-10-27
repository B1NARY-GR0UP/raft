namespace go raft

enum EntryType {
    Normal = 0,
    Config = 1,
}

// Entry log entry
struct Entry {
    1: EntryType type,
    // term when entry was received by leader
    2: i64 term,
    // position of entry in the log
    3: i64 index,
    // data to storage
    4: binary data,
}

// PersistentState Persistent state on all servers
struct PersistentState {
    1: i64 current_term,
    2: i64 voted_for,
    // log[] is persistent store by the implementation of Storage interface
}

// ClusterConf configuration of Raft cluster
struct ConfState {
    1: list<i64> voters,
    2: list<i64> voters_outgoing,
    3: bool auto_leave,
}

struct SnapshotMetadata {
    1: ConfState conf_state,
    // lastIndex index of the last entry among all the entries saved in the snapshot
    2: i64 last_index,
    // lastTerm term of the last entry among all the entries saved in the snapshot
    3: i64 last_term,
}

struct Snapshot {
    1: SnapshotMetadata metadata,
    2: binary data,
}

enum MessageType {
    // Client messages:
    //
    Propose = 0,
    // Internal messages:
    //
    // Campaign will start a leader election
    Campaign = 1,
    // Beat will signal the leader to send AppendEntries without entries to all followers
    Beat = 2,
    StorageAppend = 3,
    StorageApply = 4,
    // RPC messages:
    //
    AppendEntries = 5,
    AppendEntriesResp = 6,
    RequestVote = 7,
    RequestVoteResp = 8,
    InstallSnapshot = 9,
    InstallSnapshotResp = 10,
}

// Message RPC messages that need to transport around peers
//
// AppendEntries RPC                 RequestVote RPC             InstallSnapshot RPC
// ================================= Arguments ======================================
// term                              term                        term
// leaderId                          candidateId                 leaderId
// prevLogIndex                      lastLogIndex                lastIncludedIndex
// prevLogTerm                       lastLogTerm                 lastIncludedTerm
// entries[]                                                     offset (not used)
// leaderCommit                                                  done (not used)
//                                                               data[]
// ================================= Results ========================================
// term                              term                        term
// success                           voteGranted
struct Message {
    1: MessageType type,
    2: i64 src,
    3: i64 dst,
    4: i64 term,
    5: i64 log_index,
    6: i64 log_term,
    7: list<Entry> entries,
    8: i64 leader_commit,
    9: Snapshot snapshot,
    10: bool reject,
}

enum ConfChangeType {
    AddNode = 0,
    RemoveNode = 1,
}

enum ConfChangeTransition {
    Auto = 0,
    JointImplicit = 1,
    JointExplicit = 2,
}

struct ConfChangeSingle {
    1: ConfChangeType type,
    2: i64 node_id,
    3: binary node_url,
}

struct ConfChange {
    1: ConfChangeTransition transaction,
    2: list<ConfChangeSingle> changes,
}