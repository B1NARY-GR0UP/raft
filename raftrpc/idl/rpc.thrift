namespace go raft

include "../../raftthrift/raft.thrift"

enum Code {
    Accept = 0,
    Reject = 1,
    Timeout = 2,
}

struct DummyResp {
    1: Code code,
}

service RPC {
    DummyResp Do(1: raft.Message req),
}