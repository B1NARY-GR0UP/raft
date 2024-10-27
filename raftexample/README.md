# raftexample

raftexample is an example usage of raft-foiver. It provides a simple REST API for a key-value store cluster backed by the Raft Consensus Algorithm. Inspired by [etcd/contrib/raftexample](https://github.com/etcd-io/etcd/tree/main/contrib/raftexample).

## Usage

### Build

Clone [raft-foiver](https://github.com/B1NARY-GR0UP/raft).

```shell
cd raftexample
go build -o raftexample
```

### Start a Three-Node Cluster

1. Start Cluster

```shell
goreman start
```

2. Add Key-Value

- key: hello
- value: world

```shell
curl -L http://127.0.0.1:9999/hello -X POST -d world
```

3. Get Key-Value

```shell
curl -L http://127.0.0.1:9999/hello
```

### Fault Tolerance

1. Start Cluster and Add Key-Value (hello:world)

```shell
goreman start
curl -L http://127.0.0.1:9999/hello -X POST -d world
```

2. Stop a Node and Update Key-Value (hello:foiver)

```shell
goreman run stop raftexample3
curl -L http://127.0.0.1:7777/hello -X POST -d foiver
curl -L http://127.0.0.1:7777/hello
```

3. Restart Node and Check Key-Value (hello:foiver)

```shell
goreman run start raftexample3
curl -L http://127.0.0.1:9999/hello
```

### Dynamic Cluster Reconfiguration

1. Start Three-Node Cluster

```shell
goreman start
```

2. Add New Node to Cluster

```shell
curl -L http://127.0.0.1:9999/4 -X PUT -d localhost:6000
raftexample --id 4 --addr localhost:6000 --peerids 1,2,3 --peeraddrs localhost:7000,localhost:8000,localhost:9000 --port 6666 --join
```

3. Test Cluster

```shell
curl -L http://127.0.0.1:9999/hello -X POST -d world
curl -L http://127.0.0.1:6666/hello
```

4. Remove Node from Cluster

```shell
curl -L http://127.0.0.1:9999/2 -X DELETE
```