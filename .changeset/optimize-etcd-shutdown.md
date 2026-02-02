---
default: patch
---

fix: optimize etcd shutdown from O(shards) to O(1) by revoking lease

Previously `release_all()` deleted shard lock keys one-by-one with 2048 sequential etcd transactions, taking ~34 seconds. Now we simply revoke the runner's lease, which atomically deletes all attached keys (runner registration + all shard locks) in a single O(1) operation, reducing shutdown to milliseconds.
