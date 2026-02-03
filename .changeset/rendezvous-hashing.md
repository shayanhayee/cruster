---
default: minor
---

# Configurable shard assignment with rendezvous hashing

Implements configurable shard assignment strategies for better distribution and performance.

## New Features

### ShardAssignmentStrategy enum

Choose between different shard assignment algorithms:

- **Rendezvous** (default): Near-perfect distribution with O(shards × nodes) complexity
- **RendezvousParallel**: Same as Rendezvous but parallelized with Rayon (8x faster for 100+ nodes)
- **ConsistentHash**: Ring-based consistent hashing with configurable vnodes (requires `consistent-hash` feature)

### Benchmark Results (2048 shards, 100 nodes)

| Strategy | Time |
|----------|------|
| RendezvousParallel | 1.35 ms |
| Rendezvous | 10.9 ms |
| ConsistentHash | 175 ms |

## Configuration

```rust
use cruster::shard_assigner::ShardAssignmentStrategy;
use cruster::config::ShardingConfig;

let config = ShardingConfig {
    assignment_strategy: ShardAssignmentStrategy::RendezvousParallel,
    // ...
};
```

## Feature Flags

- `parallel`: Enables `RendezvousParallel` strategy (requires `rayon`)
- `consistent-hash`: Enables `ConsistentHash` strategy (requires `hashring`)
