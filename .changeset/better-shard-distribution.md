---
default: patch
---

# Improved shard distribution balance in rendezvous hashing

Fixes imbalanced shard distribution in rendezvous hashing by switching to a higher-quality hash function.

## Problem

The previous implementation used DJB2 hash with XOR combination, which produced poor distribution across nodes. In production with 3 nodes and 2048 shards, this resulted in up to 11% imbalance (731 vs 655 shards per node).

## Solution

- Added `hash64()` function based on xxHash algorithm with excellent avalanche properties
- Changed `compute_runner_score()` to hash the concatenated key directly instead of XOR-ing separate hashes

## Results (3 nodes, 2048 shards)

| Metric | Before | After |
|--------|--------|-------|
| Distribution | 731, 662, 655 | 689, 683, 676 |
| Max difference | 76 shards | 13 shards |
| Imbalance | 11.1% | 1.9% |

This achieves ~6x better balance with no performance impact.
