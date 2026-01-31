/// DJB2 hash function for consistent shard assignment.
/// Produces a deterministic hash for any byte slice.
pub fn djb2_hash(bytes: &[u8]) -> u32 {
    let mut hash: u32 = 5381;
    for &b in bytes {
        hash = hash.wrapping_mul(33).wrapping_add(b as u32);
    }
    hash
}

/// Produces a deterministic 64-bit hash for any byte slice.
pub fn djb2_hash64(bytes: &[u8]) -> u64 {
    djb2_hash64_with_seed(5381, bytes)
}

/// Produces a deterministic 64-bit hash seeded with the provided value.
pub fn djb2_hash64_with_seed(seed: u64, bytes: &[u8]) -> u64 {
    let mut hash = seed;
    for &b in bytes {
        hash = hash.wrapping_mul(33).wrapping_add(b as u64);
    }
    hash
}

/// Compute the shard index for an entity ID within a group.
///
/// Returns a 0-indexed shard index in `[0, shards_per_group)`.
///
/// **Note:** The TypeScript implementation uses 1-indexed shards (`hash % N + 1`).
/// This is intentional — wire compatibility with the TS implementation is a non-goal.
///
/// # Panics
///
/// Panics if `shards_per_group` is less than 1.
pub fn shard_for_entity(entity_id: &str, shards_per_group: i32) -> i32 {
    assert!(
        shards_per_group >= 1,
        "shards_per_group must be >= 1, got {shards_per_group}"
    );
    (djb2_hash(entity_id.as_bytes()) % shards_per_group as u32) as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic() {
        let h1 = djb2_hash(b"hello");
        let h2 = djb2_hash(b"hello");
        assert_eq!(h1, h2);
    }

    #[test]
    fn different_inputs_differ() {
        let h1 = djb2_hash(b"hello");
        let h2 = djb2_hash(b"world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn djb2_hash64_deterministic() {
        let h1 = djb2_hash64(b"hello");
        let h2 = djb2_hash64(b"hello");
        assert_eq!(h1, h2);
    }

    #[test]
    fn djb2_hash64_seeded_changes_output() {
        let h1 = djb2_hash64_with_seed(5381, b"hello");
        let h2 = djb2_hash64_with_seed(5382, b"hello");
        assert_ne!(h1, h2);
    }

    #[test]
    fn distribution() {
        let num_shards = 300;
        let num_keys = 10_000;
        let mut counts = vec![0u32; num_shards as usize];

        for i in 0..num_keys {
            let key = format!("entity-{i}");
            let shard = shard_for_entity(&key, num_shards);
            counts[shard as usize] += 1;
        }

        let expected = num_keys as f64 / num_shards as f64;
        let max_allowed = (expected * 2.0) as u32;
        for (i, &count) in counts.iter().enumerate() {
            assert!(
                count <= max_allowed,
                "shard {i} has {count} entities, expected at most {max_allowed}"
            );
        }
    }

    #[test]
    #[should_panic(expected = "shards_per_group must be >= 1")]
    fn shard_for_entity_zero_shards_panics() {
        shard_for_entity("test", 0);
    }

    #[test]
    #[should_panic(expected = "shards_per_group must be >= 1")]
    fn shard_for_entity_negative_shards_panics() {
        shard_for_entity("test", -1);
    }

    #[test]
    fn shard_for_entity_in_range() {
        for i in 0..1000 {
            let shard = shard_for_entity(&format!("id-{i}"), 300);
            assert!((0..300).contains(&shard));
        }
    }
}
