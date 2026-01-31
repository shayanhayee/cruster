use std::collections::HashMap;
use std::pin::Pin;

use parking_lot::Mutex;

use async_trait::async_trait;
use futures::Stream;

use crate::error::ClusterError;
use crate::runner::Runner;
use crate::runner_storage::{BatchAcquireResult, BatchRefreshResult, RunnerStorage};
use crate::types::{MachineId, RunnerAddress, ShardId};

/// In-memory runner storage for testing.
pub struct MemoryRunnerStorage {
    inner: Mutex<Inner>,
}

struct Inner {
    runners: Vec<Runner>,
    next_machine_id: i32,
    /// Tracks assigned machine IDs per runner address for stable re-registration.
    machine_ids: HashMap<RunnerAddress, MachineId>,
    /// Shard locks: shard_id -> runner address that holds the lock.
    shard_locks: HashMap<ShardId, RunnerAddress>,
    /// Watchers to notify on runner changes.
    watchers: Vec<tokio::sync::mpsc::UnboundedSender<Vec<Runner>>>,
}

impl MemoryRunnerStorage {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                runners: Vec::new(),
                next_machine_id: 1,
                machine_ids: HashMap::new(),
                shard_locks: HashMap::new(),
                watchers: Vec::new(),
            }),
        }
    }

    fn notify_watchers(inner: &mut Inner) {
        let runners = inner.runners.clone();
        inner.watchers.retain(|tx| tx.send(runners.clone()).is_ok());
    }
}

impl Default for MemoryRunnerStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RunnerStorage for MemoryRunnerStorage {
    async fn register(&self, runner: &Runner) -> Result<MachineId, ClusterError> {
        let mut inner = self.inner.lock();
        // Check if already registered.
        if let Some(existing) = inner
            .runners
            .iter()
            .position(|r| r.address == runner.address)
        {
            inner.runners[existing] = runner.clone();
        } else {
            inner.runners.push(runner.clone());
        }
        // Return the previously assigned machine ID on re-registration to avoid
        // snowflake ID collisions when the generator is updated with a new ID.
        let id = if let Some(&existing_id) = inner.machine_ids.get(&runner.address) {
            existing_id
        } else {
            let id = MachineId::wrapping(inner.next_machine_id);
            inner.next_machine_id += 1;
            inner.machine_ids.insert(runner.address.clone(), id);
            id
        };
        Self::notify_watchers(&mut inner);
        Ok(id)
    }

    async fn unregister(&self, address: &RunnerAddress) -> Result<(), ClusterError> {
        let mut inner = self.inner.lock();
        inner.runners.retain(|r| &r.address != address);
        // Release all shard locks held by this runner.
        inner.shard_locks.retain(|_, v| v != address);
        Self::notify_watchers(&mut inner);
        Ok(())
    }

    async fn get_runners(&self) -> Result<Vec<Runner>, ClusterError> {
        let inner = self.inner.lock();
        Ok(inner.runners.clone())
    }

    async fn set_runner_health(
        &self,
        address: &RunnerAddress,
        healthy: bool,
    ) -> Result<(), ClusterError> {
        let mut inner = self.inner.lock();
        if let Some(runner) = inner.runners.iter_mut().find(|r| &r.address == address) {
            runner.healthy = healthy;
        }
        Ok(())
    }

    async fn acquire(
        &self,
        shard_id: &ShardId,
        runner: &RunnerAddress,
    ) -> Result<bool, ClusterError> {
        let mut inner = self.inner.lock();
        if let Some(holder) = inner.shard_locks.get(shard_id) {
            if holder == runner {
                return Ok(true); // Already held by this runner.
            }
            return Ok(false); // Held by another runner.
        }
        inner.shard_locks.insert(shard_id.clone(), runner.clone());
        Ok(true)
    }

    async fn acquire_batch(
        &self,
        shard_ids: &[ShardId],
        runner: &RunnerAddress,
    ) -> Result<BatchAcquireResult, ClusterError> {
        let mut inner = self.inner.lock();
        let mut acquired = Vec::new();
        for shard_id in shard_ids {
            if let Some(holder) = inner.shard_locks.get(shard_id) {
                if holder == runner {
                    acquired.push(shard_id.clone());
                }
                // Held by another runner — skip
            } else {
                inner.shard_locks.insert(shard_id.clone(), runner.clone());
                acquired.push(shard_id.clone());
            }
        }
        Ok(BatchAcquireResult {
            acquired,
            failures: Vec::new(),
        })
    }

    async fn refresh(
        &self,
        shard_id: &ShardId,
        runner: &RunnerAddress,
    ) -> Result<bool, ClusterError> {
        let inner = self.inner.lock();
        Ok(inner.shard_locks.get(shard_id) == Some(runner))
    }

    async fn refresh_batch(
        &self,
        shard_ids: &[ShardId],
        runner: &RunnerAddress,
    ) -> Result<BatchRefreshResult, ClusterError> {
        let inner = self.inner.lock();
        let mut refreshed = Vec::new();
        let mut lost = Vec::new();
        for shard_id in shard_ids {
            if inner.shard_locks.get(shard_id) == Some(runner) {
                refreshed.push(shard_id.clone());
            } else {
                lost.push(shard_id.clone());
            }
        }
        Ok(BatchRefreshResult {
            refreshed,
            lost,
            failures: Vec::new(),
        })
    }

    async fn release(
        &self,
        shard_id: &ShardId,
        runner: &RunnerAddress,
    ) -> Result<(), ClusterError> {
        let mut inner = self.inner.lock();
        if inner.shard_locks.get(shard_id) == Some(runner) {
            inner.shard_locks.remove(shard_id);
        }
        Ok(())
    }

    async fn release_all(&self, runner: &RunnerAddress) -> Result<(), ClusterError> {
        let mut inner = self.inner.lock();
        inner.shard_locks.retain(|_, v| v != runner);
        Ok(())
    }

    async fn watch_runners(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Vec<Runner>> + Send>>, ClusterError> {
        let mut inner = self.inner.lock();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        inner.watchers.push(tx);
        Ok(Box::pin(
            tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::RunnerAddress;
    use tokio_stream::StreamExt;

    fn addr(port: u16) -> RunnerAddress {
        RunnerAddress::new("127.0.0.1", port)
    }

    fn runner(port: u16) -> Runner {
        Runner::new(addr(port), 1)
    }

    #[tokio::test]
    async fn register_returns_unique_machine_ids() {
        let storage = MemoryRunnerStorage::new();
        let id1 = storage.register(&runner(9001)).await.unwrap();
        let id2 = storage.register(&runner(9002)).await.unwrap();
        assert_ne!(id1, id2);
    }

    #[tokio::test]
    async fn get_runners_returns_all() {
        let storage = MemoryRunnerStorage::new();
        storage.register(&runner(9001)).await.unwrap();
        storage.register(&runner(9002)).await.unwrap();
        let runners = storage.get_runners().await.unwrap();
        assert_eq!(runners.len(), 2);
    }

    #[tokio::test]
    async fn unregister_removes_runner() {
        let storage = MemoryRunnerStorage::new();
        storage.register(&runner(9001)).await.unwrap();
        storage.unregister(&addr(9001)).await.unwrap();
        assert!(storage.get_runners().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn acquire_shard_lock() {
        let storage = MemoryRunnerStorage::new();
        let shard = ShardId::new("default", 0);

        // First runner acquires.
        assert!(storage.acquire(&shard, &addr(9001)).await.unwrap());
        // Same runner re-acquires (idempotent).
        assert!(storage.acquire(&shard, &addr(9001)).await.unwrap());
        // Different runner fails.
        assert!(!storage.acquire(&shard, &addr(9002)).await.unwrap());
    }

    #[tokio::test]
    async fn refresh_shard_lock() {
        let storage = MemoryRunnerStorage::new();
        let shard = ShardId::new("default", 0);

        storage.acquire(&shard, &addr(9001)).await.unwrap();
        assert!(storage.refresh(&shard, &addr(9001)).await.unwrap());
        assert!(!storage.refresh(&shard, &addr(9002)).await.unwrap());
    }

    #[tokio::test]
    async fn release_allows_reacquisition() {
        let storage = MemoryRunnerStorage::new();
        let shard = ShardId::new("default", 0);

        storage.acquire(&shard, &addr(9001)).await.unwrap();
        storage.release(&shard, &addr(9001)).await.unwrap();
        assert!(storage.acquire(&shard, &addr(9002)).await.unwrap());
    }

    #[tokio::test]
    async fn release_all_frees_locks() {
        let storage = MemoryRunnerStorage::new();
        let s1 = ShardId::new("default", 0);
        let s2 = ShardId::new("default", 1);

        storage.acquire(&s1, &addr(9001)).await.unwrap();
        storage.acquire(&s2, &addr(9001)).await.unwrap();
        storage.release_all(&addr(9001)).await.unwrap();

        assert!(storage.acquire(&s1, &addr(9002)).await.unwrap());
        assert!(storage.acquire(&s2, &addr(9002)).await.unwrap());
    }

    #[tokio::test]
    async fn set_runner_health() {
        let storage = MemoryRunnerStorage::new();
        storage.register(&runner(9001)).await.unwrap();
        storage.set_runner_health(&addr(9001), false).await.unwrap();

        let runners = storage.get_runners().await.unwrap();
        assert!(!runners[0].healthy);
    }

    #[tokio::test]
    async fn watch_runners_emits_on_register() {
        let storage = MemoryRunnerStorage::new();
        let mut stream = storage.watch_runners().await.unwrap();

        storage.register(&runner(9001)).await.unwrap();

        let runners = stream.next().await.unwrap();
        assert_eq!(runners.len(), 1);
    }

    #[tokio::test]
    async fn reregister_returns_same_machine_id() {
        let storage = MemoryRunnerStorage::new();
        let id1 = storage.register(&runner(9001)).await.unwrap();
        let id2 = storage.register(&runner(9001)).await.unwrap();
        assert_eq!(
            id1, id2,
            "re-registration should return the same machine ID"
        );
        // Different runner still gets a different ID.
        let id3 = storage.register(&runner(9002)).await.unwrap();
        assert_ne!(id1, id3);
    }

    #[tokio::test]
    async fn acquire_batch_acquires_multiple_shards() {
        let storage = MemoryRunnerStorage::new();
        let shards = vec![
            ShardId::new("default", 0),
            ShardId::new("default", 1),
            ShardId::new("default", 2),
        ];

        let result = storage.acquire_batch(&shards, &addr(9001)).await.unwrap();
        assert_eq!(result.acquired.len(), 3);
        assert!(result.failures.is_empty());

        // All shards are now held by runner 9001
        for shard in &shards {
            assert!(storage.refresh(shard, &addr(9001)).await.unwrap());
        }
    }

    #[tokio::test]
    async fn acquire_batch_skips_held_by_other() {
        let storage = MemoryRunnerStorage::new();
        let s0 = ShardId::new("default", 0);
        let s1 = ShardId::new("default", 1);
        let s2 = ShardId::new("default", 2);

        // Runner 9002 holds shard 1
        storage.acquire(&s1, &addr(9002)).await.unwrap();

        let shards = vec![s0.clone(), s1.clone(), s2.clone()];
        let result = storage.acquire_batch(&shards, &addr(9001)).await.unwrap();

        // Should acquire 0 and 2, skip 1
        assert_eq!(result.acquired.len(), 2);
        assert!(result.acquired.contains(&s0));
        assert!(result.acquired.contains(&s2));
        assert!(!result.acquired.contains(&s1));
        assert!(result.failures.is_empty());
    }

    #[tokio::test]
    async fn acquire_batch_idempotent_for_same_runner() {
        let storage = MemoryRunnerStorage::new();
        let shards = vec![ShardId::new("default", 0), ShardId::new("default", 1)];

        // Acquire once
        let first = storage.acquire_batch(&shards, &addr(9001)).await.unwrap();
        assert_eq!(first.acquired.len(), 2);

        // Acquire again — idempotent
        let second = storage.acquire_batch(&shards, &addr(9001)).await.unwrap();
        assert_eq!(second.acquired.len(), 2);
    }

    #[tokio::test]
    async fn acquire_batch_empty_input() {
        let storage = MemoryRunnerStorage::new();
        let result = storage.acquire_batch(&[], &addr(9001)).await.unwrap();
        assert!(result.acquired.is_empty());
        assert!(result.failures.is_empty());
    }

    #[tokio::test]
    async fn refresh_batch_refreshes_owned_shards() {
        let storage = MemoryRunnerStorage::new();
        let s0 = ShardId::new("default", 0);
        let s1 = ShardId::new("default", 1);
        let s2 = ShardId::new("default", 2);

        storage.acquire(&s0, &addr(9001)).await.unwrap();
        storage.acquire(&s1, &addr(9001)).await.unwrap();
        // s2 held by another runner
        storage.acquire(&s2, &addr(9002)).await.unwrap();

        let shards = vec![s0.clone(), s1.clone(), s2.clone()];
        let result = storage.refresh_batch(&shards, &addr(9001)).await.unwrap();

        assert_eq!(result.refreshed.len(), 2);
        assert!(result.refreshed.contains(&s0));
        assert!(result.refreshed.contains(&s1));
        assert_eq!(result.lost.len(), 1);
        assert!(result.lost.contains(&s2));
        assert!(result.failures.is_empty());
    }

    #[tokio::test]
    async fn refresh_batch_empty_input() {
        let storage = MemoryRunnerStorage::new();
        let result = storage.refresh_batch(&[], &addr(9001)).await.unwrap();
        assert!(result.refreshed.is_empty());
        assert!(result.lost.is_empty());
        assert!(result.failures.is_empty());
    }

    #[tokio::test]
    async fn unregister_releases_shard_locks() {
        let storage = MemoryRunnerStorage::new();
        let shard = ShardId::new("default", 0);

        storage.register(&runner(9001)).await.unwrap();
        storage.acquire(&shard, &addr(9001)).await.unwrap();
        storage.unregister(&addr(9001)).await.unwrap();

        // Another runner can now acquire.
        assert!(storage.acquire(&shard, &addr(9002)).await.unwrap());
    }
}
