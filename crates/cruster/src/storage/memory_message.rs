use std::collections::HashMap;

use parking_lot::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::envelope::{AckChunk, EnvelopeRequest};
use crate::error::ClusterError;
use crate::message::ReplySender;
use crate::message_storage::{MessageStorage, SaveResult};
use crate::reply::{dead_letter_reply_id, ExitResult, Reply, ReplyWithExit, EXIT_SEQUENCE};
use crate::snowflake::Snowflake;
use crate::types::{RunnerAddress, ShardId};

/// In-memory message storage for testing.
pub struct MemoryMessageStorage {
    inner: Mutex<Inner>,
    /// Guard interval for `last_read` dedup. 0 = disabled.
    last_read_guard_interval: std::time::Duration,
}

struct Inner {
    /// All stored envelopes keyed by request_id.
    messages: HashMap<Snowflake, StoredMessage>,
    /// Replies keyed by request_id.
    replies: HashMap<Snowflake, Vec<Reply>>,
    /// Live reply handlers.
    reply_handlers: HashMap<Snowflake, ReplySender>,
    /// Maximum delivery attempts before dead-lettering. 0 = unlimited.
    max_retries: u32,
}

struct StoredMessage {
    envelope: EnvelopeRequest,
    processed: bool,
    last_read: Option<DateTime<Utc>>,
    retry_count: u32,
}

impl MemoryMessageStorage {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                messages: HashMap::new(),
                replies: HashMap::new(),
                reply_handlers: HashMap::new(),
                max_retries: 0,
            }),
            last_read_guard_interval: std::time::Duration::ZERO,
        }
    }

    /// Create a new in-memory message storage with a `last_read` guard interval.
    /// Messages read by `unprocessed_messages` will not be re-read until this
    /// interval has elapsed. Default (via `new()`): disabled (zero).
    pub fn with_last_read_guard_interval(interval: std::time::Duration) -> Self {
        Self {
            inner: Mutex::new(Inner {
                messages: HashMap::new(),
                replies: HashMap::new(),
                reply_handlers: HashMap::new(),
                max_retries: 0,
            }),
            last_read_guard_interval: interval,
        }
    }

    fn prune_closed_handlers(inner: &mut Inner) {
        inner
            .reply_handlers
            .retain(|_id, sender| !sender.is_closed());
    }
}

impl Default for MemoryMessageStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MessageStorage for MemoryMessageStorage {
    async fn save_request(&self, envelope: &EnvelopeRequest) -> Result<SaveResult, ClusterError> {
        let mut inner = self.inner.lock();
        if inner.messages.contains_key(&envelope.request_id) {
            let existing_reply = inner
                .replies
                .get(&envelope.request_id)
                .and_then(|rs| rs.first().cloned());
            return Ok(SaveResult::Duplicate { existing_reply });
        }
        inner.messages.insert(
            envelope.request_id,
            StoredMessage {
                envelope: envelope.clone(),
                processed: false,
                last_read: None,
                retry_count: 0,
            },
        );
        Ok(SaveResult::Success)
    }

    async fn save_envelope(&self, envelope: &EnvelopeRequest) -> Result<SaveResult, ClusterError> {
        let mut inner = self.inner.lock();
        if inner.messages.contains_key(&envelope.request_id) {
            return Ok(SaveResult::Duplicate {
                existing_reply: None,
            });
        }
        inner.messages.insert(
            envelope.request_id,
            StoredMessage {
                envelope: envelope.clone(),
                processed: false,
                last_read: None,
                retry_count: 0,
            },
        );
        Ok(SaveResult::Success)
    }

    async fn save_reply(&self, reply: &Reply) -> Result<(), ClusterError> {
        let mut inner = self.inner.lock();
        let request_id = reply.request_id();

        Self::prune_closed_handlers(&mut inner);

        // Store the reply.
        inner
            .replies
            .entry(request_id)
            .or_default()
            .push(reply.clone());

        // Mark message as processed if it's a final reply.
        if matches!(reply, Reply::WithExit(_)) {
            if let Some(msg) = inner.messages.get_mut(&request_id) {
                msg.processed = true;
            }
        }

        // Deliver to live handler if registered.
        let handler = if matches!(reply, Reply::WithExit(_)) {
            inner.reply_handlers.remove(&request_id)
        } else {
            inner.reply_handlers.get(&request_id).cloned()
        };
        if let Some(tx) = handler {
            if let Err(err) = tx.try_send(reply.clone()) {
                if matches!(err, tokio::sync::mpsc::error::TrySendError::Closed(_)) {
                    inner.reply_handlers.remove(&request_id);
                }
                tracing::debug!(request_id = %request_id.0, "reply handler send failed — channel full or closed");
            }
        }

        Ok(())
    }

    async fn clear_replies(&self, request_id: Snowflake) -> Result<(), ClusterError> {
        let mut inner = self.inner.lock();
        inner.replies.remove(&request_id);
        Ok(())
    }

    async fn replies_for(&self, request_id: Snowflake) -> Result<Vec<Reply>, ClusterError> {
        let inner = self.inner.lock();
        let mut replies = inner.replies.get(&request_id).cloned().unwrap_or_default();
        replies.sort_by_key(|reply| match reply {
            Reply::Chunk(chunk) => (0u8, chunk.sequence, chunk.id.0),
            Reply::WithExit(exit) => (1u8, EXIT_SEQUENCE, exit.id.0),
        });
        Ok(replies)
    }

    async fn unprocessed_messages(
        &self,
        shard_ids: &[ShardId],
    ) -> Result<Vec<EnvelopeRequest>, ClusterError> {
        let mut inner = self.inner.lock();
        Self::prune_closed_handlers(&mut inner);
        let now = Utc::now();
        let guard = self.last_read_guard_interval;
        let guard_chrono = chrono::Duration::from_std(guard).unwrap_or(chrono::TimeDelta::MAX);
        let max_retries = inner.max_retries;

        // Collect matching request IDs first, then update last_read.
        let matching_ids: Vec<Snowflake> = inner
            .messages
            .iter()
            .filter(|(_id, m)| {
                !m.processed
                    && shard_ids.contains(&m.envelope.address.shard_id)
                    && m.envelope.deliver_at.is_none_or(|at| at <= now)
                    && (guard.is_zero()
                        || m.last_read.is_none()
                        || m.last_read.is_some_and(|lr| now - lr >= guard_chrono))
            })
            .map(|(id, _)| *id)
            .collect();

        let mut result = Vec::with_capacity(matching_ids.len());
        for id in matching_ids {
            let (dead_letter, envelope) = if let Some(msg) = inner.messages.get_mut(&id) {
                msg.last_read = Some(now);
                msg.retry_count = msg.retry_count.saturating_add(1);
                let dead_letter =
                    max_retries > 0 && msg.retry_count.saturating_sub(1) > max_retries;
                if dead_letter {
                    msg.processed = true;
                    (true, None)
                } else {
                    (false, Some(msg.envelope.clone()))
                }
            } else {
                (false, None)
            };

            if dead_letter {
                let failure_reply = Reply::WithExit(ReplyWithExit {
                    request_id: id,
                    id: dead_letter_reply_id(id),
                    exit: ExitResult::Failure("max retries exceeded".to_string()),
                });
                inner
                    .replies
                    .entry(id)
                    .or_default()
                    .push(failure_reply.clone());
                if let Some(handler) = inner.reply_handlers.remove(&id) {
                    if handler.try_send(failure_reply).is_err() {
                        tracing::debug!(
                            request_id = %id.0,
                            "reply handler send failed — channel full or closed"
                        );
                    }
                }
            } else if let Some(envelope) = envelope {
                result.push(envelope);
            }
        }
        Ok(result)
    }

    async fn reset_shards(&self, shard_ids: &[ShardId]) -> Result<(), ClusterError> {
        let mut inner = self.inner.lock();
        // Collect request_ids that have exit replies so we can skip them.
        let has_exit: std::collections::HashSet<Snowflake> = inner
            .replies
            .iter()
            .filter(|(_, replies)| replies.iter().any(|r| matches!(r, Reply::WithExit(_))))
            .map(|(id, _)| *id)
            .collect();

        // Reset processed flag for messages in reassigned shards.
        // Note: In-flight messages on the old runner may be delivered twice.
        // This is expected at-least-once semantics during shard transitions.
        for msg in inner.messages.values_mut() {
            if shard_ids.contains(&msg.envelope.address.shard_id)
                && !has_exit.contains(&msg.envelope.request_id)
            {
                msg.processed = false;
                msg.last_read = None;
                msg.retry_count = 0;
            }
        }
        Ok(())
    }

    async fn clear_address(&self, _address: &RunnerAddress) -> Result<(), ClusterError> {
        // In-memory impl: clear everything (single-runner assumption for tests).
        // Also clears reply_handlers to avoid leaking ReplySender channels.
        let mut inner = self.inner.lock();
        inner.messages.clear();
        inner.replies.clear();
        inner.reply_handlers.clear();
        Ok(())
    }

    async fn unprocessed_messages_by_id(
        &self,
        request_ids: &[Snowflake],
    ) -> Result<Vec<EnvelopeRequest>, ClusterError> {
        let inner = self.inner.lock();
        let now = Utc::now();
        let result = request_ids
            .iter()
            .filter_map(|id| inner.messages.get(id))
            .filter(|m| !m.processed && m.envelope.deliver_at.is_none_or(|at| at <= now))
            .map(|m| m.envelope.clone())
            .collect();
        Ok(result)
    }

    fn register_reply_handler(&self, request_id: Snowflake, sender: ReplySender) {
        let mut inner = self.inner.lock();
        inner.reply_handlers.insert(request_id, sender);
    }

    fn unregister_reply_handler(&self, request_id: Snowflake) {
        let mut inner = self.inner.lock();
        inner.reply_handlers.remove(&request_id);
    }

    async fn ack_chunk(&self, ack: &AckChunk) -> Result<(), ClusterError> {
        let mut inner = self.inner.lock();
        if let Some(replies) = inner.replies.get_mut(&ack.request_id) {
            replies.retain(|reply| match reply {
                Reply::Chunk(chunk) => !(chunk.sequence == ack.sequence && chunk.id == ack.id),
                _ => true,
            });
            if replies.is_empty() {
                inner.replies.remove(&ack.request_id);
            }
        }
        Ok(())
    }

    fn set_max_retries(&self, max_retries: u32) {
        let mut inner = self.inner.lock();
        inner.max_retries = max_retries;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EntityAddress, EntityId, EntityType, ShardId};
    use std::collections::HashMap;

    fn make_envelope(request_id: i64, shard_id: i32) -> EnvelopeRequest {
        EnvelopeRequest {
            request_id: Snowflake(request_id),
            address: EntityAddress {
                shard_id: ShardId::new("default", shard_id),
                entity_type: EntityType::new("Test"),
                entity_id: EntityId::new("e-1"),
            },
            tag: "test".into(),
            payload: vec![],
            headers: HashMap::new(),
            span_id: None,
            trace_id: None,
            sampled: None,
            persisted: false,
            uninterruptible: Default::default(),
            deliver_at: None,
        }
    }

    fn make_exit_reply(request_id: i64, reply_id: i64) -> Reply {
        Reply::WithExit(crate::reply::ReplyWithExit {
            request_id: Snowflake(request_id),
            id: Snowflake(reply_id),
            exit: crate::reply::ExitResult::Success(vec![42]),
        })
    }

    #[tokio::test]
    async fn save_request_success_then_duplicate() {
        let storage = MemoryMessageStorage::new();
        let env = make_envelope(1, 0);

        let result = storage.save_request(&env).await.unwrap();
        assert!(matches!(result, SaveResult::Success));

        let result = storage.save_request(&env).await.unwrap();
        assert!(matches!(result, SaveResult::Duplicate { .. }));
    }

    #[tokio::test]
    async fn save_envelope_success_then_duplicate() {
        let storage = MemoryMessageStorage::new();
        let env = make_envelope(2, 0);

        let result = storage.save_envelope(&env).await.unwrap();
        assert!(matches!(result, SaveResult::Success));

        let result = storage.save_envelope(&env).await.unwrap();
        assert!(matches!(result, SaveResult::Duplicate { .. }));
    }

    #[tokio::test]
    async fn duplicate_returns_existing_reply() {
        let storage = MemoryMessageStorage::new();
        let env = make_envelope(3, 0);
        storage.save_request(&env).await.unwrap();

        let reply = make_exit_reply(3, 100);
        storage.save_reply(&reply).await.unwrap();

        let result = storage.save_request(&env).await.unwrap();
        match result {
            SaveResult::Duplicate { existing_reply } => {
                assert!(existing_reply.is_some());
            }
            _ => panic!("expected Duplicate"),
        }
    }

    #[tokio::test]
    async fn replies_for_roundtrip() {
        let storage = MemoryMessageStorage::new();
        let reply = make_exit_reply(10, 200);
        storage.save_reply(&reply).await.unwrap();

        let replies = storage.replies_for(Snowflake(10)).await.unwrap();
        assert_eq!(replies.len(), 1);

        let replies = storage.replies_for(Snowflake(999)).await.unwrap();
        assert!(replies.is_empty());
    }

    #[tokio::test]
    async fn replies_for_orders_exit_last() {
        let storage = MemoryMessageStorage::new();
        let request_id = Snowflake(404);
        let chunk_one = Reply::Chunk(crate::reply::ReplyChunk {
            request_id,
            id: Snowflake(501),
            sequence: 1,
            values: vec![vec![1]],
        });
        let chunk_zero = Reply::Chunk(crate::reply::ReplyChunk {
            request_id,
            id: Snowflake(500),
            sequence: 0,
            values: vec![vec![0]],
        });
        let exit = make_exit_reply(404, 999);

        storage.save_reply(&exit).await.unwrap();
        storage.save_reply(&chunk_one).await.unwrap();
        storage.save_reply(&chunk_zero).await.unwrap();

        let replies = storage.replies_for(request_id).await.unwrap();
        assert!(matches!(replies.last(), Some(Reply::WithExit(_))));
        let sequences: Vec<i32> = replies
            .iter()
            .filter_map(|reply| match reply {
                Reply::Chunk(chunk) => Some(chunk.sequence),
                Reply::WithExit(_) => None,
            })
            .collect();
        assert_eq!(sequences, vec![0, 1]);
    }

    #[tokio::test]
    async fn clear_replies() {
        let storage = MemoryMessageStorage::new();
        storage.save_reply(&make_exit_reply(20, 300)).await.unwrap();
        assert_eq!(storage.replies_for(Snowflake(20)).await.unwrap().len(), 1);

        storage.clear_replies(Snowflake(20)).await.unwrap();
        assert!(storage.replies_for(Snowflake(20)).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn ack_chunk_removes_chunk_reply() {
        let storage = MemoryMessageStorage::new();
        let request_id = Snowflake(30);
        let chunk = Reply::Chunk(crate::reply::ReplyChunk {
            request_id,
            id: Snowflake(301),
            sequence: 1,
            values: vec![vec![1]],
        });
        storage.save_reply(&chunk).await.unwrap();
        storage.save_reply(&make_exit_reply(30, 302)).await.unwrap();

        let ack = AckChunk {
            request_id,
            id: Snowflake(301),
            sequence: 1,
        };
        storage.ack_chunk(&ack).await.unwrap();

        let replies = storage.replies_for(request_id).await.unwrap();
        assert_eq!(replies.len(), 1);
        assert!(matches!(replies[0], Reply::WithExit(_)));
    }

    #[tokio::test]
    async fn unprocessed_messages_filters_by_shard() {
        let storage = MemoryMessageStorage::new();
        storage.save_request(&make_envelope(100, 1)).await.unwrap();
        storage.save_request(&make_envelope(101, 2)).await.unwrap();
        storage.save_request(&make_envelope(102, 1)).await.unwrap();

        let shard1 = ShardId::new("default", 1);
        let msgs = storage.unprocessed_messages(&[shard1]).await.unwrap();
        assert_eq!(msgs.len(), 2);
    }

    #[tokio::test]
    async fn processed_messages_not_returned() {
        let storage = MemoryMessageStorage::new();
        storage.save_request(&make_envelope(200, 1)).await.unwrap();

        // Saving a WithExit reply marks it as processed.
        storage
            .save_reply(&make_exit_reply(200, 500))
            .await
            .unwrap();

        let shard1 = ShardId::new("default", 1);
        let msgs = storage.unprocessed_messages(&[shard1]).await.unwrap();
        assert!(msgs.is_empty());
    }

    #[tokio::test]
    async fn reset_shards_marks_unprocessed() {
        let storage = MemoryMessageStorage::new();
        // Save a message without any exit reply (simulate in-progress message).
        storage.save_request(&make_envelope(300, 1)).await.unwrap();

        // Mark as processed by saving a chunk reply (not an exit reply).
        let chunk_reply = Reply::Chunk(crate::reply::ReplyChunk {
            request_id: Snowflake(300),
            id: Snowflake(600),
            sequence: 0,
            values: vec![vec![1]],
        });
        storage.save_reply(&chunk_reply).await.unwrap();

        // Manually mark as processed by saving and consuming.
        // The message is still unprocessed since chunk replies don't mark processed.
        let shard1 = ShardId::new("default", 1);
        let msgs = storage
            .unprocessed_messages(std::slice::from_ref(&shard1))
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        // Now clear and re-test with a fresh message that we manually process.
        // Save a new message, mark it processed via exit reply, then reset.
        // Messages with exit replies should NOT be reset.
        storage.save_request(&make_envelope(301, 1)).await.unwrap();
        storage
            .save_reply(&make_exit_reply(301, 601))
            .await
            .unwrap();

        // 301 is processed (has exit reply), 300 is unprocessed.
        let msgs = storage
            .unprocessed_messages(std::slice::from_ref(&shard1))
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].request_id, Snowflake(300));

        // reset_shards should not reset 301 (has exit reply).
        storage
            .reset_shards(std::slice::from_ref(&shard1))
            .await
            .unwrap();
        let msgs = storage.unprocessed_messages(&[shard1]).await.unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].request_id, Snowflake(300));
    }

    #[tokio::test]
    async fn reset_shards_skips_already_replied_messages() {
        let storage = MemoryMessageStorage::new();

        // Message with exit reply — should NOT be reset.
        storage.save_request(&make_envelope(310, 1)).await.unwrap();
        storage
            .save_reply(&make_exit_reply(310, 610))
            .await
            .unwrap();

        // Message without exit reply — should be reset.
        storage.save_request(&make_envelope(311, 1)).await.unwrap();

        let shard1 = ShardId::new("default", 1);

        // Only 311 is unprocessed.
        let msgs = storage
            .unprocessed_messages(std::slice::from_ref(&shard1))
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].request_id, Snowflake(311));

        // Reset shards.
        storage
            .reset_shards(std::slice::from_ref(&shard1))
            .await
            .unwrap();

        // Still only 311 — 310 was not reset because it has an exit reply.
        let msgs = storage.unprocessed_messages(&[shard1]).await.unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].request_id, Snowflake(311));
    }

    #[tokio::test]
    async fn clear_address_removes_all() {
        let storage = MemoryMessageStorage::new();
        storage.save_request(&make_envelope(400, 1)).await.unwrap();
        storage
            .save_reply(&make_exit_reply(400, 700))
            .await
            .unwrap();

        let addr = RunnerAddress::new("127.0.0.1", 9000);
        storage.clear_address(&addr).await.unwrap();

        let shard1 = ShardId::new("default", 1);
        assert!(storage
            .unprocessed_messages(&[shard1])
            .await
            .unwrap()
            .is_empty());
        assert!(storage
            .replies_for(Snowflake(400))
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn reply_handler_delivers_in_realtime() {
        let storage = MemoryMessageStorage::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);

        storage.register_reply_handler(Snowflake(500), tx);

        let reply = make_exit_reply(500, 800);
        storage.save_reply(&reply).await.unwrap();

        let received = rx.try_recv().unwrap();
        assert_eq!(received.request_id(), Snowflake(500));
    }

    #[tokio::test]
    async fn save_reply_removes_handler_on_exit() {
        let storage = MemoryMessageStorage::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);

        storage.register_reply_handler(Snowflake(520), tx);

        let reply = make_exit_reply(520, 820);
        storage.save_reply(&reply).await.unwrap();

        let received = rx.try_recv().unwrap();
        assert_eq!(received.request_id(), Snowflake(520));

        let inner = storage.inner.lock();
        assert!(inner.reply_handlers.is_empty());
    }

    #[tokio::test]
    async fn save_reply_prunes_closed_handler() {
        let storage = MemoryMessageStorage::new();
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        drop(rx);

        storage.register_reply_handler(Snowflake(550), tx);
        storage
            .save_reply(&make_exit_reply(550, 801))
            .await
            .unwrap();

        let inner = storage.inner.lock();
        assert!(inner.reply_handlers.is_empty());
    }

    #[tokio::test]
    async fn unregister_reply_handler_stops_delivery() {
        let storage = MemoryMessageStorage::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);

        storage.register_reply_handler(Snowflake(600), tx);
        storage.unregister_reply_handler(Snowflake(600));

        storage
            .save_reply(&make_exit_reply(600, 900))
            .await
            .unwrap();
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn last_read_guard_prevents_redelivery() {
        let storage = MemoryMessageStorage::with_last_read_guard_interval(
            std::time::Duration::from_secs(600),
        );
        let env = make_envelope(800, 1);
        storage.save_request(&env).await.unwrap();

        let shard1 = ShardId::new("default", 1);

        // First poll returns the message.
        let msgs = storage
            .unprocessed_messages(std::slice::from_ref(&shard1))
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        // Second poll within guard interval returns empty (message was just read).
        let msgs = storage
            .unprocessed_messages(std::slice::from_ref(&shard1))
            .await
            .unwrap();
        assert_eq!(msgs.len(), 0, "last_read guard should prevent re-read");
    }

    #[tokio::test]
    async fn last_read_guard_disabled_by_default() {
        let storage = MemoryMessageStorage::new();
        let env = make_envelope(810, 1);
        storage.save_request(&env).await.unwrap();

        let shard1 = ShardId::new("default", 1);

        // Both polls return the message when guard is disabled.
        let msgs = storage
            .unprocessed_messages(std::slice::from_ref(&shard1))
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);
        let msgs = storage
            .unprocessed_messages(std::slice::from_ref(&shard1))
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);
    }

    #[tokio::test]
    async fn max_retries_dead_letters_message() {
        let storage = MemoryMessageStorage::new();
        storage.set_max_retries(1);
        storage.save_request(&make_envelope(900, 1)).await.unwrap();

        let shard1 = ShardId::new("default", 1);

        let msgs = storage
            .unprocessed_messages(std::slice::from_ref(&shard1))
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        let msgs = storage
            .unprocessed_messages(std::slice::from_ref(&shard1))
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        let msgs = storage
            .unprocessed_messages(std::slice::from_ref(&shard1))
            .await
            .unwrap();
        assert!(msgs.is_empty());

        let replies = storage.replies_for(Snowflake(900)).await.unwrap();
        assert_eq!(replies.len(), 1);
        match &replies[0] {
            Reply::WithExit(reply) => {
                assert_eq!(reply.id, dead_letter_reply_id(Snowflake(900)));
                match &reply.exit {
                    ExitResult::Failure(reason) => {
                        assert_eq!(reason, "max retries exceeded");
                    }
                    _ => panic!("expected failure exit"),
                }
            }
            _ => panic!("expected exit reply"),
        }
    }

    #[tokio::test]
    async fn reset_shards_clears_last_read() {
        let storage = MemoryMessageStorage::with_last_read_guard_interval(
            std::time::Duration::from_secs(600),
        );
        let env = make_envelope(820, 1);
        storage.save_request(&env).await.unwrap();

        let shard1 = ShardId::new("default", 1);

        // Read once to set last_read.
        let msgs = storage
            .unprocessed_messages(std::slice::from_ref(&shard1))
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);

        // Guard prevents re-read.
        let msgs = storage
            .unprocessed_messages(std::slice::from_ref(&shard1))
            .await
            .unwrap();
        assert_eq!(msgs.len(), 0);

        // Reset clears last_read.
        storage
            .reset_shards(std::slice::from_ref(&shard1))
            .await
            .unwrap();

        // After reset, message is re-readable.
        let msgs = storage
            .unprocessed_messages(std::slice::from_ref(&shard1))
            .await
            .unwrap();
        assert_eq!(msgs.len(), 1);
    }

    #[tokio::test]
    async fn unprocessed_messages_filters_by_deliver_at() {
        use chrono::{Duration, Utc};

        let storage = MemoryMessageStorage::new();

        // Message with deliver_at in the future — should be excluded.
        let mut future_env = make_envelope(700, 1);
        future_env.deliver_at = Some(Utc::now() + Duration::hours(1));
        storage.save_request(&future_env).await.unwrap();

        // Message with deliver_at in the past — should be included.
        let mut past_env = make_envelope(701, 1);
        past_env.deliver_at = Some(Utc::now() - Duration::hours(1));
        storage.save_request(&past_env).await.unwrap();

        // Message with no deliver_at — should be included.
        let no_deliver = make_envelope(702, 1);
        storage.save_request(&no_deliver).await.unwrap();

        let shard1 = ShardId::new("default", 1);
        let msgs = storage.unprocessed_messages(&[shard1]).await.unwrap();
        assert_eq!(msgs.len(), 2);
        let ids: Vec<_> = msgs.iter().map(|m| m.request_id.0).collect();
        assert!(ids.contains(&701));
        assert!(ids.contains(&702));
        assert!(!ids.contains(&700));
    }
}
