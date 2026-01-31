use cruster::envelope::EnvelopeRequest;
use cruster::message_storage::MessageStorage;
use cruster::reply::{dead_letter_reply_id, ExitResult, Reply};
use cruster::snowflake::Snowflake;
use cruster::storage::memory_message::MemoryMessageStorage;
use cruster::types::{EntityAddress, EntityId, EntityType, ShardId};
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
        payload: Vec::new(),
        headers: HashMap::new(),
        span_id: None,
        trace_id: None,
        sampled: None,
        persisted: false,
        uninterruptible: Default::default(),
        deliver_at: None,
    }
}

#[tokio::test]
async fn storage_retry_exhaustion_dead_letters_message() {
    let storage = MemoryMessageStorage::new();
    storage.set_max_retries(1);

    let request_id_value = 9000;
    storage
        .save_request(&make_envelope(request_id_value, 1))
        .await
        .unwrap();
    let request_id = Snowflake(request_id_value);
    let shard = ShardId::new("default", 1);

    for _ in 0..2 {
        let messages = storage
            .unprocessed_messages(std::slice::from_ref(&shard))
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].request_id, request_id);
    }

    let messages = storage
        .unprocessed_messages(std::slice::from_ref(&shard))
        .await
        .unwrap();
    assert!(messages.is_empty());

    let replies = storage.replies_for(request_id).await.unwrap();
    assert_eq!(replies.len(), 1);
    match &replies[0] {
        Reply::WithExit(reply) => {
            assert_eq!(reply.id, dead_letter_reply_id(request_id));
            match &reply.exit {
                ExitResult::Failure(reason) => assert_eq!(reason, "max retries exceeded"),
                _ => panic!("expected failure exit"),
            }
        }
        _ => panic!("expected exit reply"),
    }
}
