use cruster::message_storage::MessageStorage;
use cruster::reply::{ExitResult, Reply, ReplyChunk, ReplyWithExit};
use cruster::snowflake::Snowflake;
use cruster::storage::memory_message::MemoryMessageStorage;

#[tokio::test]
async fn persisted_chunk_replay_orders_by_sequence() {
    let storage = MemoryMessageStorage::new();
    let request_id = Snowflake(1010);

    let chunk_two = Reply::Chunk(ReplyChunk {
        request_id,
        id: Snowflake(20),
        sequence: 2,
        values: vec![rmp_serde::to_vec(&2i32).unwrap()],
    });
    let chunk_one = Reply::Chunk(ReplyChunk {
        request_id,
        id: Snowflake(21),
        sequence: 1,
        values: vec![rmp_serde::to_vec(&1i32).unwrap()],
    });
    let exit = Reply::WithExit(ReplyWithExit {
        request_id,
        id: Snowflake(22),
        exit: ExitResult::Success(rmp_serde::to_vec(&()).unwrap()),
    });

    storage.save_reply(&chunk_two).await.unwrap();
    storage.save_reply(&exit).await.unwrap();
    storage.save_reply(&chunk_one).await.unwrap();

    let replies = storage.replies_for(request_id).await.unwrap();
    let sequences: Vec<i32> = replies
        .iter()
        .filter_map(|reply| match reply {
            Reply::Chunk(chunk) => Some(chunk.sequence),
            Reply::WithExit(_) => None,
        })
        .collect();

    assert!(replies
        .iter()
        .any(|reply| matches!(reply, Reply::WithExit(_))));
    assert_eq!(sequences, vec![1, 2]);
}
