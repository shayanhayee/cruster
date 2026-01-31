use cruster::{entity, entity_impl, entity_trait, entity_trait_impl};
use cruster::entity::Entity;
use cruster::error::ClusterError;

#[entity_trait]
#[derive(Clone)]
struct LoggerTrait;

#[entity_trait_impl]
impl LoggerTrait {
    #[rpc]
    async fn log(&self, message: String) -> Result<String, ClusterError> {
        Ok(message)
    }
}

#[entity]
#[derive(Clone)]
struct LoggingEntity;

#[entity_impl(traits(LoggerTrait))]
impl LoggingEntity {
    #[rpc]
    async fn ping(&self) -> Result<String, ClusterError> {
        Ok("pong".to_string())
    }
}

fn assert_entity<T: Entity>() {}

fn main() {
    assert_entity::<LoggingEntity>();
}
