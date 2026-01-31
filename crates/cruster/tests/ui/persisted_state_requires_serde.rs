use cruster::{entity, entity_impl};
use cruster::entity::EntityContext;
use cruster::error::ClusterError;

struct BadState {
    value: std::sync::Mutex<i32>,
}

#[entity]
#[derive(Clone)]
struct BadStateEntity;

#[entity_impl]
#[state(BadState)]
impl BadStateEntity {
    fn init(&self, _ctx: &EntityContext) -> Result<BadState, ClusterError> {
        Ok(BadState {
            value: std::sync::Mutex::new(0),
        })
    }

    #[rpc]
    async fn ping(&self) -> Result<(), ClusterError> {
        Ok(())
    }
}

fn main() {}
