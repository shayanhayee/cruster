use cruster::{entity, entity_impl};
use cruster::error::ClusterError;

struct NotSerializable {
    value: std::cell::Cell<i32>,
}

#[entity]
#[derive(Clone)]
struct BadEntity;

#[entity_impl]
impl BadEntity {
    #[workflow]
    async fn bad(&self, value: NotSerializable) -> Result<(), ClusterError> {
        let _ = value;
        Ok(())
    }
}

fn main() {}
