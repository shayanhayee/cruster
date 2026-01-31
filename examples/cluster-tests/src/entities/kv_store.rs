//! KVStore entity - key-value store for testing more complex state operations.
//!
//! This entity provides key-value operations to test:
//! - CRUD operations
//! - Large value handling
//! - Many keys performance

use cruster::entity::EntityContext;
use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// State for a KVStore entity.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct KVStoreState {
    /// Key-value data storage.
    pub data: HashMap<String, serde_json::Value>,
}

/// KVStore entity for testing key-value operations.
///
/// ## State (Persisted)
/// - data: HashMap<String, JsonValue> - the key-value store
///
/// ## RPCs
/// - `set(key, value)` - Set a key to a value
/// - `get(key)` - Get value for a key
/// - `delete(key)` - Delete a key
/// - `list_keys()` - List all keys
/// - `clear()` - Clear all data
#[entity(max_idle_time_secs = 60)]
#[derive(Clone)]
pub struct KVStore;

/// Request to set a key-value pair.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SetRequest {
    /// Key to set.
    pub key: String,
    /// Value to set.
    pub value: serde_json::Value,
}

/// Request to get a value by key.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetRequest {
    /// Key to get.
    pub key: String,
}

/// Request to delete a key.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeleteRequest {
    /// Key to delete.
    pub key: String,
}

#[entity_impl]
#[state(KVStoreState)]
impl KVStore {
    fn init(&self, _ctx: &EntityContext) -> Result<KVStoreState, ClusterError> {
        Ok(KVStoreState::default())
    }

    #[activity]
    async fn do_set(&mut self, key: String, value: serde_json::Value) -> Result<(), ClusterError> {
        self.state.data.insert(key, value);
        Ok(())
    }

    #[activity]
    async fn do_delete(&mut self, key: String) -> Result<bool, ClusterError> {
        Ok(self.state.data.remove(&key).is_some())
    }

    #[activity]
    async fn do_clear(&mut self) -> Result<(), ClusterError> {
        self.state.data.clear();
        Ok(())
    }

    /// Set a key to a value.
    #[workflow]
    pub async fn set(&self, request: SetRequest) -> Result<(), ClusterError> {
        self.do_set(request.key, request.value).await
    }

    /// Get the value for a key.
    #[rpc]
    pub async fn get(
        &self,
        request: GetRequest,
    ) -> Result<Option<serde_json::Value>, ClusterError> {
        Ok(self.state.data.get(&request.key).cloned())
    }

    /// Delete a key and return whether it existed.
    #[workflow]
    pub async fn delete(&self, request: DeleteRequest) -> Result<bool, ClusterError> {
        self.do_delete(request.key).await
    }

    /// List all keys.
    #[rpc]
    pub async fn list_keys(&self) -> Result<Vec<String>, ClusterError> {
        Ok(self.state.data.keys().cloned().collect())
    }

    /// Clear all data.
    #[workflow]
    pub async fn clear(&self) -> Result<(), ClusterError> {
        self.do_clear().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kv_store_state_serialization() {
        let mut state = KVStoreState::default();
        state
            .data
            .insert("key1".to_string(), serde_json::json!("value1"));
        state.data.insert("key2".to_string(), serde_json::json!(42));

        let json = serde_json::to_string(&state).unwrap();
        let parsed: KVStoreState = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.data.get("key1"), Some(&serde_json::json!("value1")));
        assert_eq!(parsed.data.get("key2"), Some(&serde_json::json!(42)));
    }

    #[test]
    fn test_kv_store_state_default() {
        let state = KVStoreState::default();
        assert!(state.data.is_empty());
    }
}
