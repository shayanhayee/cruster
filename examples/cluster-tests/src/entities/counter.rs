//! Counter entity - basic entity for testing state persistence and simple RPCs.
//!
//! This entity provides basic counter operations to test:
//! - State persistence across calls
//! - State survival after entity eviction and reload
//! - Serialization of concurrent increments

use cruster::entity::EntityContext;
use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};

/// State for a counter entity.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CounterState {
    /// Current counter value.
    pub value: i64,
}

/// Counter entity for testing basic cluster operations.
///
/// ## State (Persisted)
/// - value: i64 - the counter value
///
/// ## RPCs
/// - `increment(amount)` - Add to counter, return new value
/// - `decrement(amount)` - Subtract from counter, return new value
/// - `get()` - Get current value
/// - `reset()` - Reset to zero
#[entity(max_idle_time_secs = 5)]
#[derive(Clone)]
pub struct Counter;

/// Request to increment the counter.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IncrementRequest {
    /// Amount to increment by.
    pub amount: i64,
}

/// Request to decrement the counter.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DecrementRequest {
    /// Amount to decrement by.
    pub amount: i64,
}

#[entity_impl]
#[state(CounterState)]
impl Counter {
    fn init(&self, _ctx: &EntityContext) -> Result<CounterState, ClusterError> {
        Ok(CounterState::default())
    }

    #[activity]
    async fn do_increment(&mut self, amount: i64) -> Result<i64, ClusterError> {
        self.state.value += amount;
        Ok(self.state.value)
    }

    #[activity]
    async fn do_decrement(&mut self, amount: i64) -> Result<i64, ClusterError> {
        self.state.value -= amount;
        Ok(self.state.value)
    }

    #[activity]
    async fn do_reset(&mut self) -> Result<(), ClusterError> {
        self.state.value = 0;
        Ok(())
    }

    /// Increment the counter by the given amount and return the new value.
    #[workflow]
    pub async fn increment(&self, request: IncrementRequest) -> Result<i64, ClusterError> {
        self.do_increment(request.amount).await
    }

    /// Decrement the counter by the given amount and return the new value.
    #[workflow]
    pub async fn decrement(&self, request: DecrementRequest) -> Result<i64, ClusterError> {
        self.do_decrement(request.amount).await
    }

    /// Get the current counter value.
    #[rpc]
    pub async fn get(&self) -> Result<i64, ClusterError> {
        Ok(self.state.value)
    }

    /// Reset the counter to zero.
    #[workflow]
    pub async fn reset(&self) -> Result<(), ClusterError> {
        self.do_reset().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter_state_serialization() {
        let state = CounterState { value: 42 };
        let json = serde_json::to_string(&state).unwrap();
        let parsed: CounterState = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.value, 42);
    }

    #[test]
    fn test_counter_state_default() {
        let state = CounterState::default();
        assert_eq!(state.value, 0);
    }
}
