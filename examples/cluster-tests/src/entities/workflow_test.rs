//! WorkflowTest entity - entity for testing durable workflow execution and replay.
//!
//! This entity provides workflow operations to test:
//! - Workflow completes all steps
//! - Workflow replays correctly after runner restart
//! - Failed workflow can be retried
//! - Steps are idempotent on replay

use cruster::entity::EntityContext;
use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};

/// Record of a single workflow execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkflowExecution {
    /// Unique identifier for this execution.
    pub id: String,
    /// Steps that have been completed.
    pub steps_completed: Vec<String>,
    /// Final result if workflow completed.
    pub result: Option<String>,
}

/// State for a WorkflowTest entity.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WorkflowTestState {
    /// All workflow executions for this entity.
    pub executions: Vec<WorkflowExecution>,
}

/// WorkflowTest entity for testing durable workflow execution.
///
/// ## State (Persisted)
/// - executions: `Vec<WorkflowExecution>` - all workflow executions
///
/// ## Workflows
/// - `run_simple_workflow(exec_id)` - Execute 3 steps, return result
/// - `run_failing_workflow(exec_id, fail_at)` - Fail at specific step
/// - `run_long_workflow(exec_id, steps)` - Execute N steps
///
/// ## RPCs
/// - `get_execution(exec_id)` - Get a specific execution
/// - `list_executions()` - List all executions
#[entity(max_idle_time_secs = 60)]
#[derive(Clone)]
pub struct WorkflowTest;

/// Request to run a simple workflow.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunSimpleWorkflowRequest {
    /// Unique execution ID.
    pub exec_id: String,
}

/// Request to run a workflow that fails at a specific step.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunFailingWorkflowRequest {
    /// Unique execution ID.
    pub exec_id: String,
    /// Step number to fail at (0-indexed).
    pub fail_at: usize,
}

/// Request to run a long workflow with N steps.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunLongWorkflowRequest {
    /// Unique execution ID.
    pub exec_id: String,
    /// Number of steps to execute.
    pub steps: usize,
}

/// Request to get a specific execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetExecutionRequest {
    /// Execution ID to retrieve.
    pub exec_id: String,
}

#[entity_impl]
#[state(WorkflowTestState)]
impl WorkflowTest {
    fn init(&self, _ctx: &EntityContext) -> Result<WorkflowTestState, ClusterError> {
        Ok(WorkflowTestState::default())
    }

    #[activity]
    async fn create_execution(&mut self, exec_id: String) -> Result<(), ClusterError> {
        let execution = WorkflowExecution {
            id: exec_id,
            steps_completed: Vec::new(),
            result: None,
        };
        self.state.executions.push(execution);
        Ok(())
    }

    #[activity]
    async fn complete_step(
        &mut self,
        exec_id: String,
        step_name: String,
    ) -> Result<(), ClusterError> {
        if let Some(exec) = self.state.executions.iter_mut().find(|e| e.id == exec_id) {
            exec.steps_completed.push(step_name);
        }
        Ok(())
    }

    #[activity]
    async fn mark_completed(
        &mut self,
        exec_id: String,
        result: String,
    ) -> Result<(), ClusterError> {
        if let Some(exec) = self.state.executions.iter_mut().find(|e| e.id == exec_id) {
            exec.result = Some(result);
        }
        Ok(())
    }

    #[activity]
    async fn mark_failed(&mut self, exec_id: String, step: usize) -> Result<(), ClusterError> {
        if let Some(exec) = self.state.executions.iter_mut().find(|e| e.id == exec_id) {
            exec.result = Some(format!("failed:step{}", step));
        }
        Ok(())
    }

    /// Execute a simple workflow with 3 steps.
    #[workflow]
    pub async fn run_simple_workflow(
        &self,
        request: RunSimpleWorkflowRequest,
    ) -> Result<String, ClusterError> {
        let exec_id = request.exec_id;

        // Create execution record
        self.create_execution(exec_id.clone()).await?;

        // Execute step 1
        self.complete_step(exec_id.clone(), "step1".to_string())
            .await?;

        // Execute step 2
        self.complete_step(exec_id.clone(), "step2".to_string())
            .await?;

        // Execute step 3
        self.complete_step(exec_id.clone(), "step3".to_string())
            .await?;

        // Mark as completed
        let result = format!("completed:{}", exec_id);
        self.mark_completed(exec_id, result.clone()).await?;

        Ok(result)
    }

    /// Execute a workflow that fails at a specific step.
    #[workflow]
    pub async fn run_failing_workflow(
        &self,
        request: RunFailingWorkflowRequest,
    ) -> Result<String, ClusterError> {
        let exec_id = request.exec_id;
        let fail_at = request.fail_at;

        // Create execution record
        self.create_execution(exec_id.clone()).await?;

        // Execute steps until failure
        for i in 0..3 {
            if i == fail_at {
                // Mark as failed
                self.mark_failed(exec_id.clone(), i).await?;
                return Err(ClusterError::MalformedMessage {
                    reason: format!("Intentional failure at step {}", i),
                    source: None,
                });
            }
            self.complete_step(exec_id.clone(), format!("step{}", i))
                .await?;
        }

        // Mark as completed
        let result = format!("completed:{}", exec_id);
        self.mark_completed(exec_id, result.clone()).await?;

        Ok(result)
    }

    /// Execute a workflow with N steps.
    #[workflow]
    pub async fn run_long_workflow(
        &self,
        request: RunLongWorkflowRequest,
    ) -> Result<String, ClusterError> {
        let exec_id = request.exec_id;
        let steps = request.steps;

        // Create execution record
        self.create_execution(exec_id.clone()).await?;

        // Execute all steps
        for i in 0..steps {
            self.complete_step(exec_id.clone(), format!("step{}", i))
                .await?;
        }

        // Mark as completed
        let result = format!("completed:{}", exec_id);
        self.mark_completed(exec_id, result.clone()).await?;

        Ok(result)
    }

    /// Get a specific workflow execution.
    #[rpc]
    pub async fn get_execution(
        &self,
        request: GetExecutionRequest,
    ) -> Result<Option<WorkflowExecution>, ClusterError> {
        Ok(self
            .state
            .executions
            .iter()
            .find(|e| e.id == request.exec_id)
            .cloned())
    }

    /// List all workflow executions.
    #[rpc]
    pub async fn list_executions(&self) -> Result<Vec<WorkflowExecution>, ClusterError> {
        Ok(self.state.executions.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workflow_execution_serialization() {
        let execution = WorkflowExecution {
            id: "test-exec".to_string(),
            steps_completed: vec!["step1".to_string(), "step2".to_string()],
            result: Some("completed".to_string()),
        };

        let json = serde_json::to_string(&execution).unwrap();
        let parsed: WorkflowExecution = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.id, "test-exec");
        assert_eq!(parsed.steps_completed.len(), 2);
        assert_eq!(parsed.result, Some("completed".to_string()));
    }

    #[test]
    fn test_workflow_test_state_default() {
        let state = WorkflowTestState::default();
        assert!(state.executions.is_empty());
    }

    #[test]
    fn test_workflow_test_state_serialization() {
        let mut state = WorkflowTestState::default();
        state.executions.push(WorkflowExecution {
            id: "exec1".to_string(),
            steps_completed: vec!["step1".to_string()],
            result: None,
        });

        let json = serde_json::to_string(&state).unwrap();
        let parsed: WorkflowTestState = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.executions.len(), 1);
        assert_eq!(parsed.executions[0].id, "exec1");
    }
}
