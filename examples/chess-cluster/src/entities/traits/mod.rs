//! Shared entity traits.
//!
//! - `Auditable` - Provides audit logging capability for entities

pub mod auditable;

pub use auditable::{
    AuditEntry, AuditLog, Auditable, GetAuditLogRequest, GetAuditLogResponse,
    LogPlayerActionRequest, LogSystemActionRequest,
};
