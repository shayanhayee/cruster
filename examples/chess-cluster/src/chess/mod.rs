//! Chess logic integration with shakmaty.
//!
//! This module provides a clean interface to shakmaty for:
//! - Move validation
//! - Game state management
//! - FEN string handling
//! - Legal move generation

pub mod engine;

pub use engine::{ChessError, ChessPosition, Outcome};
