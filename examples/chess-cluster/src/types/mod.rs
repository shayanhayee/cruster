//! Type definitions for the chess cluster.
//!
//! ## Modules
//!
//! - `chess` - Chess types wrapping shakmaty (Color, Square, etc.)
//! - `game` - Game-related types (GameId, GameStatus, MoveRecord)
//! - `player` - Player-related types (PlayerId, PlayerStatus)

pub mod chess;
pub mod game;
pub mod player;

// Re-export commonly used types
pub use chess::{Color, LegalMove, Piece, PieceType, Square, UciMove};
pub use game::{
    DrawOffer, GameId, GameResult, GameResultInfo, GameState, GameStatus, MoveRecord, TimeControl,
};
pub use player::{PlayerId, PlayerInfo, PlayerRating, PlayerStatus};
