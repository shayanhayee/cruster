//! PlayerSession entity - tracks a player's active connection and current game.
//!
//! This is an in-memory entity that maintains state only while the player is connected.
//! State is lost on restart, which is appropriate since it tracks ephemeral connection state.

use cruster::entity::EntityContext;
use cruster::error::ClusterError;
use cruster::prelude::*;
use serde::{Deserialize, Serialize};

use crate::types::chess::Color;
use crate::types::game::GameId;
use crate::types::player::{PlayerId, PlayerInfo, PlayerStatus};

/// State for a player session entity.
///
/// This is in-memory only - when the server restarts, sessions are lost.
/// Players need to reconnect to establish a new session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlayerSessionState {
    /// The player ID (derived from entity ID at init time).
    pub player_id: PlayerId,
    /// Player information (set after connect).
    pub info: Option<PlayerInfo>,
}

/// PlayerSession entity tracks a connected player's state.
///
/// ## State
/// - player_id, username, current_game_id, status, connected_at, last_activity
///
/// ## RPCs
/// - `connect(username)` - Initialize session
/// - `join_matchmaking_queue()` - Enter queue
/// - `leave_queue()` - Exit queue
/// - `get_status()` - Return current state
/// - `resign_game()` - Resign current game
/// - `notify_game_event(event)` - Called by ChessGame to push events
/// - `notify_match_found(game_id, color)` - Called by Matchmaking when paired
#[entity(max_idle_time_secs = 60)]
#[derive(Clone)]
pub struct PlayerSession;

/// Request to connect a player session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectRequest {
    /// Display username for the player.
    pub username: String,
}

/// Response from connecting a session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectResponse {
    /// The player ID (derived from entity ID).
    pub player_id: PlayerId,
    /// The assigned username.
    pub username: String,
    /// Current status after connecting.
    pub status: PlayerStatus,
}

/// Response for getting player status.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatusResponse {
    /// Whether the session is active.
    pub connected: bool,
    /// Player info if connected.
    pub info: Option<PlayerInfo>,
}

/// A game event notification sent to the player.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GameEvent {
    /// The game has started.
    GameStarted {
        /// ID of the game.
        game_id: GameId,
        /// Opponent's player ID.
        opponent: PlayerId,
        /// Your color in the game.
        your_color: Color,
    },
    /// Opponent made a move.
    OpponentMoved {
        /// ID of the game.
        game_id: GameId,
        /// The move in UCI notation.
        uci_move: String,
        /// The move in SAN notation.
        san_move: String,
    },
    /// Opponent offered a draw.
    DrawOffered {
        /// ID of the game.
        game_id: GameId,
    },
    /// The game has ended.
    GameEnded {
        /// ID of the game.
        game_id: GameId,
        /// Whether you won, lost, or drew.
        result: GameEventResult,
        /// Reason the game ended.
        reason: String,
    },
    /// It's your turn to move.
    YourTurn {
        /// ID of the game.
        game_id: GameId,
    },
}

/// Result of a game from the player's perspective.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GameEventResult {
    /// You won the game.
    Won,
    /// You lost the game.
    Lost,
    /// The game was a draw.
    Draw,
}

/// Notification that a match was found.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MatchFoundNotification {
    /// The game ID for the new game.
    pub game_id: GameId,
    /// Your assigned color.
    pub color: Color,
    /// The opponent's player ID.
    pub opponent_id: PlayerId,
}

/// Error types specific to player session operations.
#[derive(Clone, Debug, Serialize, Deserialize, thiserror::Error)]
pub enum PlayerSessionError {
    /// Player is not connected.
    #[error("player not connected")]
    NotConnected,
    /// Player is already connected.
    #[error("player already connected")]
    AlreadyConnected,
    /// Player cannot join queue in current state.
    #[error("cannot join queue: player status is {status}")]
    CannotJoinQueue {
        /// Current status preventing queue join.
        status: PlayerStatus,
    },
    /// Player is not in the queue.
    #[error("player not in queue")]
    NotInQueue,
    /// Player is not in a game.
    #[error("player not in game")]
    NotInGame,
    /// Invalid game reference.
    #[error("not in the specified game")]
    WrongGame,
}

impl From<PlayerSessionError> for ClusterError {
    fn from(err: PlayerSessionError) -> Self {
        ClusterError::MalformedMessage {
            reason: err.to_string(),
            source: None,
        }
    }
}

#[entity_impl]
#[state(PlayerSessionState)]
impl PlayerSession {
    fn init(&self, ctx: &EntityContext) -> Result<PlayerSessionState, ClusterError> {
        // Parse the entity ID as a player ID
        let player_id: PlayerId =
            ctx.address
                .entity_id
                .as_ref()
                .parse()
                .map_err(|e| ClusterError::MalformedMessage {
                    reason: format!("invalid player ID: {e}"),
                    source: None,
                })?;

        Ok(PlayerSessionState {
            player_id,
            info: None,
        })
    }

    /// Connect a player session with the given username.
    #[workflow]
    pub async fn connect(&self, request: ConnectRequest) -> Result<ConnectResponse, ClusterError> {
        // Validation (deterministic)
        if self.state.info.is_some() {
            return Err(PlayerSessionError::AlreadyConnected.into());
        }
        let player_id = self.state.player_id;

        // State mutation via activity
        self.do_connect(player_id, request.username.clone()).await?;

        Ok(ConnectResponse {
            player_id,
            username: request.username,
            status: PlayerStatus::Online,
        })
    }

    #[activity]
    async fn do_connect(
        &mut self,
        player_id: PlayerId,
        username: String,
    ) -> Result<(), ClusterError> {
        let info = PlayerInfo::new(player_id, username);
        self.state.info = Some(info);
        Ok(())
    }

    /// Disconnect the player session.
    #[workflow]
    pub async fn disconnect(&self) -> Result<(), ClusterError> {
        self.do_disconnect().await
    }

    #[activity]
    async fn do_disconnect(&mut self) -> Result<(), ClusterError> {
        if let Some(info) = &mut self.state.info {
            info.status = PlayerStatus::Offline;
        }
        Ok(())
    }

    /// Get the current status of the player session.
    #[rpc]
    pub async fn get_status(&self) -> Result<StatusResponse, ClusterError> {
        Ok(StatusResponse {
            connected: self
                .state
                .info
                .as_ref()
                .is_some_and(|i| i.status.is_connected()),
            info: self.state.info.clone(),
        })
    }

    /// Join the matchmaking queue.
    #[workflow]
    pub async fn join_matchmaking_queue(&self) -> Result<u32, ClusterError> {
        // Validation (deterministic)
        let info = self
            .state
            .info
            .as_ref()
            .ok_or(PlayerSessionError::NotConnected)?;
        if !info.status.can_join_queue() {
            return Err(PlayerSessionError::CannotJoinQueue {
                status: info.status,
            }
            .into());
        }

        // State mutation via activity
        self.do_join_queue().await?;
        Ok(1) // Placeholder queue position
    }

    #[activity]
    async fn do_join_queue(&mut self) -> Result<(), ClusterError> {
        let info = self
            .state
            .info
            .as_mut()
            .ok_or(PlayerSessionError::NotConnected)?;
        info.status = PlayerStatus::InQueue;
        info.touch();
        Ok(())
    }

    /// Leave the matchmaking queue.
    #[workflow]
    pub async fn leave_queue(&self) -> Result<(), ClusterError> {
        // Validation (deterministic)
        let info = self
            .state
            .info
            .as_ref()
            .ok_or(PlayerSessionError::NotConnected)?;
        if info.status != PlayerStatus::InQueue {
            return Err(PlayerSessionError::NotInQueue.into());
        }

        // State mutation via activity
        self.do_leave_queue().await
    }

    #[activity]
    async fn do_leave_queue(&mut self) -> Result<(), ClusterError> {
        let info = self
            .state
            .info
            .as_mut()
            .ok_or(PlayerSessionError::NotConnected)?;
        info.status = PlayerStatus::Online;
        info.touch();
        Ok(())
    }

    /// Resign from the current game.
    #[workflow]
    pub async fn resign_game(&self) -> Result<GameId, ClusterError> {
        // Validation (deterministic)
        let game_id = self
            .state
            .info
            .as_ref()
            .ok_or(PlayerSessionError::NotConnected)?
            .current_game_id
            .ok_or(PlayerSessionError::NotInGame)?;

        // State mutation via activity
        self.do_resign_game().await?;
        Ok(game_id)
    }

    #[activity]
    async fn do_resign_game(&mut self) -> Result<(), ClusterError> {
        let info = self
            .state
            .info
            .as_mut()
            .ok_or(PlayerSessionError::NotConnected)?;
        info.current_game_id = None;
        info.status = PlayerStatus::Online;
        info.touch();
        Ok(())
    }

    /// Notify this player of a game event.
    #[workflow]
    pub async fn notify_game_event(&self, event: GameEvent) -> Result<(), ClusterError> {
        // Validation (deterministic)
        if self.state.info.is_none() {
            return Err(PlayerSessionError::NotConnected.into());
        }

        // State mutation via activity
        self.do_notify_game_event(event).await
    }

    #[activity]
    async fn do_notify_game_event(&mut self, event: GameEvent) -> Result<(), ClusterError> {
        let info = self
            .state
            .info
            .as_mut()
            .ok_or(PlayerSessionError::NotConnected)?;

        match &event {
            GameEvent::GameStarted { game_id, .. } => {
                info.current_game_id = Some(*game_id);
                info.status = PlayerStatus::InGame;
            }
            GameEvent::GameEnded { .. } => {
                info.current_game_id = None;
                info.status = PlayerStatus::Online;
            }
            _ => {}
        }
        info.touch();
        Ok(())
    }

    /// Notify this player that a match was found.
    #[workflow]
    pub async fn notify_match_found(
        &self,
        notification: MatchFoundNotification,
    ) -> Result<(), ClusterError> {
        // Validation (deterministic)
        if self.state.info.is_none() {
            return Err(PlayerSessionError::NotConnected.into());
        }

        // State mutation via activity
        self.do_notify_match_found(notification.game_id).await
    }

    #[activity]
    async fn do_notify_match_found(&mut self, game_id: GameId) -> Result<(), ClusterError> {
        let info = self
            .state
            .info
            .as_mut()
            .ok_or(PlayerSessionError::NotConnected)?;
        info.current_game_id = Some(game_id);
        info.status = PlayerStatus::InGame;
        info.touch();
        Ok(())
    }

    /// Get the player's current game ID, if any.
    #[rpc]
    pub async fn get_current_game(&self) -> Result<Option<GameId>, ClusterError> {
        Ok(self.state.info.as_ref().and_then(|i| i.current_game_id))
    }

    /// Update the last activity timestamp.
    #[workflow]
    pub async fn heartbeat(&self) -> Result<(), ClusterError> {
        self.do_heartbeat().await
    }

    #[activity]
    async fn do_heartbeat(&mut self) -> Result<(), ClusterError> {
        if let Some(info) = &mut self.state.info {
            info.touch();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_player_session_state_serialization() {
        let state = PlayerSessionState {
            player_id: PlayerId::new(),
            info: None,
        };
        let json = serde_json::to_string(&state).unwrap();
        let parsed: PlayerSessionState = serde_json::from_str(&json).unwrap();
        assert!(parsed.info.is_none());
    }

    #[test]
    fn test_game_event_serialization() {
        let event = GameEvent::GameStarted {
            game_id: GameId::new(),
            opponent: PlayerId::new(),
            your_color: Color::White,
        };
        let json = serde_json::to_string(&event).unwrap();
        let parsed: GameEvent = serde_json::from_str(&json).unwrap();

        match parsed {
            GameEvent::GameStarted { your_color, .. } => {
                assert_eq!(your_color, Color::White);
            }
            _ => panic!("wrong event type"),
        }
    }

    #[test]
    fn test_player_session_error_display() {
        let err = PlayerSessionError::CannotJoinQueue {
            status: PlayerStatus::InGame,
        };
        assert!(err.to_string().contains("in_game"));
    }
}
