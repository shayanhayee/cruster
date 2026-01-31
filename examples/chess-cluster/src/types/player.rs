//! Player-related types.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::game::GameId;

/// Unique identifier for a player.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PlayerId(Uuid);

impl PlayerId {
    /// Create a new random player ID.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Create a nil (all zeros) player ID, used as a placeholder.
    #[must_use]
    pub const fn nil() -> Self {
        Self(Uuid::nil())
    }

    /// Check if this is a nil (placeholder) player ID.
    #[must_use]
    pub fn is_nil(&self) -> bool {
        self.0.is_nil()
    }

    /// Create a player ID from an existing UUID.
    #[must_use]
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Get the underlying UUID.
    #[must_use]
    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for PlayerId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for PlayerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for PlayerId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

/// Player connection/activity status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlayerStatus {
    /// Player is connected and available.
    Online,
    /// Player is currently in a game.
    InGame,
    /// Player is connected but idle.
    Idle,
    /// Player is in the matchmaking queue.
    InQueue,
    /// Player has disconnected.
    Offline,
}

impl PlayerStatus {
    /// Returns true if the player can join matchmaking.
    #[must_use]
    pub const fn can_join_queue(&self) -> bool {
        matches!(self, Self::Online | Self::Idle)
    }

    /// Returns true if the player is actively connected.
    #[must_use]
    pub const fn is_connected(&self) -> bool {
        !matches!(self, Self::Offline)
    }
}

impl std::fmt::Display for PlayerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Online => write!(f, "online"),
            Self::InGame => write!(f, "in_game"),
            Self::Idle => write!(f, "idle"),
            Self::InQueue => write!(f, "in_queue"),
            Self::Offline => write!(f, "offline"),
        }
    }
}

/// Player rating information for the leaderboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerRating {
    /// Current ELO rating.
    pub elo: i32,
    /// Number of wins.
    pub wins: u32,
    /// Number of losses.
    pub losses: u32,
    /// Number of draws.
    pub draws: u32,
    /// Peak ELO rating ever achieved.
    pub peak_elo: i32,
}

impl PlayerRating {
    /// Starting ELO rating for new players.
    pub const STARTING_ELO: i32 = 1200;

    /// Create a new player rating with default values.
    #[must_use]
    pub fn new() -> Self {
        Self {
            elo: Self::STARTING_ELO,
            wins: 0,
            losses: 0,
            draws: 0,
            peak_elo: Self::STARTING_ELO,
        }
    }

    /// Total number of games played.
    #[must_use]
    pub const fn total_games(&self) -> u32 {
        self.wins + self.losses + self.draws
    }

    /// Win rate as a percentage (0.0 to 1.0).
    #[must_use]
    pub fn win_rate(&self) -> f64 {
        let total = self.total_games();
        if total == 0 {
            0.0
        } else {
            f64::from(self.wins) / f64::from(total)
        }
    }
}

impl Default for PlayerRating {
    fn default() -> Self {
        Self::new()
    }
}

/// Player information stored in session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerInfo {
    /// Unique player identifier.
    pub id: PlayerId,
    /// Display name.
    pub username: String,
    /// Current game if any.
    pub current_game_id: Option<GameId>,
    /// Current status.
    pub status: PlayerStatus,
    /// When the player connected.
    pub connected_at: DateTime<Utc>,
    /// Last activity timestamp.
    pub last_activity: DateTime<Utc>,
}

impl PlayerInfo {
    /// Create a new player info for a connecting player.
    #[must_use]
    pub fn new(id: PlayerId, username: String) -> Self {
        let now = Utc::now();
        Self {
            id,
            username,
            current_game_id: None,
            status: PlayerStatus::Online,
            connected_at: now,
            last_activity: now,
        }
    }

    /// Update the last activity timestamp.
    pub fn touch(&mut self) {
        self.last_activity = Utc::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_player_id_roundtrip() {
        let id = PlayerId::new();
        let s = id.to_string();
        let parsed: PlayerId = s.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_player_status_can_join_queue() {
        assert!(PlayerStatus::Online.can_join_queue());
        assert!(PlayerStatus::Idle.can_join_queue());
        assert!(!PlayerStatus::InGame.can_join_queue());
        assert!(!PlayerStatus::InQueue.can_join_queue());
        assert!(!PlayerStatus::Offline.can_join_queue());
    }

    #[test]
    fn test_player_rating_defaults() {
        let rating = PlayerRating::new();
        assert_eq!(rating.elo, PlayerRating::STARTING_ELO);
        assert_eq!(rating.total_games(), 0);
        assert_eq!(rating.win_rate(), 0.0);
    }

    #[test]
    fn test_player_rating_win_rate() {
        let rating = PlayerRating {
            elo: 1300,
            wins: 7,
            losses: 2,
            draws: 1,
            peak_elo: 1350,
        };
        assert_eq!(rating.total_games(), 10);
        assert!((rating.win_rate() - 0.7).abs() < f64::EPSILON);
    }
}
