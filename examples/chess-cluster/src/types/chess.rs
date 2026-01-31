//! Chess types wrapping shakmaty.
//!
//! This module provides serializable wrapper types around shakmaty's types,
//! making them suitable for use in cluster entities and API responses.

use serde::{Deserialize, Serialize};

/// Chess piece color.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Color {
    /// White pieces.
    White,
    /// Black pieces.
    Black,
}

impl Color {
    /// Get the opposite color.
    #[must_use]
    pub const fn opposite(self) -> Self {
        match self {
            Self::White => Self::Black,
            Self::Black => Self::White,
        }
    }

    /// Returns true if this is white.
    #[must_use]
    pub const fn is_white(self) -> bool {
        matches!(self, Self::White)
    }

    /// Returns true if this is black.
    #[must_use]
    pub const fn is_black(self) -> bool {
        matches!(self, Self::Black)
    }
}

impl From<shakmaty::Color> for Color {
    fn from(c: shakmaty::Color) -> Self {
        match c {
            shakmaty::Color::White => Self::White,
            shakmaty::Color::Black => Self::Black,
        }
    }
}

impl From<Color> for shakmaty::Color {
    fn from(c: Color) -> Self {
        match c {
            Color::White => Self::White,
            Color::Black => Self::Black,
        }
    }
}

impl std::fmt::Display for Color {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::White => write!(f, "white"),
            Self::Black => write!(f, "black"),
        }
    }
}

/// Chess piece type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PieceType {
    /// Pawn.
    Pawn,
    /// Knight.
    Knight,
    /// Bishop.
    Bishop,
    /// Rook.
    Rook,
    /// Queen.
    Queen,
    /// King.
    King,
}

impl From<shakmaty::Role> for PieceType {
    fn from(r: shakmaty::Role) -> Self {
        match r {
            shakmaty::Role::Pawn => Self::Pawn,
            shakmaty::Role::Knight => Self::Knight,
            shakmaty::Role::Bishop => Self::Bishop,
            shakmaty::Role::Rook => Self::Rook,
            shakmaty::Role::Queen => Self::Queen,
            shakmaty::Role::King => Self::King,
        }
    }
}

impl From<PieceType> for shakmaty::Role {
    fn from(p: PieceType) -> Self {
        match p {
            PieceType::Pawn => Self::Pawn,
            PieceType::Knight => Self::Knight,
            PieceType::Bishop => Self::Bishop,
            PieceType::Rook => Self::Rook,
            PieceType::Queen => Self::Queen,
            PieceType::King => Self::King,
        }
    }
}

impl std::fmt::Display for PieceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pawn => write!(f, "pawn"),
            Self::Knight => write!(f, "knight"),
            Self::Bishop => write!(f, "bishop"),
            Self::Rook => write!(f, "rook"),
            Self::Queen => write!(f, "queen"),
            Self::King => write!(f, "king"),
        }
    }
}

/// A chess piece with color and type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Piece {
    /// Piece color.
    pub color: Color,
    /// Piece type.
    pub piece_type: PieceType,
}

impl From<shakmaty::Piece> for Piece {
    fn from(p: shakmaty::Piece) -> Self {
        Self {
            color: p.color.into(),
            piece_type: p.role.into(),
        }
    }
}

impl From<Piece> for shakmaty::Piece {
    fn from(p: Piece) -> Self {
        Self {
            color: p.color.into(),
            role: p.piece_type.into(),
        }
    }
}

/// A square on the chess board (a1-h8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Square(u8);

impl Square {
    /// Create a square from file (0-7) and rank (0-7).
    #[must_use]
    pub const fn new(file: u8, rank: u8) -> Option<Self> {
        if file < 8 && rank < 8 {
            Some(Self(rank * 8 + file))
        } else {
            None
        }
    }

    /// Get the file (0-7, a-h).
    #[must_use]
    pub const fn file(self) -> u8 {
        self.0 % 8
    }

    /// Get the rank (0-7, 1-8).
    #[must_use]
    pub const fn rank(self) -> u8 {
        self.0 / 8
    }
}

impl From<shakmaty::Square> for Square {
    fn from(s: shakmaty::Square) -> Self {
        Self(s as u8)
    }
}

impl From<Square> for shakmaty::Square {
    fn from(s: Square) -> Self {
        // SAFETY: Square is always in valid range 0-63
        Self::new(s.0 as u32)
    }
}

impl std::fmt::Display for Square {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let file = (b'a' + self.file()) as char;
        let rank = (b'1' + self.rank()) as char;
        write!(f, "{file}{rank}")
    }
}

impl std::str::FromStr for Square {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 2 {
            return Err("square must be 2 characters");
        }
        let mut chars = s.chars();
        let file = chars.next().ok_or("missing file")?;
        let rank = chars.next().ok_or("missing rank")?;

        if !('a'..='h').contains(&file) {
            return Err("file must be a-h");
        }
        if !('1'..='8').contains(&rank) {
            return Err("rank must be 1-8");
        }

        let file_idx = (file as u8) - b'a';
        let rank_idx = (rank as u8) - b'1';

        Self::new(file_idx, rank_idx).ok_or("invalid square")
    }
}

/// A UCI move string (e.g., "e2e4", "e7e8q").
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UciMove(String);

impl UciMove {
    /// Create a new UCI move from a string.
    /// Does basic validation but not legality checking.
    pub fn new(s: impl Into<String>) -> Result<Self, &'static str> {
        let s = s.into();
        if s.len() < 4 || s.len() > 5 {
            return Err("UCI move must be 4-5 characters");
        }
        // Basic format check: two squares, optional promotion
        let _from: Square = s[0..2].parse()?;
        let _to: Square = s[2..4].parse()?;
        if s.len() == 5 {
            let promo = s.chars().nth(4).unwrap();
            if !['q', 'r', 'b', 'n'].contains(&promo) {
                return Err("promotion must be q, r, b, or n");
            }
        }
        Ok(Self(s))
    }

    /// Get the move as a string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Get the source square.
    #[must_use]
    pub fn from_square(&self) -> Square {
        self.0[0..2].parse().expect("validated in constructor")
    }

    /// Get the destination square.
    #[must_use]
    pub fn to_square(&self) -> Square {
        self.0[2..4].parse().expect("validated in constructor")
    }

    /// Get the promotion piece type, if any.
    #[must_use]
    pub fn promotion(&self) -> Option<PieceType> {
        self.0.chars().nth(4).map(|c| match c {
            'q' => PieceType::Queen,
            'r' => PieceType::Rook,
            'b' => PieceType::Bishop,
            'n' => PieceType::Knight,
            _ => unreachable!("validated in constructor"),
        })
    }
}

impl std::fmt::Display for UciMove {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for UciMove {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

/// Legal move information for API responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegalMove {
    /// UCI notation (e.g., "e2e4").
    pub uci: String,
    /// SAN notation (e.g., "e4").
    pub san: String,
    /// Source square.
    pub from: String,
    /// Destination square.
    pub to: String,
    /// Promotion piece if applicable.
    pub promotion: Option<PieceType>,
    /// Is this a capture?
    pub is_capture: bool,
    /// Is this a check?
    pub is_check: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_color_opposite() {
        assert_eq!(Color::White.opposite(), Color::Black);
        assert_eq!(Color::Black.opposite(), Color::White);
    }

    #[test]
    fn test_color_conversion() {
        let white = Color::White;
        let shak_white: shakmaty::Color = white.into();
        assert_eq!(shak_white, shakmaty::Color::White);

        let back: Color = shak_white.into();
        assert_eq!(back, Color::White);
    }

    #[test]
    fn test_square_roundtrip() {
        let sq = Square::new(4, 3).unwrap(); // e4
        assert_eq!(sq.file(), 4);
        assert_eq!(sq.rank(), 3);
        assert_eq!(sq.to_string(), "e4");

        let parsed: Square = "e4".parse().unwrap();
        assert_eq!(parsed, sq);
    }

    #[test]
    fn test_square_bounds() {
        assert!(Square::new(0, 0).is_some()); // a1
        assert!(Square::new(7, 7).is_some()); // h8
        assert!(Square::new(8, 0).is_none());
        assert!(Square::new(0, 8).is_none());
    }

    #[test]
    fn test_uci_move_valid() {
        let m = UciMove::new("e2e4").unwrap();
        assert_eq!(m.as_str(), "e2e4");
        assert_eq!(m.from_square().to_string(), "e2");
        assert_eq!(m.to_square().to_string(), "e4");
        assert!(m.promotion().is_none());

        let promo = UciMove::new("e7e8q").unwrap();
        assert_eq!(promo.promotion(), Some(PieceType::Queen));
    }

    #[test]
    fn test_uci_move_invalid() {
        assert!(UciMove::new("e2").is_err());
        assert!(UciMove::new("e2e4e5").is_err());
        assert!(UciMove::new("e2e4x").is_err());
        assert!(UciMove::new("i2i4").is_err());
        assert!(UciMove::new("e0e4").is_err());
    }
}
