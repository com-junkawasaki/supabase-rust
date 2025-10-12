use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a full message received or sent over the WebSocket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealtimeMessage {
    pub topic: String,
    pub event: ChannelEvent,        // Use the ChannelEvent enum
    pub payload: serde_json::Value, // Flexible payload
    #[serde(rename = "ref")]
    pub message_ref: serde_json::Value, // Can be string or null
}

/// チャンネルイベント (including Phoenix/Realtime specific events)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")] // Use snake_case for serialization
pub enum ChannelEvent {
    Insert, // Database change events
    Update,
    Delete,
    All,             // Wildcard for database changes
    PostgresChanges, // Specific event type for Supabase DB changes

    #[serde(rename = "phx_join")] // Explicit rename for Phoenix events
    PhoenixJoin,
    #[serde(rename = "phx_reply")]
    PhoenixReply,
    #[serde(rename = "phx_error")]
    PhoenixError,
    #[serde(rename = "phx_close")]
    PhoenixClose,

    Heartbeat,
    Presence,
    Broadcast,
    // Add other known events as needed
}

impl std::fmt::Display for ChannelEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Use serde to get the correct string representation
        // This relies on the rename attributes defined above.
        write!(
            f,
            "{}",
            serde_json::to_string(self).unwrap_or_else(|_| format!("{self:?}"))
        )
        // Simple match for basic cases (less reliable than serde)
        // match self {
        //     Self::Insert => write!(f, "INSERT"),
        //     Self::Update => write!(f, "UPDATE"),
        //     Self::Delete => write!(f, "DELETE"),
        //     Self::All => write!(f, "*"),
        //     Self::PostgresChanges => write!(f, "postgres_changes"),
        //     Self::PhoenixJoin => write!(f, "phx_join"),
        //     Self::PhoenixReply => write!(f, "phx_reply"),
        //     Self::PhoenixError => write!(f, "phx_error"),
        //     Self::PhoenixClose => write!(f, "phx_close"),
        //     Self::Heartbeat => write!(f, "heartbeat"),
        //     Self::Presence => write!(f, "presence"),
        //     Self::Broadcast => write!(f, "broadcast"),
        // }
    }
}

/// メッセージペイロード
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Payload {
    // Consider making fields private and using accessors if necessary
    pub data: serde_json::Value,
    #[serde(rename = "type")] // Map 'type' field in JSON
    pub event_type: Option<String>,
    pub timestamp: Option<String>, // Timestamps often come as strings
}

/// プレゼンス変更情報
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresenceChange {
    pub joins: HashMap<String, serde_json::Value>,
    pub leaves: HashMap<String, serde_json::Value>,
}

/// プレゼンス状態全体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresenceState {
    // Consider wrapping HashMap for better type safety/abstraction
    pub state: HashMap<String, serde_json::Value>,
}

impl PresenceState {
    pub fn new() -> Self {
        Self {
            state: HashMap::new(),
        }
    }

    /// Apply presence diff to update the state
    pub fn sync(&mut self, presence_diff: &PresenceChange) {
        for (key, value) in &presence_diff.joins {
            self.state.insert(key.clone(), value.clone());
        }
        for key in presence_diff.leaves.keys() {
            self.state.remove(key);
        }
    }

    /// List current presence state as key-value pairs
    pub fn list(&self) -> Vec<(String, serde_json::Value)> {
        self.state
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Get presence info for a specific key
    pub fn get(&self, key: &str) -> Option<&serde_json::Value> {
        self.state.get(key)
    }
}

impl Default for PresenceState {
    fn default() -> Self {
        Self::new()
    }
}
