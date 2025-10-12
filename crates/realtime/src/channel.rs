use crate::client::RealtimeClient; // Removed unused ConnectionState
use crate::error::RealtimeError;
use crate::filters::{DatabaseFilter, FilterOperator};
use crate::message::{ChannelEvent, Payload, PresenceChange, RealtimeMessage};
use log::{debug, error, info, trace}; // Removed unused warn
use serde::Serialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
// use tokio::sync::mpsc; // Unused import after commenting out `socket` field
use tokio::sync::RwLock;
use tokio::time::{timeout, Duration};
// use tokio_tungstenite::tungstenite::Message; // Removed unused import

/// データベース変更監視設定
#[derive(Debug, Clone, Serialize)]
pub struct DatabaseChanges {
    schema: String,
    table: String,
    events: Vec<ChannelEvent>,
    filter: Option<Vec<DatabaseFilter>>,
}

impl DatabaseChanges {
    /// 新しいデータベース変更監視設定を作成
    pub fn new(table: &str) -> Self {
        Self {
            schema: "public".to_string(),
            table: table.to_string(),
            events: Vec::new(),
            filter: None,
        }
    }

    /// スキーマを設定
    pub fn schema(mut self, schema: &str) -> Self {
        self.schema = schema.to_string();
        self
    }

    /// イベントを追加
    pub fn event(mut self, event: ChannelEvent) -> Self {
        if !self.events.contains(&event) {
            self.events.push(event);
        }
        self
    }

    /// フィルター条件を追加
    pub fn filter(mut self, filter: DatabaseFilter) -> Self {
        self.filter.get_or_insert_with(Vec::new).push(filter);
        self
    }

    // --- Filter convenience methods ---

    pub fn eq<T: Into<serde_json::Value>>(self, column: &str, value: T) -> Self {
        self.filter(DatabaseFilter {
            column: column.to_string(),
            operator: FilterOperator::Eq,
            value: value.into(),
        })
    }

    pub fn neq<T: Into<serde_json::Value>>(self, column: &str, value: T) -> Self {
        self.filter(DatabaseFilter {
            column: column.to_string(),
            operator: FilterOperator::Neq,
            value: value.into(),
        })
    }

    pub fn gt<T: Into<serde_json::Value>>(self, column: &str, value: T) -> Self {
        self.filter(DatabaseFilter {
            column: column.to_string(),
            operator: FilterOperator::Gt,
            value: value.into(),
        })
    }

    pub fn gte<T: Into<serde_json::Value>>(self, column: &str, value: T) -> Self {
        self.filter(DatabaseFilter {
            column: column.to_string(),
            operator: FilterOperator::Gte,
            value: value.into(),
        })
    }

    pub fn lt<T: Into<serde_json::Value>>(self, column: &str, value: T) -> Self {
        self.filter(DatabaseFilter {
            column: column.to_string(),
            operator: FilterOperator::Lt,
            value: value.into(),
        })
    }

    pub fn lte<T: Into<serde_json::Value>>(self, column: &str, value: T) -> Self {
        self.filter(DatabaseFilter {
            column: column.to_string(),
            operator: FilterOperator::Lte,
            value: value.into(),
        })
    }

    pub fn in_values<T: Into<serde_json::Value>>(self, column: &str, values: Vec<T>) -> Self {
        self.filter(DatabaseFilter {
            column: column.to_string(),
            operator: FilterOperator::In,
            value: values
                .into_iter()
                .map(|v| v.into())
                .collect::<Vec<_>>()
                .into(),
        })
    }

    // Add other filter methods (like, ilike, contains) if needed

    // --- Internal methods ---

    // /// Convert config to JSON for the websocket message
    // pub(crate) fn to_channel_config(&self) -> serde_json::Value {
    //     // ... implementation ...
    // }
}

/// ブロードキャストイベント監視設定
#[derive(Debug, Clone, Serialize)]
pub struct BroadcastChanges {
    event: String, // Specific event name to listen for
}

impl BroadcastChanges {
    pub fn new(event: &str) -> Self {
        Self {
            event: event.to_string(),
        }
    }

    #[allow(dead_code)] // Mark as allowed since it might be useful later
    pub(crate) fn get_event_name(&self) -> &str {
        &self.event
    }
}

/// プレゼンスイベント監視設定 (シンプルなマーカー型)
#[derive(Debug, Clone, Default, Serialize)]
pub struct PresenceChanges;

impl PresenceChanges {
    pub fn new() -> Self {
        // Self::default() // Clippy: default_constructed_unit_structs
        PresenceChanges // Create directly
    }
}

/// アクティブなチャンネル購読を表す
pub struct Subscription {
    id: String, // Internal subscription identifier
    channel: Arc<Channel>,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let id_clone = self.id.clone();
        let channel_clone = self.channel.clone();
        tokio::spawn(async move {
            if let Err(e) = channel_clone.unsubscribe(&id_clone).await {
                // TODO: Log unsubscribe error properly
                eprintln!("Error unsubscribing from channel: {e}");
            }
        });
    }
}

type CallbackFn = Box<dyn Fn(Payload) + Send + Sync>;
type PresenceCallbackFn = Box<dyn Fn(PresenceChange) + Send + Sync>;

/// 内部チャンネル表現
pub(crate) struct Channel {
    topic: String,
    client: Arc<RealtimeClient>, // Store Arc<RealtimeClient> for sending messages
    callbacks: Arc<RwLock<HashMap<String, CallbackFn>>>,
    presence_callbacks: Arc<RwLock<Vec<PresenceCallbackFn>>>,
    // Add channel state
    state: Arc<RwLock<ChannelState>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChannelState {
    Closed,
    Joining,
    Joined,
    Leaving,
    Errored,
}

impl Channel {
    pub(crate) fn new(topic: String, client: Arc<RealtimeClient>) -> Self {
        debug!("Channel::new created for topic: {topic}");
        Self {
            topic,
            client,
            callbacks: Arc::new(RwLock::new(HashMap::new())),
            presence_callbacks: Arc::new(RwLock::new(Vec::new())),
            state: Arc::new(RwLock::new(ChannelState::Closed)),
        }
    }

    async fn set_state(&self, state: ChannelState) {
        let mut current_state = self.state.write().await;
        if *current_state != state {
            info!(
                "Channel '{}' state changing from {:?} to {:?}",
                self.topic, *current_state, state
            );
            *current_state = state;
        } else {
            trace!(
                "Channel '{}' state already {:?}, not changing.",
                self.topic,
                state
            );
        }
    }

    // Simplified join - just sends the message
    async fn join(&self) -> Result<(), RealtimeError> {
        self.set_state(ChannelState::Joining).await;
        let join_ref = self.client.next_ref();
        info!(
            "Channel '{}' sending join message with ref {}",
            self.topic, join_ref
        );
        let join_msg = json!({
            "topic": self.topic,
            "event": ChannelEvent::PhoenixJoin,
            "payload": {},
            "ref": join_ref
        });
        // TODO: Add timeout for join reply
        self.client.send_message(join_msg).await
        // Need mechanism to wait for phx_reply with matching ref
    }

    // async fn send_message(&self, payload: serde_json::Value) -> Result<(), RealtimeError> {
    //    // ... implementation ...
    // }

    async fn unsubscribe(&self, id: &str) -> Result<(), RealtimeError> {
        // Remove callback
        self.callbacks.write().await.remove(id);
        // TODO: Unsubscribe presence if needed

        // Send unsubscribe message if this was the last callback? Requires tracking.
        // For simplicity, assume client handles full channel leave when all subscriptions drop.
        println!(
            "Subscription {} dropped. Channel {} might need explicit leave.",
            id, self.topic
        );
        Ok(())
    }

    // Adjusted to accept RealtimeMessage
    pub(crate) async fn handle_message(&self, message: RealtimeMessage) {
        debug!(
            "Channel '{}' handling message: event={:?}, ref={:?}",
            self.topic, message.event, message.message_ref
        );

        match message.event {
            ChannelEvent::PhoenixReply => {
                // TODO: Check ref against pending joins/leaves
                info!(
                    "Channel '{}' received PhoenixReply: {:?}",
                    self.topic, message.payload
                );
                if *self.state.read().await == ChannelState::Joining {
                    // Basic assumption: any reply means join succeeded for now
                    self.set_state(ChannelState::Joined).await;
                } else if *self.state.read().await == ChannelState::Leaving {
                    self.set_state(ChannelState::Closed).await;
                }
            }
            ChannelEvent::PhoenixClose => {
                info!(
                    "Channel '{}' received PhoenixClose. Setting state to Closed.",
                    self.topic
                );
                self.set_state(ChannelState::Closed).await;
            }
            ChannelEvent::PhoenixError => {
                error!(
                    "Channel '{}' received PhoenixError: {:?}",
                    self.topic, message.payload
                );
                self.set_state(ChannelState::Errored).await;
            }
            ChannelEvent::PostgresChanges | ChannelEvent::Broadcast | ChannelEvent::Presence => {
                // These events have nested data we need to pass to callbacks
                let payload = Payload {
                    data: message.payload.clone(), // Pass the whole payload as data for now
                    event_type: Some(message.event.to_string()), // Reflect the event type
                    timestamp: None, // Timestamp might be deeper in payload, needs parsing
                };
                trace!(
                    "Channel '{}' dispatching event {:?} to callbacks",
                    self.topic,
                    message.event
                );
                let callbacks_guard = self.callbacks.read().await;
                for callback in callbacks_guard.values() {
                    // Execute callback - Consider spawning if long-running
                    callback(payload.clone());
                }
                // TODO: Handle presence callbacks separately if event is Presence
            }
            // Ignore other events like Heartbeat, Insert, Update, Delete, All at the channel level
            // (Those might be relevant *inside* a PostgresChanges payload)
            _ => {
                trace!(
                    "Channel '{}' ignored event: {:?}",
                    self.topic,
                    message.event
                );
            }
        }
    }
}

/// チャンネル作成と購読設定のためのビルダー
pub struct ChannelBuilder<'a> {
    client: &'a RealtimeClient,
    topic: String,
    db_callbacks: HashMap<String, (DatabaseChanges, CallbackFn)>,
    broadcast_callbacks: HashMap<String, (BroadcastChanges, CallbackFn)>,
    presence_callbacks: Vec<PresenceCallbackFn>,
}

impl<'a> ChannelBuilder<'a> {
    pub(crate) fn new(client: &'a RealtimeClient, topic: &str) -> Self {
        debug!("ChannelBuilder::new for topic: {topic}");
        Self {
            client,
            topic: topic.to_string(),
            db_callbacks: HashMap::new(),
            broadcast_callbacks: HashMap::new(),
            presence_callbacks: Vec::new(),
        }
    }

    /// データベース変更イベントのコールバックを登録
    pub fn on<F>(mut self, changes: DatabaseChanges, callback: F) -> Self
    where
        F: Fn(Payload) + Send + Sync + 'static,
    {
        // Use a unique identifier for the subscription
        let id = uuid::Uuid::new_v4().to_string();
        self.db_callbacks.insert(id, (changes, Box::new(callback)));
        self
    }

    /// ブロードキャストイベントのコールバックを登録
    pub fn on_broadcast<F>(mut self, changes: BroadcastChanges, callback: F) -> Self
    where
        F: Fn(Payload) + Send + Sync + 'static,
    {
        let id = uuid::Uuid::new_v4().to_string();
        self.broadcast_callbacks
            .insert(id, (changes, Box::new(callback)));
        self
    }

    /// プレゼンス変更イベントのコールバックを登録
    pub fn on_presence<F>(mut self, callback: F) -> Self
    where
        F: Fn(PresenceChange) + Send + Sync + 'static,
    {
        self.presence_callbacks.push(Box::new(callback));
        self
    }

    /// チャンネルへの接続と購読を開始
    pub async fn subscribe(self) -> Result<Vec<Subscription>, RealtimeError> {
        info!("ChannelBuilder subscribing for topic: {}", self.topic);
        let client_arc = Arc::new(self.client.clone()); // Clone client Arcs into a new Arc for the Channel

        // Get or create the channel instance
        let mut channels_guard = client_arc.channels.write().await;
        let channel = channels_guard
            .entry(self.topic.clone())
            .or_insert_with(|| Arc::new(Channel::new(self.topic.clone(), client_arc.clone())))
            .clone();
        drop(channels_guard); // Release write lock
        debug!("Got or created Channel Arc for topic: {}", self.topic);

        let mut subscriptions = Vec::new();
        let mut callbacks_guard = channel.callbacks.write().await;
        let mut presence_callbacks_guard = channel.presence_callbacks.write().await;

        // Add database change callbacks
        for (id, (_changes, callback)) in self.db_callbacks {
            debug!("Adding DB callback ID {} to channel {}", id, self.topic);
            callbacks_guard.insert(id.clone(), callback);
            subscriptions.push(Subscription {
                id,
                channel: channel.clone(),
            });
        }

        // Add broadcast callbacks
        for (id, (_changes, callback)) in self.broadcast_callbacks {
            debug!(
                "Adding Broadcast callback ID {} to channel {}",
                id, self.topic
            );
            // Assuming broadcast uses the same callback mechanism for now
            callbacks_guard.insert(id.clone(), callback);
            subscriptions.push(Subscription {
                id,
                channel: channel.clone(),
            });
        }

        // Add presence callbacks
        for callback in self.presence_callbacks {
            debug!("Adding Presence callback to channel {}", self.topic);
            presence_callbacks_guard.push(callback);
            // How to represent presence subscription? Use a fixed ID?
            let id = format!("presence_{}", self.topic); // Example ID
            subscriptions.push(Subscription {
                id,
                channel: channel.clone(),
            });
        }

        drop(callbacks_guard);
        drop(presence_callbacks_guard);

        // Only send join if channel wasn't already joined/joining
        let current_state = *channel.state.read().await;
        if current_state == ChannelState::Closed || current_state == ChannelState::Errored {
            info!(
                "Channel '{}' is {:?}, attempting to join.",
                self.topic, current_state
            );
            match channel.join().await {
                Ok(_) => {
                    // Join message sent, now wait for reply (handled by reader task)
                    debug!(
                        "Join message sent for channel '{}'. Waiting for reply.",
                        self.topic
                    );
                    // We might want a timeout here to ensure the join completes
                    match timeout(Duration::from_secs(10), async {
                        while *channel.state.read().await != ChannelState::Joined {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            // Add a check for Errored or Closed state too
                            let check_state = *channel.state.read().await;
                            if check_state == ChannelState::Errored
                                || check_state == ChannelState::Closed
                            {
                                return Err(RealtimeError::SubscriptionError(format!(
                                    "Channel '{}' entered state {:?} while waiting for join reply",
                                    self.topic, check_state
                                )));
                            }
                        }
                        Ok(())
                    })
                    .await
                    {
                        Ok(Ok(_)) => info!("Channel '{}' successfully joined.", self.topic),
                        Ok(Err(e)) => {
                            error!(
                                "Error waiting for join confirmation for channel '{}': {:?}",
                                self.topic, e
                            );
                            return Err(e);
                        }
                        Err(_) => {
                            error!(
                                "Timed out waiting for join confirmation for channel '{}'",
                                self.topic
                            );
                            channel.set_state(ChannelState::Errored).await;
                            return Err(RealtimeError::SubscriptionError(format!(
                                "Timed out waiting for join confirmation for channel '{}'",
                                self.topic
                            )));
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to send join message for channel '{}': {}",
                        self.topic, e
                    );
                    channel.set_state(ChannelState::Errored).await;
                    return Err(e);
                }
            }
        } else {
            info!(
                "Channel '{}' is already {:?}, not sending join message.",
                self.topic, current_state
            );
        }

        info!(
            "ChannelBuilder subscribe finished for topic '{}', returning {} subscriptions.",
            self.topic,
            subscriptions.len()
        );
        Ok(subscriptions)
    }

    // Method to track presence - might belong on RealtimeClient or Channel directly?
    pub async fn track_presence(
        &self,
        _user_id: &str,
        _user_data: serde_json::Value,
    ) -> Result<(), RealtimeError> {
        // TODO: Implement sending presence track message
        Err(RealtimeError::ChannelError(
            "track_presence not implemented".to_string(),
        ))
    }
}
