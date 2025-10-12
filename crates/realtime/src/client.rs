use crate::channel::{Channel, ChannelBuilder}; // Added ChannelBuilder import
use crate::error::RealtimeError;
use crate::message::RealtimeMessage; // Added ChannelEvent import here
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::{broadcast, RwLock};
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, instrument, trace, warn};
use url::Url; // Import Rng trait for random number generation

/// 接続状態
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
}

/// RealtimeClient設定オプション
#[derive(Debug, Clone)]
pub struct RealtimeClientOptions {
    pub auto_reconnect: bool,
    pub max_reconnect_attempts: Option<u32>,
    pub reconnect_interval: u64,
    pub reconnect_backoff_factor: f64,
    pub max_reconnect_interval: u64,
    pub heartbeat_interval: u64,
}

impl Default for RealtimeClientOptions {
    fn default() -> Self {
        Self {
            auto_reconnect: true,
            max_reconnect_attempts: None, // Infinite attempts
            reconnect_interval: 1000,     // 1 second
            reconnect_backoff_factor: 1.5,
            max_reconnect_interval: 30000, // 30 seconds
            heartbeat_interval: 30000,     // 30 seconds
        }
    }
}

/// Realtimeクライアント本体
pub struct RealtimeClient {
    pub(crate) url: String,
    pub(crate) key: String,
    pub(crate) next_ref: AtomicU32,
    // Shared map of active channels (topic -> Channel)
    pub(crate) channels: Arc<RwLock<HashMap<String, Arc<Channel>>>>,
    // Shared sender for the WebSocket task
    pub(crate) socket: Arc<RwLock<Option<mpsc::Sender<Message>>>>,
    pub(crate) options: RealtimeClientOptions,
    state: Arc<RwLock<ConnectionState>>,
    reconnect_attempts: AtomicU32,
    // Wrap AtomicBool in Arc for sharing across tasks
    is_manually_closed: Arc<AtomicBool>,
    state_change: broadcast::Sender<ConnectionState>,
    // Make token field accessible within the crate
    pub(crate) access_token: Arc<RwLock<Option<String>>>,
}

impl RealtimeClient {
    /// デフォルトオプションで新しいクライアントを作成
    #[instrument(skip(key))]
    pub fn new(url: &str, key: &str) -> Self {
        info!("Creating new RealtimeClient");
        Self::new_with_options(url, key, RealtimeClientOptions::default())
    }

    /// カスタムオプションで新しいクライアントを作成
    #[instrument(skip(key))]
    pub fn new_with_options(url: &str, key: &str, options: RealtimeClientOptions) -> Self {
        info!("Creating new RealtimeClient with options: {:?}", options);
        let (state_change_tx, _) = broadcast::channel(16); // Channel for state changes
        Self {
            url: url.to_string(),
            key: key.to_string(),
            next_ref: AtomicU32::new(1),
            channels: Arc::new(RwLock::new(HashMap::new())),
            socket: Arc::new(RwLock::new(None)),
            options,
            state: Arc::new(RwLock::new(ConnectionState::Disconnected)),
            reconnect_attempts: AtomicU32::new(0),
            // Initialize the Arc<AtomicBool>
            is_manually_closed: Arc::new(AtomicBool::new(false)),
            state_change: state_change_tx,
            // Initialize token as None
            access_token: Arc::new(RwLock::new(None)),
        }
    }

    /// Method to set the authentication token
    #[instrument(skip(self, token))]
    pub async fn set_auth(&self, token: Option<String>) {
        info!("Setting auth token (is_some: {})", token.is_some());
        let mut current_token = self.access_token.write().await;
        *current_token = token;
        // TODO: Handle auth update while connected
    }

    /// 接続状態変更の通知を受け取るためのレシーバーを取得
    #[instrument(skip(self))]
    pub fn on_state_change(&self) -> broadcast::Receiver<ConnectionState> {
        debug!("Subscribing to state changes");
        self.state_change.subscribe()
    }

    /// 現在の接続状態を取得
    #[instrument(skip(self))]
    pub async fn get_connection_state(&self) -> ConnectionState {
        let state = *self.state.read().await;
        debug!(?state, "Getting current connection state");
        state
    }

    /// 特定のトピックに対するチャンネルビルダーを作成
    #[instrument(skip(self))]
    pub fn channel(&self, topic: &str) -> ChannelBuilder<'_> {
        info!(?topic, "Creating channel builder");
        ChannelBuilder::new(self, topic)
    }

    /// 次のメッセージ参照番号を生成
    pub(crate) fn next_ref(&self) -> String {
        let next = self.next_ref.fetch_add(1, Ordering::SeqCst);
        trace!(next_ref = next, "Generated next ref");
        next.to_string()
    }

    /// 内部接続状態を設定し、変更を通知
    async fn set_connection_state(&self, state: ConnectionState) {
        let mut current_state = self.state.write().await;
        if *current_state != state {
            info!(from = ?*current_state, to = ?state, "Client state changing");
            *current_state = state;
            // Ignore send error if no receivers are listening
            if let Err(e) = self.state_change.send(state) {
                warn!(error = %e, ?state, "Failed to broadcast state change");
            }
        } else {
            trace!(?state, "Client state already set, not changing.");
        }
    }

    /// WebSocket接続を開始および管理するタスク
    #[instrument(skip(self))]
    pub fn connect(
        &self,
    ) -> impl std::future::Future<Output = Result<(), RealtimeError>> + Send + 'static {
        info!("Connect task initiated");
        // Clone necessary Arcs and fields for the async task
        let url = self.url.clone();
        let key = self.key.clone();
        let socket_arc = self.socket.clone();
        let state_arc = self.state.clone();
        let state_change_tx = self.state_change.clone();
        let _channels_arc = self.channels.clone();
        let options = self.options.clone();
        let is_manually_closed_arc = self.is_manually_closed.clone();
        let token_arc = self.access_token.clone(); // Clone token Arc

        async move {
            info!("Connect task initiated");
            is_manually_closed_arc.store(false, Ordering::SeqCst);
            debug!("Reset manual close flag");

            let token_guard = token_arc.read().await;
            let token_param = token_guard
                .as_ref()
                .map(|t| format!("&token={t}"))
                .unwrap_or_default();
            debug!(token_present = token_guard.is_some(), "Read auth token");
            drop(token_guard); // Release lock

            let base_url = match Url::parse(&url) {
                Ok(u) => u,
                Err(e) => {
                    error!(url = %url, error = %e, "Failed to parse base URL");
                    // Ensure state is set before returning
                    Self::set_connection_state_internal(
                        state_arc.clone(),
                        state_change_tx.clone(),
                        ConnectionState::Disconnected,
                    )
                    .await;
                    return Err(RealtimeError::UrlParseError(e));
                }
            };
            debug!(url = %base_url, "Parsed base URL");
            // Allow ws/wss schemes directly, map http/https
            match base_url.scheme() {
                "http" | "ws" | "https" | "wss" => { /* Ok */ }
                // Reject other schemes
                s => {
                    error!(scheme = %s, "Unsupported URL scheme");
                    Self::set_connection_state_internal(
                        state_arc.clone(),
                        state_change_tx.clone(),
                        ConnectionState::Disconnected,
                    )
                    .await;
                    return Err(RealtimeError::ConnectionError(format!(
                        "Unsupported URL scheme: {s}"
                    )));
                }
            };

            // Use the correct path /realtime/v1/websocket
            let _host = match base_url.host_str() {
                Some(h) => h, // Directly use the host string if Some
                None => {
                    // Handle the None case
                    error!(url = %base_url, "Failed to get host from URL (no host)");
                    Self::set_connection_state_internal(
                        state_arc.clone(),
                        state_change_tx.clone(),
                        ConnectionState::Disconnected,
                    )
                    .await;
                    return Err(RealtimeError::UrlParseError(url::ParseError::EmptyHost));
                    // Or a more specific error
                }
            };
            let ws_url = match base_url.join("/realtime/v1/websocket?vsn=2.0.0") {
                Ok(mut joined_url) => {
                    joined_url
                        .query_pairs_mut()
                        .append_pair("apikey", &key)
                        .append_pair("token", token_param.trim_start_matches("&token=")); // Add token if present
                    info!(url = %joined_url, "Constructed WebSocket URL");
                    joined_url.to_string()
                }
                Err(e) => {
                    error!(error = %e, base_url = %base_url, "Failed to join path to base URL");
                    Self::set_connection_state_internal(
                        state_arc.clone(),
                        state_change_tx.clone(),
                        ConnectionState::Disconnected,
                    )
                    .await;
                    return Err(RealtimeError::UrlParseError(e));
                }
            };

            info!(url = %ws_url, "Attempting to connect to WebSocket");

            Self::set_connection_state_internal(
                state_arc.clone(),
                state_change_tx.clone(),
                ConnectionState::Connecting,
            )
            .await;

            let connect_result = connect_async(&ws_url).await; // Store result
            let ws_stream = match connect_result {
                Ok((stream, response)) => {
                    info!(response = ?response, "WebSocket connection successful");
                    stream
                }
                Err(e) => {
                    error!(error = %e, url = %ws_url, "WebSocket connection failed");
                    // Set state before returning error
                    Self::set_connection_state_internal(
                        state_arc.clone(),
                        state_change_tx.clone(),
                        ConnectionState::Disconnected,
                    )
                    .await;
                    return Err(RealtimeError::ConnectionError(format!(
                        "WebSocket connection failed: {e}"
                    )));
                }
            };

            Self::set_connection_state_internal(
                state_arc.clone(),
                state_change_tx.clone(),
                ConnectionState::Connected,
            )
            .await;

            let (write, read) = ws_stream.split();
            debug!("WebSocket stream split into writer and reader");

            let (socket_tx, socket_rx) = mpsc::channel::<Message>(100);
            *socket_arc.write().await = Some(socket_tx.clone()); // Clone for writer task
            debug!("Internal MPSC channel created, sender stored");

            // --- WebSocket Writer Task ---
            let writer_socket_arc = socket_arc.clone();
            let writer_state_arc = state_arc.clone();
            let writer_state_change_tx = state_change_tx.clone();
            let _writer_handle = tokio::spawn(async move {
                // Add instrument to writer task
                #[instrument(skip_all, name = "ws_writer")]
                async fn writer_task(
                    mut write: impl SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error>
                        + Unpin,
                    mut socket_rx: mpsc::Receiver<Message>,
                    writer_socket_arc: Arc<RwLock<Option<mpsc::Sender<Message>>>>,
                    writer_state_arc: Arc<RwLock<ConnectionState>>,
                    writer_state_change_tx: broadcast::Sender<ConnectionState>,
                    heartbeat_interval_ms: u64,
                ) {
                    info!("Writer task started");
                    let heartbeat_interval = Duration::from_millis(heartbeat_interval_ms);
                    let mut heartbeat_timer = tokio::time::interval(heartbeat_interval);

                    loop {
                        tokio::select! {
                            // Read from internal MPSC channel
                            Some(msg) = socket_rx.recv() => {
                                trace!(message = ?msg, "Sending message via WebSocket");
                                if let Err(e) = write.send(msg).await {
                                    error!(error = %e, "Failed to send message via WebSocket");
                                    // Indicate disconnection on send error
                                    {
                                        let mut current_state = writer_state_arc.write().await;
                                        if *current_state != ConnectionState::Disconnected {
                                             info!(from = ?*current_state, to = ?ConnectionState::Disconnected, "Writer: Setting state Disconnected on send error");
                                            *current_state = ConnectionState::Disconnected;
                                            let _ = writer_state_change_tx.send(ConnectionState::Disconnected);
                                        }
                                    }
                                    break;
                                }
                            }
                            // Send heartbeat
                            _ = heartbeat_timer.tick() => {
                                let heartbeat_ref = format!("hb-{}", rand::thread_rng().gen::<u32>());
                                let heartbeat_msg = json!({
                                    "topic": "phoenix",
                                    "event": "heartbeat",
                                    "payload": {},
                                    "ref": heartbeat_ref
                                });
                                trace!(heartbeat_ref = %heartbeat_ref, "Sending heartbeat");
                                if let Err(e) = write.send(Message::Text(heartbeat_msg.to_string())).await {
                                    error!(error = %e, "Failed to send heartbeat");
                                    // Update state directly using captured Arcs
                                    {
                                        let mut current_state = writer_state_arc.write().await;
                                        if *current_state != ConnectionState::Disconnected {
                                             info!(from = ?*current_state, to = ?ConnectionState::Disconnected, "Writer: Setting state Disconnected on heartbeat error");
                                            *current_state = ConnectionState::Disconnected;
                                            let _ = writer_state_change_tx.send(ConnectionState::Disconnected);
                                        }
                                    }
                                    break;
                                }
                            }
                            else => {
                                info!("Writer loop finished (select exhausted)");
                                break;
                            }
                        }
                    }
                    info!("Writer task finished");
                    // Clear socket sender when task finishes
                    *writer_socket_arc.write().await = None;
                }
                writer_task(
                    write,
                    socket_rx,
                    writer_socket_arc,
                    writer_state_arc,
                    writer_state_change_tx,
                    options.heartbeat_interval,
                )
                .await;
            });

            // --- WebSocket Reader Task ---
            let reader_socket_arc = socket_arc.clone();
            let reader_state_arc = state_arc.clone();
            let reader_state_change_tx = state_change_tx.clone();
            let reader_channels_arc = _channels_arc.clone(); // Use the cloned channels arc
            let reader_reconnect_attempts = Arc::new(AtomicU32::new(0)); // Use new Arc for reader's attempts
            let reader_options = options.clone();
            let reader_is_manually_closed = is_manually_closed_arc.clone();
            let _reader_handle = tokio::spawn(async move {
                // Add instrument to reader task
                // Remove the instrument macro to avoid too_many_arguments error for now
                // #[instrument(skip_all, name = "ws_reader")]
                #[allow(clippy::too_many_arguments)] // Allow > 7 arguments for this task
                async fn reader_task(
                    mut read: impl StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
                        + Unpin,
                    reader_channels_arc: Arc<RwLock<HashMap<String, Arc<Channel>>>>,
                    reader_socket_arc: Arc<RwLock<Option<mpsc::Sender<Message>>>>, // Need socket to potentially rejoin
                    reader_state_arc: Arc<RwLock<ConnectionState>>,
                    reader_state_change_tx: broadcast::Sender<ConnectionState>,
                    _reader_reconnect_attempts: Arc<AtomicU32>, // Prefix unused parameter
                    reader_options: RealtimeClientOptions,      // Pass options
                    reader_is_manually_closed: Arc<AtomicBool>,
                ) {
                    info!("Reader task started");
                    while let Some(result) = read.next().await {
                        match result {
                            Ok(msg) => {
                                trace!(message = ?msg, "Received message from WebSocket");
                                match msg {
                                    Message::Text(text) => {
                                        match serde_json::from_str::<RealtimeMessage>(&text) {
                                            Ok(parsed_msg) => {
                                                trace!(message = ?parsed_msg, "Parsed RealtimeMessage");
                                                // Route message to appropriate channel
                                                let channels = reader_channels_arc.read().await;
                                                if let Some(channel) =
                                                    channels.get(&parsed_msg.topic)
                                                {
                                                    channel.handle_message(parsed_msg).await;
                                                }
                                                // TODO: Handle phoenix-level messages (e.g., replies)
                                            }
                                            Err(e) => {
                                                error!(error = %e, raw_message = %text, "Failed to parse RealtimeMessage");
                                            }
                                        }
                                    }
                                    Message::Close(close_frame) => {
                                        info!(frame = ?close_frame, "Received WebSocket Close frame");
                                        break; // Exit loop on Close frame
                                    }
                                    Message::Ping(ping_data) => {
                                        trace!(data = ?ping_data, "Received Ping, sending Pong");
                                        // Try sending Pong via the writer task's MPSC channel
                                        if let Some(tx) = reader_socket_arc.read().await.as_ref() {
                                            if let Err(e) = tx.send(Message::Pong(ping_data)).await
                                            {
                                                error!(error = %e, "Failed to queue Pong message");
                                            }
                                        } else {
                                            warn!("Socket sender not available to send Pong");
                                        }
                                    }
                                    Message::Pong(_) => {
                                        trace!("Received Pong");
                                        // Heartbeat mechanism usually handles this
                                    }
                                    Message::Binary(_) => {
                                        warn!("Received unexpected Binary message");
                                    }
                                    Message::Frame(_) => {
                                        // Raw frame, usually not handled directly
                                        trace!("Received low-level Frame");
                                    }
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "WebSocket read error");
                                break; // Exit loop on read error
                            }
                        }
                    }
                    info!("Reader loop finished");

                    // Connection closed, check if it was manual or needs reconnect
                    if !reader_is_manually_closed.load(Ordering::SeqCst)
                        && reader_options.auto_reconnect
                    {
                        warn!("WebSocket connection lost unexpectedly, attempting reconnect...");
                        // Update state directly using captured Arcs
                        {
                            let mut current_state = reader_state_arc.write().await;
                            if *current_state != ConnectionState::Reconnecting {
                                info!(from = ?*current_state, to = ?ConnectionState::Reconnecting, "Reader: Setting state Reconnecting");
                                *current_state = ConnectionState::Reconnecting;
                                let _ = reader_state_change_tx.send(ConnectionState::Reconnecting);
                            }
                        }
                        // Spawn reconnect task (consider moving reconnect logic outside reader)
                        // TODO: Implement proper reconnect logic here using reader_options and reader_reconnect_attempts
                        // For now, just set state to disconnected
                        warn!(
                            "Reconnect logic not fully implemented, setting state to Disconnected"
                        );
                        // Update state directly using captured Arcs
                        {
                            let mut current_state = reader_state_arc.write().await;
                            if *current_state != ConnectionState::Disconnected {
                                info!(from = ?*current_state, to = ?ConnectionState::Disconnected, "Reader: Setting state Disconnected (reconnect N/A)");
                                *current_state = ConnectionState::Disconnected;
                                let _ = reader_state_change_tx.send(ConnectionState::Disconnected);
                            }
                        }
                    } else {
                        info!("WebSocket connection closed (manual or auto_reconnect=false)");
                        // Update state directly using captured Arcs
                        {
                            let mut current_state = reader_state_arc.write().await;
                            if *current_state != ConnectionState::Disconnected {
                                info!(from = ?*current_state, to = ?ConnectionState::Disconnected, "Reader: Setting state Disconnected (manual/no-reconnect)");
                                *current_state = ConnectionState::Disconnected;
                                let _ = reader_state_change_tx.send(ConnectionState::Disconnected);
                            }
                        }
                    }
                    // Clear socket sender after reader finishes too
                    *reader_socket_arc.write().await = None;
                    info!("Reader task finished");
                }
                reader_task(
                    read,
                    reader_channels_arc,
                    reader_socket_arc,
                    reader_state_arc,
                    reader_state_change_tx,
                    reader_reconnect_attempts,
                    reader_options,
                    reader_is_manually_closed,
                )
                .await;
            });

            info!("Connect task completed successfully (connection established, reader/writer tasks spawned)");
            // Note: The outer future completes here, but the reader/writer tasks continue.
            Ok(())
        }
    }

    /// Helper for setting state internally, avoiding self borrow issues.
    #[instrument(skip(state_arc, state_change_tx), fields(state = ?state))]
    async fn set_connection_state_internal(
        state_arc: Arc<RwLock<ConnectionState>>,
        state_change_tx: broadcast::Sender<ConnectionState>,
        state: ConnectionState,
    ) {
        let mut current_state = state_arc.write().await;
        if *current_state != state {
            info!(from = ?*current_state, to = ?state, "Internal state changing");
            *current_state = state;
            if let Err(e) = state_change_tx.send(state) {
                warn!(error = %e, state = ?state, "Failed to broadcast internal state change");
            }
        } else {
            trace!(state = ?state, "Internal state already set, not changing.");
        }
    }

    /// WebSocket接続を切断
    #[instrument(skip(self))]
    pub async fn disconnect(&self) -> Result<(), RealtimeError> {
        info!("disconnect() called");
        self.is_manually_closed.store(true, Ordering::SeqCst);
        debug!("Set manual close flag");

        self.set_connection_state(ConnectionState::Disconnected)
            .await;

        let mut socket_guard = self.socket.write().await;
        if let Some(socket_tx) = socket_guard.take() {
            info!("Closing WebSocket connection via MPSC channel");
            // Send a close message or signal the writer task to close the WebSocket
            // Option 1: Send a specific marker message (if writer handles it)
            // Option 2: Drop the sender - writer loop will detect channel closed
            // Dropping the sender is simpler if the writer loop handles it correctly.
            drop(socket_tx); // Drop sender, writer task should detect this
            debug!("Dropped MPSC sender to signal writer task termination");
            // TODO: Ensure reader/writer tasks properly terminate after this.
            // Currently relies on tasks detecting channel closure or socket close.
        } else {
            info!("disconnect() called but no active socket sender found (already disconnected?)");
        }
        drop(socket_guard);

        // Clean up channels (optional, depends on desired behavior on disconnect)
        // let mut channels = self.channels.write().await;
        // channels.clear();
        // info!("Cleared active channels");

        info!("disconnect() finished");
        Ok(())
    }

    /// 再接続ロジック
    #[instrument(skip(self))]
    fn reconnect(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        info!("reconnect() called");
        let self_clone = self.clone(); // Clone self for the async task
        async move {
            let mut attempts = self_clone.reconnect_attempts.fetch_add(1, Ordering::SeqCst);
            info!(attempts, "Reconnect attempt initiated");

            if let Some(max_attempts) = self_clone.options.max_reconnect_attempts {
                if attempts >= max_attempts {
                    error!(max_attempts, "Max reconnect attempts reached. Giving up.");
                    self_clone
                        .set_connection_state(ConnectionState::Disconnected)
                        .await;
                    return;
                }
            }

            let interval_ms = std::cmp::min(
                self_clone.options.max_reconnect_interval,
                (self_clone.options.reconnect_interval as f64
                    * self_clone
                        .options
                        .reconnect_backoff_factor
                        .powi(attempts as i32)) as u64,
            );
            let interval = Duration::from_millis(interval_ms);
            info!(interval = ?interval, "Waiting before next reconnect attempt");

            sleep(interval).await;

            info!("Attempting to reconnect...");
            match self_clone.connect().await {
                Ok(_) => {
                    info!("Reconnect successful!");
                    self_clone.reconnect_attempts.store(0, Ordering::SeqCst); // Reset attempts on success
                                                                              // State should be set to Connected by the connect task
                }
                Err(e) => {
                    error!(error = %e, attempts, "Reconnect attempt failed");
                    // State should be handled by the failed connect attempt
                    // Spawn another reconnect task if allowed
                    if self_clone.options.auto_reconnect {
                        // Check attempts again before scheduling next reconnect
                        attempts = self_clone.reconnect_attempts.load(Ordering::SeqCst); // Reload current attempts
                        if let Some(max_attempts) = self_clone.options.max_reconnect_attempts {
                            if attempts >= max_attempts {
                                warn!("Max reconnect attempts reached after failed attempt.");
                                return; // Already handled setting state to Disconnected
                            }
                        }
                        warn!("Scheduling next reconnect attempt...");
                        tokio::spawn(self_clone.reconnect());
                    }
                }
            }
            info!("reconnect() task finished for this attempt");
        }
    }

    /// メッセージをWebSocket経由で送信 (内部利用)
    #[instrument(skip(self, message))]
    pub(crate) async fn send_message(
        &self,
        message: serde_json::Value,
    ) -> Result<(), RealtimeError> {
        let msg_text = message.to_string();
        trace!(message = %msg_text, "Preparing to send message");
        let ws_message = Message::Text(msg_text);

        let socket_guard = self.socket.read().await;
        if let Some(socket_tx) = socket_guard.as_ref() {
            debug!("Sending message via MPSC channel to writer task");
            socket_tx.send(ws_message).await.map_err(|e| {
                error!(error = %e, "Failed to send message via MPSC channel");
                RealtimeError::ConnectionError(format!(
                    "Failed to send message via MPSC channel: {e}"
                ))
            })
        } else {
            error!("Cannot send message: WebSocket sender not available (not connected?)");
            Err(RealtimeError::ConnectionError(
                "WebSocket sender not available (not connected?)".to_string(),
            ))
        }
    }
}

impl Clone for RealtimeClient {
    fn clone(&self) -> Self {
        // Increment Arc counts for shared state
        Self {
            url: self.url.clone(),
            key: self.key.clone(),
            next_ref: AtomicU32::new(self.next_ref.load(Ordering::SeqCst)), // Clone current value
            channels: self.channels.clone(),
            socket: self.socket.clone(),
            options: self.options.clone(),
            state: self.state.clone(),
            reconnect_attempts: AtomicU32::new(self.reconnect_attempts.load(Ordering::SeqCst)),
            is_manually_closed: self.is_manually_closed.clone(),
            state_change: self.state_change.clone(),
            access_token: self.access_token.clone(),
        }
    }
}

// Convert MPSC send error to RealtimeError
impl From<tokio::sync::mpsc::error::SendError<Message>> for RealtimeError {
    fn from(err: tokio::sync::mpsc::error::SendError<Message>) -> Self {
        RealtimeError::ConnectionError(format!("Failed to send message to socket task: {err}"))
    }
}
