// src/client.rs

// Reverting to original structure with v0.2.0 path dependencies
// and stubbing out problematic implementations.

use crate::error::{Result, SupabaseError};
use crate::models::{AuthCredentials, Item, User};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

// Correct imports based on crate structure
use reqwest::Client as ReqwestClient;
use supabase_rust_auth::AuthOptions;
use supabase_rust_auth::{Auth, AuthError, Session as AuthSession};
use supabase_rust_functions::FunctionsClient;
use supabase_rust_postgrest::{PostgrestClient, PostgrestError};
use supabase_rust_realtime::RealtimeClient;
use supabase_rust_storage::StorageClient;

use tokio::sync::{mpsc, Mutex};
use url::Url;
use uuid::Uuid;

/// Configuration for the Supabase client.
/// It's recommended to load these values from environment variables or a secure config source.
#[derive(Debug, Clone)]
pub struct SupabaseConfig {
    pub url: Url,
    pub anon_key: String,
}

impl SupabaseConfig {
    /// Creates a new configuration, validating the URL.
    pub fn new(url_str: &str, anon_key: String) -> Result<Self> {
        let url = Url::parse(url_str).map_err(SupabaseError::UrlParse)?;
        if anon_key.is_empty() {
            return Err(SupabaseError::Config(
                "anon_key cannot be empty".to_string(),
            ));
        }
        Ok(Self { url, anon_key })
    }

    /// Attempts to create configuration from environment variables.
    pub fn from_env() -> Result<Self> {
        let url_str = std::env::var("SUPABASE_URL").map_err(|_| {
            SupabaseError::Config("SUPABASE_URL environment variable not found".to_string())
        })?;
        let anon_key = std::env::var("SUPABASE_ANON_KEY").map_err(|_| {
            SupabaseError::Config("SUPABASE_ANON_KEY environment variable not found".to_string())
        })?;
        Self::new(&url_str, anon_key)
    }
}

/// Represents the different types of changes received from a realtime subscription.
#[derive(Debug, Clone, PartialEq)]
pub enum ItemChange {
    Insert(Item),
    Update(Item),
    Delete(HashMap<String, Value>),
    Error(String),
}

/// Wraps Supabase sub-clients and manages configuration/state.
#[derive(Clone)]
pub struct SupabaseClientWrapper {
    config: Arc<SupabaseConfig>,
    http_client: ReqwestClient,
    pub auth: Arc<Auth>,
    pub functions: Arc<FunctionsClient>,
    pub realtime: Arc<RealtimeClient>,
    pub storage: Arc<StorageClient>,
    current_session: Arc<Mutex<Option<AuthSession>>>,
}

impl SupabaseClientWrapper {
    pub fn from(&self, table: &str) -> PostgrestClient {
        PostgrestClient::new(
            self.config.url.as_str(),
            self.config.anon_key.as_str(),
            table,
            self.http_client.clone(),
        )
    }
}

impl SupabaseClientWrapper {
    /// Creates a new Supabase client wrapper from configuration.
    pub fn new(config: SupabaseConfig) -> Result<Self> {
        let http_client = ReqwestClient::builder()
            .build()
            .map_err(SupabaseError::Network)?;

        let auth_client = Auth::new(
            config.url.as_str(),
            &config.anon_key,
            http_client.clone(),
            AuthOptions::default(),
        );

        let functions_client =
            FunctionsClient::new(config.url.as_str(), &config.anon_key, http_client.clone());

        let mut rt_url_builder = config.url.clone();
        let scheme = if config.url.scheme() == "https" {
            "wss"
        } else {
            "ws"
        };
        rt_url_builder.set_scheme(scheme).map_err(|_| {
            SupabaseError::Initialization("Failed to set scheme for Realtime URL".to_string())
        })?;
        let rt_url = rt_url_builder.join("realtime/v1").map_err(|e| {
            SupabaseError::Initialization(format!("Failed to construct Realtime URL: {e}"))
        })?;
        let realtime_client = RealtimeClient::new(rt_url.as_ref(), &config.anon_key);

        let storage_client =
            StorageClient::new(config.url.as_str(), &config.anon_key, http_client.clone());

        println!("Supabase client initialized (Auth & Realtime - Postgrest on demand).");

        Ok(Self {
            config: Arc::new(config),
            http_client,
            auth: Arc::new(auth_client),
            functions: Arc::new(functions_client),
            realtime: Arc::new(realtime_client),
            storage: Arc::new(storage_client),
            current_session: Arc::new(Mutex::new(None)),
        })
    }

    /// Convenience function to create a client directly from environment variables.
    pub fn from_env() -> Result<Self> {
        let config = SupabaseConfig::from_env()?;
        Self::new(config)
    }

    /// Authenticates a user using email and password.
    /// Corresponds to `authenticateUser` in the SSOT.
    /// Returns the Supabase User details on success.
    pub async fn authenticate(&self, credentials: AuthCredentials) -> Result<User> {
        println!(
            "[IMPL] Attempting to authenticate user: {}",
            credentials.email
        ); // Changed STUB to IMPL for clarity
        match self
            .auth
            .sign_in_with_password(&credentials.email, &credentials.password)
            .await
        {
            Ok(session) => {
                // Authentication successful, store the session
                let mut session_guard = self.current_session.lock().await;
                *session_guard = Some(session.clone()); // Clone session to store and return user
                println!(
                    "[IMPL] Authentication successful for user: {}",
                    session.user.id
                );
                Ok(session.user.into()) // Convert auth::User to models::User
            }
            Err(e) => {
                // Authentication failed
                eprintln!("[IMPL] Authentication failed: {e:?}"); // Use eprintln for errors
                Err(SupabaseError::Auth(e)) // Map the AuthError to SupabaseError
            }
        }
    }

    /// Logs out the currently authenticated user by invalidating the session/token.
    /// Corresponds to `logoutUser` in the SSOT.
    pub async fn logout(&self) -> Result<()> {
        println!("[STUB] Attempting to log out user");
        unimplemented!("Logout logic needs fixing for v0.2.0 API");
    }

    /// Fetches 'items' from the database.
    /// Requires authentication.
    /// Corresponds to `fetchItemsFromSupabase` in the SSOT.
    pub async fn fetch_items(&self) -> Result<Vec<Item>> {
        println!("[IMPL] Attempting to fetch items");
        let token = self.get_auth_token().await?;

        let client = supabase_rust_postgrest::PostgrestClient::new(
            self.config.url.as_str(),
            &self.config.anon_key,
            "items",
            self.http_client.clone(),
        )
        .with_auth(&token)?;

        // execute<T>() deserializes into Vec<T>
        client
            .select("*")
            .execute::<Item>() // T is Item, returns Result<Vec<Item>, PostgrestError>
            .await
            .map_err(SupabaseError::Postgrest)
    }

    /// Subscribes to item changes.
    /// Corresponds to `subscribeToItemChanges` in the SSOT.
    pub async fn subscribe_to_item_changes(&self) -> Result<mpsc::UnboundedReceiver<ItemChange>> {
        println!("[STUB] Attempting to subscribe to item changes");
        unimplemented!("Realtime subscription logic needs fixing for v0.2.0 API");
    }

    // --- CRUD Operations for Items ---

    /// Creates a new item in the database.
    /// Requires authentication.
    pub async fn create_item(&self, new_item: Item) -> Result<Item> {
        println!("[IMPL] Attempting to create item");
        let token = self.get_auth_token().await?;

        let client = supabase_rust_postgrest::PostgrestClient::new(
            self.config.url.as_str(),
            &self.config.anon_key,
            "items",
            self.http_client.clone(),
        )
        .with_auth(&token)?;

        // insert() returns a Future<Output = Result<Value, PostgrestError>>
        let response_value = client
            .insert(vec![new_item])
            .await // Await the future directly
            .map_err(SupabaseError::Postgrest)?;

        // Parse the serde_json::Value into Vec<Item>
        // Postgrest insert with return=representation returns an array
        let mut created_items: Vec<Item> =
            serde_json::from_value(response_value).map_err(SupabaseError::Json)?; // Map serde_json::Error using #[from]

        // Extract the first item
        created_items.pop().ok_or_else(|| {
            // Use PostgrestError::DeserializationError when parsing is ok but result is empty/unexpected
            SupabaseError::Postgrest(PostgrestError::DeserializationError(
                "No item data returned after insert".to_string(),
            ))
        })
    }

    /// Fetches a single 'item' by its ID.
    pub async fn fetch_item_by_id(&self, _item_id: Uuid) -> Result<Option<Item>> {
        println!("[STUB] Attempting to fetch item by ID");
        unimplemented!("Postgrest fetch single logic needs fixing for v0.2.0 API");
    }

    /// Updates an existing 'item' by its ID.
    pub async fn update_item(&self, _item_id: Uuid, _item_update: Item) -> Result<Item> {
        println!("[STUB] Attempting to update item");
        unimplemented!("Postgrest update logic needs fixing for v0.2.0 API");
    }

    /// Deletes an 'item' by its ID.
    pub async fn delete_item(&self, _item_id: Uuid) -> Result<()> {
        println!("[STUB] Attempting to delete item");
        unimplemented!("Postgrest delete logic needs fixing for v0.2.0 API");
    }

    #[allow(dead_code)] // Allowed because methods using it are stubbed
    async fn get_auth_token(&self) -> Result<String> {
        let session_guard = self.current_session.lock().await;
        session_guard
            .as_ref()
            .map(|s| s.access_token.clone()) // Use map() instead of and_then(Some())
            .ok_or_else(|| {
                SupabaseError::Auth(AuthError::ApiError("Missing session token".to_string()))
            })
    }

    // --- Test-only Helper ---
    pub async fn set_session_for_test(&self, session: Option<AuthSession>) {
        let mut session_guard = self.current_session.lock().await;
        *session_guard = session;
    }

    // --- Public Getters ---
    /// Returns the Supabase Anon Key used by the client.
    pub fn anon_key(&self) -> &str {
        &self.config.anon_key
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Import items from parent module
    use dotenv::dotenv;

    #[test]
    fn config_new_valid() {
        dotenv().ok(); // Load .env file for testing if available

        // Temporarily set dummy env vars for this test
        let url = "http://localhost:12345";
        let key = "dummy-anon-key";
        std::env::set_var("SUPABASE_URL", url);
        std::env::set_var("SUPABASE_ANON_KEY", key);

        // Test creating config with the (now set) env vars
        // let url_from_env = std::env::var("SUPABASE_URL").expect("SUPABASE_URL must be set for tests");
        // let key_from_env =
        //     std::env::var("SUPABASE_ANON_KEY").expect("SUPABASE_ANON_KEY must be set for tests");
        // Use the values directly now that we set them
        let config = SupabaseConfig::new(url, key.to_string()).unwrap();

        // Fix: Url::parse adds a trailing slash if missing path, format! needs literal and arg
        // Use the original URL value for comparison
        assert_eq!(config.url.to_string(), format!("{url}/"));
        assert_eq!(config.anon_key, key);

        // Optional: Unset the vars? Generally not needed as it affects only this process.
        // std::env::remove_var("SUPABASE_URL");
        // std::env::remove_var("SUPABASE_ANON_KEY");
    }

    #[test]
    fn config_new_invalid_url() {
        let url = "not a valid url";
        let key = "some_anon_key";
        let config = SupabaseConfig::new(url, key.to_string());
        assert!(config.is_err());
        match config.err().unwrap() {
            SupabaseError::UrlParse(_) => {} // Expected error
            _ => panic!("Expected UrlParse error"),
        }
    }

    #[test]
    fn config_new_empty_key() {
        let url = "http://localhost:54321";
        let key = "";
        let config = SupabaseConfig::new(url, key.to_string());
        assert!(config.is_err());
        match config.err().unwrap() {
            SupabaseError::Config(msg) => assert!(msg.contains("anon_key cannot be empty")),
            _ => panic!("Expected Config error for empty key"),
        }
    }

    // Add tests for SupabaseConfig::from_env() - requires setting env vars for test
    // This might be better suited for integration tests or require helper libraries.
}
