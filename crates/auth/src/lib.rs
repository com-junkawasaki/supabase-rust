//! Supabase Auth client for Rust
//!
//! This crate provides authentication functionality for Supabase,
//! including sign up, sign in, session management, and user operations.

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::RwLock;
use thiserror::Error;

/// エラー型
#[derive(Error, Debug)]
pub enum AuthError {
    #[error("API error: {0}")]
    ApiError(String),

    #[error("Authentication error: {0}")]
    AuthenticationError(String),

    #[error("Network error: {0}")]
    NetworkError(#[from] reqwest::Error),

    #[error("JSON serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Missing session")]
    MissingSession,

    #[error("Invalid token: {0}")]
    InvalidToken(String),
}

/// ユーザー情報
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: String,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub app_metadata: serde_json::Value,
    pub user_metadata: serde_json::Value,
    pub created_at: String,
    pub updated_at: String,
}

/// セッション情報
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in: i64,
    pub token_type: String,
    pub user: User,
}

/// サインイン認証情報
#[derive(Debug, Serialize)]
pub struct SignInCredentials {
    pub email: String,
    pub password: String,
}

/// クライアントオプション
#[derive(Debug, Clone)]
pub struct AuthOptions {
    pub auto_refresh_token: bool,
    pub persist_session: bool,
    pub detect_session_in_url: bool,
}

impl Default for AuthOptions {
    fn default() -> Self {
        Self {
            auto_refresh_token: true,
            persist_session: true,
            detect_session_in_url: true,
        }
    }
}

/// OAuth プロバイダ
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum OAuthProvider {
    Google,
    Facebook,
    Twitter,
    Github,
    Apple,
    Discord,
    Gitlab,
    Bitbucket,
    Linkedin,
    Microsoft,
    Slack,
    Spotify,
}

impl OAuthProvider {
    fn display(&self) -> &'static str {
        match self {
            Self::Google => "google",
            Self::Facebook => "facebook",
            Self::Twitter => "twitter",
            Self::Github => "github",
            Self::Apple => "apple",
            Self::Discord => "discord",
            Self::Gitlab => "gitlab",
            Self::Bitbucket => "bitbucket",
            Self::Linkedin => "linkedin",
            Self::Microsoft => "microsoft",
            Self::Slack => "slack",
            Self::Spotify => "spotify",
        }
    }
}

/// OAuth サインイン設定
#[derive(Debug, Clone, Serialize, Default)]
pub struct OAuthSignInOptions {
    pub redirect_to: Option<String>,
    pub scopes: Option<String>,
    pub provider_scope: Option<String>,
    pub skip_browser_redirect: Option<bool>,
}

/// メール確認設定
#[derive(Debug, Clone, Serialize, Default)]
pub struct EmailConfirmOptions {
    pub redirect_to: Option<String>,
}

/// MFAファクターのタイプ
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MFAFactorType {
    Totp,
}

/// MFAファクターの状態
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MFAFactorStatus {
    Unverified,
    Verified,
}

/// MFAファクター情報
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MFAFactor {
    pub id: String,
    pub friendly_name: Option<String>,
    #[serde(rename = "factor_type")]
    pub factor_type: MFAFactorType,
    pub status: MFAFactorStatus,
    pub created_at: String,
    pub updated_at: String,
}

/// TOTP MFAチャレンジ
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MFAChallenge {
    pub id: String,
    #[serde(rename = "factor_id")]
    pub factor_id: String,
    pub created_at: String,
    pub expires_at: Option<String>,
}

/// MFAチャレンジ検証結果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MFAVerifyResponse {
    pub access_token: String,
    pub refresh_token: Option<String>,
    #[serde(rename = "type")]
    pub token_type: String,
    pub expires_in: i64,
}

/// TOTP設定情報
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TOTPSetupInfo {
    pub qr_code: String,
    pub secret: String,
    pub uri: String,
}

/// 電話番号認証のレスポンス
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhoneVerificationResponse {
    pub phone: String,
    pub verification_id: String,
    pub expires_at: String,
}

/// Auth クライアント
pub struct Auth {
    url: String,
    key: String,
    http_client: Client,
    options: AuthOptions,
    current_session: Arc<RwLock<Option<Session>>>,
    admin: Option<AdminAuth>,
}

/// Auth Admin クライアント - 管理者用API
pub struct AdminAuth {
    url: String,
    service_role_key: String,
    http_client: Client,
}

// AdminAuth実装
impl AdminAuth {
    /// 新しいAdminAuthクライアントを作成
    pub fn new(url: &str, service_role_key: &str, http_client: Client) -> Self {
        Self {
            url: url.to_string(),
            service_role_key: service_role_key.to_string(),
            http_client,
        }
    }

    /// Gets a user by their ID.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use supabase_rust_auth::{Auth, AdminAuth, AuthOptions, AuthError};
    /// # use reqwest::Client;
    /// #
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut auth = Auth::new("https://example.supabase.co/auth/v1", "anon-key", Client::new(), AuthOptions::default());
    ///
    /// // Initialize the admin client using a service role key
    /// let auth = auth.init_admin("your-service-role-key");
    ///
    /// if let Some(admin_auth) = auth.admin() {
    ///     let user = admin_auth.get_user_by_id("some-user-id").await?;
    ///     println!("User: {:?}", user);
    /// } else {
    ///     println!("Admin client not initialized");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_user_by_id(&self, user_id: &str) -> Result<User, AuthError> {
        let url = format!("{}/admin/users/{}", self.url, user_id);

        let response = self
            .http_client
            .get(&url)
            .header("apikey", &self.service_role_key)
            .header(
                "Authorization",
                format!("Bearer {}", &self.service_role_key),
            )
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(AuthError::ApiError(format!(
                "Failed to get user: {error_text}"
            )));
        }

        let user_data = response.json::<serde_json::Value>().await?;

        match serde_json::from_value::<User>(user_data) {
            Ok(user) => Ok(user),
            Err(err) => Err(AuthError::SerializationError(err)),
        }
    }

    /// Lists users with pagination.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use supabase_rust_auth::{Auth, AdminAuth, AuthOptions, AuthError};
    /// # use reqwest::Client;
    /// #
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut auth = Auth::new("https://example.supabase.co/auth/v1", "anon-key", Client::new(), AuthOptions::default());
    ///
    /// // Initialize the admin client using a service role key
    /// let auth = auth.init_admin("your-service-role-key");
    ///
    /// if let Some(admin_auth) = auth.admin() {
    ///     let users = admin_auth.list_users(Some(1), Some(100)).await?;
    ///     println!("Users: {:?}", users);
    /// } else {
    ///     println!("Admin client not initialized");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_users(
        &self,
        page: Option<u32>,
        per_page: Option<u32>,
    ) -> Result<Vec<User>, AuthError> {
        let page = page.unwrap_or(1);
        let per_page = per_page.unwrap_or(50);

        let url = format!(
            "{}/admin/users?page={}&per_page={}",
            self.url, page, per_page
        );

        let response = self
            .http_client
            .get(&url)
            .header("apikey", &self.service_role_key)
            .header(
                "Authorization",
                format!("Bearer {}", &self.service_role_key),
            )
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(AuthError::ApiError(format!(
                "Failed to list users: {error_text}"
            )));
        }

        let users_data = response.json::<serde_json::Value>().await?;

        match serde_json::from_value::<Vec<User>>(users_data) {
            Ok(users) => Ok(users),
            Err(err) => Err(AuthError::SerializationError(err)),
        }
    }

    /// 新しいユーザーを作成します
    ///
    /// # 引数
    ///
    /// * `email` - ユーザーのEメールアドレス
    /// * `password` - ユーザーのパスワード（オプション）
    /// * `user_metadata` - ユーザーのメタデータ（オプション）
    /// * `email_confirm` - メールアドレスを確認済みとしてマークするかどうか（オプション、デフォルトはfalse）
    ///
    /// # 例
    ///
    /// ```no_run
    /// # use supabase_rust_auth::{Auth, AdminAuth, AuthOptions, AuthError};
    /// # use reqwest::Client;
    /// #
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut auth = Auth::new("https://example.supabase.co/auth/v1", "anon-key", Client::new(), AuthOptions::default());
    ///
    /// // Initialize the admin client using a service role key
    /// let auth = auth.init_admin("your-service-role-key");
    ///
    /// if let Some(admin_auth) = auth.admin() {
    ///     let metadata = serde_json::json!({
    ///         "first_name": "John",
    ///         "last_name": "Doe"
    ///     });
    ///
    ///     let user = admin_auth.create_user(
    ///         "user@example.com",
    ///         Some("password123"),
    ///         Some(metadata),
    ///         Some(true)
    ///     ).await?;
    ///     println!("Created user: {:?}", user);
    /// } else {
    ///     println!("Admin client not initialized");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_user(
        &self,
        email: &str,
        password: Option<&str>,
        user_metadata: Option<serde_json::Value>,
        email_confirm: Option<bool>,
    ) -> Result<User, AuthError> {
        let url = format!("{}/admin/users", self.url);

        let mut payload = serde_json::json!({
            "email": email,
            "email_confirm": email_confirm.unwrap_or(false)
        });

        if let Some(pw) = password {
            payload["password"] = serde_json::Value::String(pw.to_string());
        }

        if let Some(metadata) = user_metadata {
            payload["user_metadata"] = metadata;
        }

        let response = self
            .http_client
            .post(&url)
            .header("apikey", &self.service_role_key)
            .header(
                "Authorization",
                format!("Bearer {}", &self.service_role_key),
            )
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(AuthError::ApiError(format!(
                "Failed to create user: {error_text}",
            )));
        }

        let user_data = response.json::<serde_json::Value>().await?;

        match serde_json::from_value::<User>(user_data) {
            Ok(user) => Ok(user),
            Err(err) => Err(AuthError::SerializationError(err)),
        }
    }

    /// ユーザーを削除します
    ///
    /// # 引数
    ///
    /// * `user_id` - 削除するユーザーのID
    ///
    /// # 例
    ///
    /// ```no_run
    /// # use supabase_rust_auth::{Auth, AdminAuth, AuthOptions, AuthError};
    /// # use reqwest::Client;
    /// #
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut auth = Auth::new("https://example.supabase.co/auth/v1", "anon-key", Client::new(), AuthOptions::default());
    ///
    /// // Initialize the admin client using a service role key
    /// let auth = auth.init_admin("your-service-role-key");
    ///
    /// if let Some(admin_auth) = auth.admin() {
    ///     admin_auth.delete_user("some-user-id").await?;
    ///     println!("User deleted");
    /// } else {
    ///     println!("Admin client not initialized");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_user(&self, user_id: &str) -> Result<(), AuthError> {
        let url = format!("{}/admin/users/{}", self.url, user_id);

        let response = self
            .http_client
            .delete(&url)
            .header("apikey", &self.service_role_key)
            .header(
                "Authorization",
                format!("Bearer {}", &self.service_role_key),
            )
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(AuthError::ApiError(format!(
                "Failed to delete user: {error_text}"
            )));
        }

        Ok(())
    }

    /// ユーザーの情報を更新します
    ///
    /// # 引数
    ///
    /// * `user_id` - 更新するユーザーのID
    /// * `attributes` - 更新するユーザー属性（email, password, user_metadata, email_confirm, phone_confirm など）
    ///
    /// # 例
    ///
    /// ```no_run
    /// # use supabase_rust_auth::{Auth, AdminAuth, AuthOptions, AuthError};
    /// # use reqwest::Client;
    /// #
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut auth = Auth::new("https://example.supabase.co/auth/v1", "anon-key", Client::new(), AuthOptions::default());
    ///
    /// // Initialize the admin client using a service role key
    /// let auth = auth.init_admin("your-service-role-key");
    ///
    /// if let Some(admin_auth) = auth.admin() {
    ///     let attributes = serde_json::json!({
    ///         "email": "newemail@example.com",
    ///         "user_metadata": {
    ///             "first_name": "Jane",
    ///             "last_name": "Smith"
    ///         },
    ///         "email_confirm": true
    ///     });
    ///
    ///     let user = admin_auth.update_user(
    ///         "some-user-id",
    ///         attributes
    ///     ).await?;
    ///     println!("Updated user: {:?}", user);
    /// } else {
    ///     println!("Admin client not initialized");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update_user(
        &self,
        user_id: &str,
        attributes: serde_json::Value,
    ) -> Result<User, AuthError> {
        let url = format!("{}/admin/users/{}", self.url, user_id);

        let response = self
            .http_client
            .put(&url)
            .header("apikey", &self.service_role_key)
            .header(
                "Authorization",
                format!("Bearer {}", &self.service_role_key),
            )
            .json(&attributes)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(AuthError::ApiError(format!(
                "Failed to update user: {error_text}"
            )));
        }

        let user_data = response.json::<serde_json::Value>().await?;

        match serde_json::from_value::<User>(user_data) {
            Ok(user) => Ok(user),
            Err(err) => Err(AuthError::SerializationError(err)),
        }
    }

    /// メール招待リンクを送信します
    ///
    /// # 引数
    ///
    /// * `email` - 招待するユーザーのEメールアドレス
    /// * `redirect_to` - 認証後のリダイレクト先URL（オプション）
    ///
    /// # 例
    ///
    /// ```no_run
    /// # use supabase_rust_auth::{Auth, AdminAuth, AuthOptions, AuthError};
    /// # use reqwest::Client;
    /// #
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut auth = Auth::new("https://example.supabase.co/auth/v1", "anon-key", Client::new(), AuthOptions::default());
    ///
    /// // Initialize the admin client using a service role key
    /// let auth = auth.init_admin("your-service-role-key");
    ///
    /// if let Some(admin_auth) = auth.admin() {
    ///     let user = admin_auth.invite_user_by_email(
    ///         "user@example.com",
    ///         Some("https://your-app.com/welcome")
    ///     ).await?;
    ///     println!("Invited user: {:?}", user);
    /// } else {
    ///     println!("Admin client not initialized");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn invite_user_by_email(
        &self,
        email: &str,
        redirect_to: Option<&str>,
    ) -> Result<User, AuthError> {
        let url = format!("{}/admin/users/invite", self.url);

        let mut payload = serde_json::json!({
            "email": email
        });

        if let Some(redirect) = redirect_to {
            payload["redirect_to"] = serde_json::Value::String(redirect.to_string());
        }

        let response = self
            .http_client
            .post(&url)
            .header("apikey", &self.service_role_key)
            .header(
                "Authorization",
                format!("Bearer {}", &self.service_role_key),
            )
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(AuthError::ApiError(format!(
                "Failed to invite user: {error_text}"
            )));
        }

        let user_data = response.json::<serde_json::Value>().await?;

        match serde_json::from_value::<User>(user_data) {
            Ok(user) => Ok(user),
            Err(err) => Err(AuthError::SerializationError(err)),
        }
    }

    /// ユーザーのMFAファクターを削除します
    ///
    /// # 引数
    ///
    /// * `user_id` - ユーザーのID
    /// * `factor_id` - 削除するMFAファクターのID
    ///
    /// # 例
    ///
    /// ```no_run
    /// # use supabase_rust_auth::{Auth, AdminAuth, AuthOptions, AuthError};
    /// # use reqwest::Client;
    /// #
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut auth = Auth::new("https://example.supabase.co/auth/v1", "anon-key", Client::new(), AuthOptions::default());
    ///
    /// // Initialize the admin client using a service role key
    /// let auth = auth.init_admin("your-service-role-key");
    ///
    /// if let Some(admin_auth) = auth.admin() {
    ///     admin_auth.delete_user_factor(
    ///         "some-user-id",
    ///         "factor-id"
    ///     ).await?;
    ///     println!("User factor deleted");
    /// } else {
    ///     println!("Admin client not initialized");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_user_factor(
        &self,
        user_id: &str,
        factor_id: &str,
    ) -> Result<(), AuthError> {
        let url = format!("{}/admin/users/{}/factors/{}", self.url, user_id, factor_id);

        let response = self
            .http_client
            .delete(&url)
            .header("apikey", &self.service_role_key)
            .header(
                "Authorization",
                format!("Bearer {}", &self.service_role_key),
            )
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(AuthError::ApiError(format!(
                "Failed to delete user factor: {error_text}"
            )));
        }

        Ok(())
    }

    /// メールリンクを生成します (マジックリンク, パスワードリセットなど)
    ///
    /// # 引数
    ///
    /// * `email` - ユーザーのEメールアドレス
    /// * `type` - リンクの種類 ("signup", "magiclink", "recovery", "invite")
    /// * `redirect_to` - 認証後のリダイレクト先URL（オプション）
    ///
    /// # 例
    ///
    /// ```no_run
    /// # use supabase_rust_auth::{Auth, AdminAuth, AuthOptions, AuthError};
    /// # use reqwest::Client;
    /// #
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut auth = Auth::new("https://example.supabase.co/auth/v1", "anon-key", Client::new(), AuthOptions::default());
    ///
    /// // Initialize the admin client using a service role key
    /// let auth = auth.init_admin("your-service-role-key");
    ///
    /// if let Some(admin_auth) = auth.admin() {
    ///     let link = admin_auth.generate_link(
    ///         "user@example.com",
    ///         "magiclink",
    ///         Some("https://your-app.com/welcome")
    ///     ).await?;
    ///     println!("Generated link: {}", link);
    /// } else {
    ///     println!("Admin client not initialized");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn generate_link(
        &self,
        email: &str,
        link_type: &str,
        redirect_to: Option<&str>,
    ) -> Result<String, AuthError> {
        let url = format!("{}/admin/users/generate_link", self.url);

        let mut payload = serde_json::json!({
            "email": email,
            "type": link_type
        });

        if let Some(redirect) = redirect_to {
            payload["redirect_to"] = serde_json::Value::String(redirect.to_string());
        }

        let response = self
            .http_client
            .post(&url)
            .header("apikey", &self.service_role_key)
            .header(
                "Authorization",
                format!("Bearer {}", &self.service_role_key),
            )
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(AuthError::ApiError(format!(
                "Failed to generate link: {error_text}"
            )));
        }

        let data = response.json::<serde_json::Value>().await?;

        match data.get("action_link") {
            Some(link) => match link.as_str() {
                Some(s) => Ok(s.to_string()),
                None => Err(AuthError::ApiError("Invalid link format".to_string())),
            },
            None => Err(AuthError::ApiError("No link returned".to_string())),
        }
    }
}

impl Auth {
    /// 新しい Auth クライアントを作成
    pub fn new(url: &str, key: &str, http_client: Client, options: AuthOptions) -> Self {
        Self {
            url: url.to_string(),
            key: key.to_string(),
            http_client: http_client.clone(),
            options,
            current_session: Arc::new(RwLock::new(None)),
            admin: None,
        }
    }

    /// 管理者用APIクライアントを初期化
    ///
    /// # 引数
    ///
    /// * `service_role_key` - サービスロールキー（重要: クライアント側では使用せず、サーバー側でのみ使用する）
    ///
    /// # 例
    ///
    /// ```no_run
    /// # use supabase_rust_auth::{Auth, AuthOptions};
    /// # use reqwest::Client;
    /// #
    /// # fn example() {
    /// # let auth = Auth::new("https://example.supabase.co/auth/v1", "anon-key", Client::new(), AuthOptions::default());
    /// # let mut auth = auth;
    /// let auth = auth.init_admin("your-service-role-key");
    /// # }
    /// ```
    pub fn init_admin(&mut self, service_role_key: &str) -> &Self {
        self.admin = Some(AdminAuth::new(
            &self.url,
            service_role_key,
            self.http_client.clone(),
        ));
        self
    }

    /// 管理者用APIクライアントを取得
    ///
    /// # 例
    ///
    /// ```no_run
    /// # use supabase_rust_auth::{Auth, AuthOptions};
    /// # use reqwest::Client;
    /// #
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut auth = Auth::new("https://example.supabase.co/auth/v1", "anon-key", Client::new(), AuthOptions::default());
    /// # let mut auth = auth.init_admin("your-service-role-key");
    /// if let Some(admin) = auth.admin() {
    ///     // Use admin API here
    ///     // let users = admin.list_users(None, None).await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn admin(&self) -> Option<&AdminAuth> {
        self.admin.as_ref()
    }

    /// ユーザー登録
    pub async fn sign_up(&self, email: &str, password: &str) -> Result<Session, AuthError> {
        let url = format!("{}/auth/v1/signup", self.url);

        let payload = serde_json::json!({
            "email": email,
            "password": password,
        });

        let response = self
            .http_client
            .post(&url)
            .header("apikey", &self.key)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(AuthError::ApiError(error_text));
        }

        let session: Session = response.json().await?;

        // セッションを保存
        if self.options.persist_session {
            let mut write_guard = self.current_session.write().unwrap();
            *write_guard = Some(session.clone());
        }

        Ok(session)
    }

    /// メール・パスワードでログイン
    pub async fn sign_in_with_password(
        &self,
        email: &str,
        password: &str,
    ) -> Result<Session, AuthError> {
        let url = format!("{}/auth/v1/token?grant_type=password", self.url);

        let payload = serde_json::json!({
            "email": email,
            "password": password,
        });

        let response = self
            .http_client
            .post(&url)
            .header("apikey", &self.key)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(AuthError::ApiError(error_text));
        }

        let session: Session = response.json().await?;

        // セッションを保存
        if self.options.persist_session {
            let mut write_guard = self.current_session.write().unwrap();
            *write_guard = Some(session.clone());
        }

        Ok(session)
    }

    /// 現在のセッションを取得
    pub fn get_session(&self) -> Option<Session> {
        let read_guard = self.current_session.read().unwrap();
        read_guard.clone()
    }

    /// 現在のユーザーを取得
    pub async fn get_user(&self) -> Result<User, AuthError> {
        let session = self.get_session().ok_or(AuthError::MissingSession)?;

        let url = format!("{}/auth/v1/user", self.url);

        let response = self
            .http_client
            .get(&url)
            .header("apikey", &self.key)
            .header("Authorization", format!("Bearer {}", session.access_token))
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(AuthError::ApiError(error_text));
        }

        let user: User = response.json().await?;

        Ok(user)
    }

    /// セッションをリフレッシュ
    pub async fn refresh_session(&self) -> Result<Session, AuthError> {
        let session = self.get_session().ok_or(AuthError::MissingSession)?;

        let url = format!("{}/auth/v1/token?grant_type=refresh_token", self.url);

        let payload = serde_json::json!({
            "refresh_token": session.refresh_token,
        });

        let response = self
            .http_client
            .post(&url)
            .header("apikey", &self.key)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(AuthError::ApiError(error_text));
        }

        let new_session: Session = response.json().await?;

        // セッションを更新
        if self.options.persist_session {
            let mut write_guard = self.current_session.write().unwrap();
            *write_guard = Some(new_session.clone());
        }

        Ok(new_session)
    }

    /// サインアウト
    pub async fn sign_out(&self) -> Result<(), AuthError> {
        let session = self.get_session().ok_or(AuthError::MissingSession)?;

        let url = format!("{}/auth/v1/logout", self.url);

        let response = self
            .http_client
            .post(&url)
            .header("apikey", &self.key)
            .header("Authorization", format!("Bearer {}", session.access_token))
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(AuthError::ApiError(error_text));
        }

        // セッションをクリア
        let mut write_guard = self.current_session.write().unwrap();
        *write_guard = None;

        Ok(())
    }

    /// パスワードリセットメールの送信
    pub async fn reset_password_for_email(&self, email: &str) -> Result<(), AuthError> {
        let url = format!("{}/auth/v1/recover", self.url);

        let payload = serde_json::json!({
            "email": email,
        });

        let response = self
            .http_client
            .post(&url)
            .header("apikey", &self.key)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(AuthError::ApiError(error_text));
        }

        Ok(())
    }

    /// OAuth プロバイダを通じたサインインのためのURL生成
    pub fn get_oauth_sign_in_url(
        &self,
        provider: OAuthProvider,
        options: Option<OAuthSignInOptions>,
    ) -> String {
        let provider_id = provider.display();
        let options = options.unwrap_or_default();

        let mut url = format!("{}/auth/v1/authorize?provider={}", self.url, provider_id);

        if let Some(redirect_to) = options.redirect_to {
            url.push_str(&format!(
                "&redirect_to={}",
                urlencoding::encode(&redirect_to)
            ));
        }

        if let Some(scopes) = options.scopes {
            url.push_str(&format!("&scopes={}", urlencoding::encode(&scopes)));
        }

        if let Some(provider_scope) = options.provider_scope {
            url.push_str(&format!(
                "&provider_scope={}",
                urlencoding::encode(&provider_scope)
            ));
        }

        url
    }

    /// OAuthで認証をリクエスト
    pub async fn sign_in_with_oauth(
        &self,
        provider: OAuthProvider,
        options: Option<OAuthSignInOptions>,
    ) -> Result<String, AuthError> {
        // OAuth認証URLを生成
        let url = self.get_oauth_sign_in_url(provider, options.clone());

        // 自動リダイレクトオプション
        let skip_browser_redirect = options
            .and_then(|opt| opt.skip_browser_redirect)
            .unwrap_or(false);

        if skip_browser_redirect {
            return Ok(url);
        }

        // 通常はクライアント側でURLにリダイレクトする必要があるため、
        // ここではURLを返します。Rustの場合、環境によって適切なブラウザ起動方法が異なります。
        Ok(url)
    }

    /// OAuthコールバックからのコードを処理してセッション取得
    pub async fn exchange_code_for_session(&self, code: &str) -> Result<Session, AuthError> {
        let url = format!("{}/auth/v1/token?grant_type=authorization_code", self.url);

        let payload = serde_json::json!({
            "code": code,
        });

        let response = self
            .http_client
            .post(&url)
            .header("apikey", &self.key)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(AuthError::ApiError(error_text));
        }

        let session: Session = response.json().await?;

        // セッションを保存
        if self.options.persist_session {
            let mut write_guard = self.current_session.write().unwrap();
            *write_guard = Some(session.clone());
        }

        Ok(session)
    }

    /// MFAで保護されたサインイン - 最初のステップ（パスワードでの認証）
    ///
    /// このメソッドは通常のサインインプロセスと同様ですが、ユーザーが
    /// MFAを有効化している場合は、次のステップで検証が必要なチャレンジを返します。
    pub async fn sign_in_with_password_mfa(
        &self,
        email: &str,
        password: &str,
    ) -> Result<Result<Session, MFAChallenge>, AuthError> {
        let url = format!("{}/auth/v1/token?grant_type=password", self.url);

        let payload = serde_json::json!({
            "email": email,
            "password": password,
        });

        let response = self
            .http_client
            .post(&url)
            .header("apikey", &self.key)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        // サインイン結果をパース
        let status = response.status();
        let body = response.text().await?;

        if status.is_success() {
            // 通常のサインイン成功（MFAが必要ない）
            let session: Session = serde_json::from_str(&body)?;

            // セッションを保存
            if self.options.persist_session {
                let mut write_guard = self.current_session.write().unwrap();
                *write_guard = Some(session.clone());
            }

            Ok(Ok(session))
        } else if status.as_u16() == 401 {
            // MFA認証が必要かチェック
            if let Ok(challenge) = serde_json::from_str::<MFAChallenge>(&body) {
                // MFAチャレンジ
                Ok(Err(challenge))
            } else {
                // 通常の認証エラー
                Err(AuthError::ApiError(body))
            }
        } else {
            // その他のエラー
            Err(AuthError::ApiError(body))
        }
    }

    /// MFAチャレンジの検証 - 第二ステップ（コードによる検証）
    pub async fn verify_mfa_challenge(
        &self,
        challenge_id: &str,
        code: &str,
    ) -> Result<Session, AuthError> {
        let url = format!("{}/auth/v1/mfa/verify", self.url);

        let payload = serde_json::json!({
            "challenge_id": challenge_id,
            "code": code,
        });

        let response = self
            .http_client
            .post(&url)
            .header("apikey", &self.key)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(AuthError::ApiError(error_text));
        }

        let verify_response: MFAVerifyResponse = response.json().await?;

        // セッションオブジェクトに変換
        let user = self
            .get_user_by_token(&verify_response.access_token)
            .await?;

        let session = Session {
            access_token: verify_response.access_token,
            refresh_token: verify_response.refresh_token.unwrap_or_default(),
            expires_in: verify_response.expires_in,
            token_type: verify_response.token_type,
            user,
        };

        // セッションを保存
        if self.options.persist_session {
            let mut write_guard = self.current_session.write().unwrap();
            *write_guard = Some(session.clone());
        }

        Ok(session)
    }

    /// MFAファクターを登録する
    pub async fn enroll_totp(&self) -> Result<TOTPSetupInfo, AuthError> {
        let session = self.get_session().ok_or(AuthError::MissingSession)?;

        let url = format!("{}/auth/v1/mfa/totp", self.url);

        let response = self
            .http_client
            .post(&url)
            .header("apikey", &self.key)
            .header("Authorization", format!("Bearer {}", session.access_token))
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(AuthError::ApiError(error_text));
        }

        let setup_info: TOTPSetupInfo = response.json().await?;

        Ok(setup_info)
    }

    /// TOTP MFAファクターを検証して有効化
    pub async fn verify_totp(&self, factor_id: &str, code: &str) -> Result<MFAFactor, AuthError> {
        let session = self.get_session().ok_or(AuthError::MissingSession)?;

        let url = format!("{}/auth/v1/mfa/totp/verify", self.url);

        let payload = serde_json::json!({
            "factor_id": factor_id,
            "code": code,
        });

        let response = self
            .http_client
            .post(&url)
            .header("apikey", &self.key)
            .header("Authorization", format!("Bearer {}", session.access_token))
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(AuthError::ApiError(error_text));
        }

        let factor: MFAFactor = response.json().await?;

        Ok(factor)
    }

    /// ユーザーの登録済みMFAファクター一覧を取得
    pub async fn list_factors(&self) -> Result<Vec<MFAFactor>, AuthError> {
        let session = self.get_session().ok_or(AuthError::MissingSession)?;

        let url = format!("{}/auth/v1/mfa/factors", self.url);

        let response = self
            .http_client
            .get(&url)
            .header("apikey", &self.key)
            .header("Authorization", format!("Bearer {}", session.access_token))
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(AuthError::ApiError(error_text));
        }

        let factors: Vec<MFAFactor> = response.json().await?;

        Ok(factors)
    }

    /// MFAファクターを無効化（削除）
    pub async fn unenroll_factor(&self, factor_id: &str) -> Result<(), AuthError> {
        let session = self.get_session().ok_or(AuthError::MissingSession)?;

        let url = format!("{}/auth/v1/mfa/factors/{}", self.url, factor_id);

        let response = self
            .http_client
            .delete(&url)
            .header("apikey", &self.key)
            .header("Authorization", format!("Bearer {}", session.access_token))
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(AuthError::ApiError(error_text));
        }

        Ok(())
    }

    /// トークンを使ってユーザー情報を取得（内部メソッド）
    async fn get_user_by_token(&self, token: &str) -> Result<User, AuthError> {
        let url = format!("{}/auth/v1/user", self.url);

        let response = self
            .http_client
            .get(&url)
            .header("apikey", &self.key)
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(AuthError::ApiError(error_text));
        }

        let user: User = response.json().await?;

        Ok(user)
    }

    /// 匿名認証でサインイン
    pub async fn sign_in_anonymously(&self) -> Result<Session, AuthError> {
        let endpoint = format!("{}/auth/v1/signup", self.url);

        let response = self
            .http_client
            .post(&endpoint)
            .header("apikey", &self.key)
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({
                "data": {}
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let error_msg = response.text().await?;
            return Err(AuthError::ApiError(error_msg));
        }

        let session: Session = response.json().await?;

        // セッションを保存
        if self.options.persist_session {
            let mut writable_session = self.current_session.write().unwrap();
            *writable_session = Some(session.clone());
        }

        Ok(session)
    }

    /// メール確認のリクエストを送信する
    ///
    /// # Arguments
    ///
    /// * `email` - 確認メールを送信するメールアドレス
    /// * `options` - オプション設定（リダイレクトURLなど）
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use supabase_rust_auth::{Auth, AuthOptions, EmailConfirmOptions};
    /// # use reqwest::Client;
    /// #
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let auth = Auth::new("https://example.supabase.co/auth/v1", "anon-key", Client::new(), AuthOptions::default());
    /// let options = EmailConfirmOptions {
    ///     redirect_to: Some("https://example.com/confirm-success".to_string()),
    /// };
    ///
    /// auth.send_confirm_email_request("user@example.com", Some(options)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_confirm_email_request(
        &self,
        email: &str,
        options: Option<EmailConfirmOptions>,
    ) -> Result<(), AuthError> {
        let endpoint = format!("{}/auth/v1/signup", self.url);

        let mut payload = serde_json::json!({
            "email": email,
            "data": {}
        });

        if let Some(opts) = options {
            if let Some(redirect_to) = opts.redirect_to {
                payload["options"] = serde_json::json!({
                    "redirect_to": redirect_to
                });
            }
        }

        let response = self
            .http_client
            .post(&endpoint)
            .header("apikey", &self.key)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_msg = response.text().await?;
            return Err(AuthError::ApiError(error_msg));
        }

        Ok(())
    }

    /// メール確認トークンを検証する
    ///
    /// # Arguments
    ///
    /// * `token` - メール確認用のトークン（確認リンクから取得）
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use supabase_rust_auth::{Auth, AuthOptions};
    /// # use reqwest::Client;
    /// #
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let auth = Auth::new("https://example.supabase.co/auth/v1", "anon-key", Client::new(), AuthOptions::default());
    /// let session = auth.verify_email("confirmation-token-from-email").await?;
    /// println!("Email verified for user: {:?}", session.user.email);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn verify_email(&self, token: &str) -> Result<Session, AuthError> {
        let endpoint = format!("{}/auth/v1/verify", self.url);

        let response = self
            .http_client
            .post(&endpoint)
            .header("apikey", &self.key)
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({
                "type": "signup",
                "token": token
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let error_msg = response.text().await?;
            return Err(AuthError::ApiError(error_msg));
        }

        let session: Session = response.json().await?;

        // セッションを保存
        if self.options.persist_session {
            let mut writable_session = self.current_session.write().unwrap();
            *writable_session = Some(session.clone());
        }

        Ok(session)
    }

    /// パスワードリセット後にリセットトークンを検証する
    ///
    /// # Arguments
    ///
    /// * `token` - パスワードリセット用のトークン（リセットリンクから取得）
    /// * `new_password` - 新しいパスワード
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use supabase_rust_auth::{Auth, AuthOptions};
    /// # use reqwest::Client;
    /// #
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let auth = Auth::new("https://example.supabase.co/auth/v1", "anon-key", Client::new(), AuthOptions::default());
    /// let session = auth.verify_password_reset("reset-token-from-email", "new-secure-password").await?;
    /// println!("Password reset for user: {:?}", session.user.email);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn verify_password_reset(
        &self,
        token: &str,
        new_password: &str,
    ) -> Result<Session, AuthError> {
        let endpoint = format!("{}/auth/v1/verify", self.url);

        let response = self
            .http_client
            .post(&endpoint)
            .header("apikey", &self.key)
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({
                "type": "recovery",
                "token": token,
                "password": new_password
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let error_msg = response.text().await?;
            return Err(AuthError::ApiError(error_msg));
        }

        let session: Session = response.json().await?;

        // セッションを保存
        if self.options.persist_session {
            let mut writable_session = self.current_session.write().unwrap();
            *writable_session = Some(session.clone());
        }

        Ok(session)
    }

    pub async fn send_verification_code(
        &self,
        phone: &str,
    ) -> Result<PhoneVerificationResponse, AuthError> {
        let url = format!("{}/auth/v1/otp", self.url);

        let payload = serde_json::json!({
            "phone": phone,
            "channel": "sms"
        });

        let response = self
            .http_client
            .post(&url)
            .header("apikey", &self.key)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(AuthError::ApiError(error_text));
        }

        let verification: PhoneVerificationResponse = response.json().await?;
        Ok(verification)
    }

    /// 電話番号と検証コードでサインイン
    pub async fn verify_phone_code(
        &self,
        phone: &str,
        verification_id: &str,
        code: &str,
    ) -> Result<Session, AuthError> {
        let url = format!("{}/auth/v1/verify", self.url);

        let payload = serde_json::json!({
            "phone": phone,
            "verification_id": verification_id,
            "code": code,
            "type": "sms"
        });

        let response = self
            .http_client
            .post(&url)
            .header("apikey", &self.key)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(AuthError::ApiError(error_text));
        }

        let session: Session = response.json().await?;

        // セッションを保存
        if self.options.persist_session {
            let mut write_guard = self.current_session.write().unwrap();
            *write_guard = Some(session.clone());
        }

        Ok(session)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};
    // http::Responseを明示的にインポート

    #[test]
    fn test_sign_up() {
        tokio_test::block_on(async {
            let mock_server = MockServer::start().await;

            let response_body = serde_json::json!({
                "access_token": "test_access_token",
                "refresh_token": "test_refresh_token",
                "expires_in": 3600,
                "token_type": "bearer",
                "user": {
                    "id": "test_user_id",
                    "email": "test@example.com",
                    "phone": null,
                    "app_metadata": {},
                    "user_metadata": {},
                    "created_at": "2021-01-01T00:00:00Z",
                    "updated_at": "2021-01-01T00:00:00Z"
                }
            });

            Mock::given(method("POST"))
                .and(path("/auth/v1/signup"))
                .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
                .mount(&mock_server)
                .await;

            let http_client = Client::new();
            let auth = Auth::new(
                &mock_server.uri(),
                "test_key",
                http_client,
                AuthOptions::default(),
            );

            let result = auth.sign_up("test@example.com", "password123").await;

            assert!(result.is_ok());
            let session = result.unwrap();
            assert_eq!(session.access_token, "test_access_token");
            assert_eq!(session.user.email, Some("test@example.com".to_string()));
        });
    }

    #[test]
    fn test_oauth_sign_in_url() {
        tokio_test::block_on(async {
            let client = Client::new();
            let auth = Auth::new(
                "https://example.supabase.co",
                "test-key",
                client,
                AuthOptions::default(),
            );

            let url = auth.get_oauth_sign_in_url(super::OAuthProvider::Google, None);
            assert!(url.contains("provider=google"));

            let options = super::OAuthSignInOptions {
                redirect_to: Some("https://example.com/callback".to_string()),
                scopes: Some("email profile".to_string()),
                ..Default::default()
            };

            let url_with_options =
                auth.get_oauth_sign_in_url(super::OAuthProvider::Github, Some(options));
            assert!(url_with_options.contains("provider=github"));
            assert!(url_with_options.contains("redirect_to="));
            assert!(url_with_options.contains("scopes="));
        });
    }
}
