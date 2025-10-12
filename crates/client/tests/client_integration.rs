// crates/supabase_client/tests/client_integration.rs

// Import the crate itself
use supabase_rust_client::client::SupabaseClientWrapper;
use supabase_rust_client::client::SupabaseConfig;
use supabase_rust_client::error::SupabaseError;
use supabase_rust_client::models::{AuthCredentials, Item};

// Import dev dependencies for mocking, etc.
use chrono::Utc;
use dotenv::dotenv;
// To create mock JSON bodies
use serde_json::json;
use std::env;
use uuid::Uuid;
use wiremock::{
    matchers::{header, method, path_regex}, // Use path_regex
    Mock,
    MockServer,
    ResponseTemplate,
};

// Import Auth client directly for more controlled testing
use reqwest::Client as ReqwestClient;
use supabase_rust_auth::{Auth, AuthError, AuthOptions, Session as AuthSession, User as AuthUser};

// Helper function
async fn setup_mock_config(mock_server: &MockServer) -> SupabaseConfig {
    dotenv().ok();
    let mut url_string = mock_server.uri(); // Get base URI as string
                                            // Ensure no trailing slash for base URL passed to config
    if url_string.ends_with('/') {
        url_string.pop();
    }
    let anon_key = env::var("SUPABASE_ANON_KEY").unwrap_or_else(|_| "mock_anon_key".to_string());
    // Create config using the cleaned URL string
    SupabaseConfig::new(&url_string, anon_key).unwrap()
}

#[tokio::test]
async fn test_authenticate_success() {
    let mock_server = MockServer::start().await;
    // Use the raw address string
    let base_url = format!("http://{}", mock_server.address());
    let anon_key = "mock_anon_key";

    // Create Auth client directly
    let http_client = ReqwestClient::new();
    let auth_client = Auth::new(&base_url, anon_key, http_client, AuthOptions::default());

    let credentials = AuthCredentials {
        email: "test@example.com".to_string(),
        password: "password".to_string(),
    };

    let mock_user_id_str = Uuid::new_v4().to_string();
    let mock_access_token = "mock_access_token_auth_success";
    let mock_created_at = Utc::now().to_rfc3339();
    let mock_updated_at = Utc::now().to_rfc3339();

    let mock_session_response = json!({
        "access_token": mock_access_token,
        "refresh_token": "mock_refresh_token",
        "expires_in": 3600,
        "token_type": "bearer",
        "user": {
            "id": mock_user_id_str,
            "email": Some(credentials.email.clone()),
            "phone": None::<String>,
            "app_metadata": {},
            "user_metadata": {},
            "created_at": mock_created_at,
            "updated_at": mock_updated_at,
        }
    });

    Mock::given(method("POST"))
        .and(path_regex(r"^/auth/v1/token(?:\?.*)?$"))
        .and(header("apikey", anon_key))
        .and(header("Content-Type", "application/json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&mock_session_response))
        .expect(1)
        .mount(&mock_server)
        .await;

    // Call authenticate directly
    let auth_result = auth_client
        .sign_in_with_password(&credentials.email, &credentials.password)
        .await;

    // Assert success and check session data
    assert!(auth_result.is_ok());
    let session = auth_result.unwrap();
    assert_eq!(session.access_token, mock_access_token);
    assert_eq!(session.user.id, mock_user_id_str);
    assert_eq!(session.user.email, Some(credentials.email));
    assert_eq!(session.user.created_at, mock_created_at);
}

#[tokio::test]
async fn test_authenticate_failure() {
    let mock_server = MockServer::start().await;
    // Use the raw address string for the Auth client
    let base_url = format!("http://{}", mock_server.address());
    let anon_key = "mock_anon_key";

    // Create Auth client directly for test
    let http_client = ReqwestClient::new();
    let auth_client = Auth::new(
        &base_url, // Use the raw base URL string
        anon_key,
        http_client,
        AuthOptions::default(),
    );

    let credentials = AuthCredentials {
        email: "wrong@example.com".to_string(),
        password: "wrong".to_string(),
    };

    let mock_error_response = json!({
        "error": "invalid_grant",
        "error_description": "Invalid email or password"
    });

    Mock::given(method("POST"))
        .and(path_regex(r"^/auth/v1/token(?:\?.*)?$"))
        .and(header("apikey", anon_key))
        .and(header("Content-Type", "application/json"))
        .respond_with(ResponseTemplate::new(401).set_body_json(&mock_error_response))
        .expect(1)
        .mount(&mock_server) // Mount on the server instance
        .await;

    // Call authenticate directly on the test auth_client
    let auth_result = auth_client
        .sign_in_with_password(&credentials.email, &credentials.password)
        .await;

    // Assert that the specific error occurred
    assert!(auth_result.is_err());
    match auth_result.err().unwrap() {
        AuthError::ApiError(_) => { /* Expected */ }
        e => panic!("Expected ApiError, got {:?}", e),
    }
}

#[tokio::test]
async fn test_fetch_items_authenticated() {
    let mock_server = MockServer::start().await;
    let config = setup_mock_config(&mock_server).await;
    let client = SupabaseClientWrapper::new(config.clone()).unwrap();
    let mock_access_token = "mock_access_token_fetch";
    let mock_user_id = Uuid::new_v4(); // Use Uuid for consistency in test logic

    // Simulate authentication by creating a mock session matching auth v0.2.0 structs
    let mock_session = AuthSession {
        access_token: mock_access_token.to_string(),
        refresh_token: "mock_refresh_token".to_string(),
        expires_in: 3600,
        token_type: "bearer".to_string(),
        user: AuthUser {
            // Use the actual User struct from supabase_rust_auth
            id: mock_user_id.to_string(), // ID is string
            email: Some("test@example.com".to_string()),
            phone: None,
            created_at: Utc::now().to_rfc3339(), // created_at is string
            updated_at: Utc::now().to_rfc3339(), // updated_at is string
            app_metadata: json!({}),             // Use json! macro for Value
            user_metadata: json!({ "test_field": "test_value" }),
        },
    };

    // Use the test helper method to set the session
    client.set_session_for_test(Some(mock_session)).await;

    let mock_item_id = Uuid::new_v4();
    let mock_items = vec![Item {
        id: mock_item_id,
        user_id: mock_user_id,
        name: "Mock Item 1".to_string(),
        description: Some("Description for mock item".to_string()),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }];
    let auth_header_value = format!("Bearer {}", mock_access_token);

    Mock::given(method("GET"))
        .and(wiremock::matchers::path_regex(r"/rest/v1/items(?:\?.*)?"))
        .and(header("Authorization", auth_header_value.as_str()))
        .and(header("apikey", config.anon_key.as_str()))
        .respond_with(ResponseTemplate::new(200).set_body_json(&mock_items))
        .expect(1)
        .mount(&mock_server)
        .await;

    let fetch_result = client.fetch_items().await;

    assert!(
        fetch_result.is_ok(),
        "Fetch failed: {:?}",
        fetch_result.err()
    );
    let items = fetch_result.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].id, mock_item_id);
    assert_eq!(items[0].user_id, mock_user_id);
}

#[tokio::test]
async fn test_fetch_items_unauthenticated() {
    let mock_server = MockServer::start().await;
    let config = setup_mock_config(&mock_server).await;
    let client = SupabaseClientWrapper::new(config).unwrap();

    // Ensure session is None (default state)
    client.set_session_for_test(None).await;

    // Mock is not strictly needed as get_auth_token should fail first,
    // but we can keep it for defence.
    Mock::given(method("GET"))
        .and(wiremock::matchers::path_regex(r"/rest/v1/items(?:\?.*)?"))
        .and(header("apikey", client.anon_key()))
        .respond_with(
            ResponseTemplate::new(401).set_body_json(json!({ "message": "Unauthorized" })),
        )
        .expect(0) // Expect 0 calls matching this (client should error before request)
        .mount(&mock_server)
        .await;

    let fetch_result = client.fetch_items().await;
    assert!(fetch_result.is_err());
    match fetch_result.err().unwrap() {
        SupabaseError::Auth(AuthError::ApiError(msg)) => {
            assert!(msg.contains("Missing session token"))
        }
        e => panic!(
            "Expected AuthError::ApiError(Missing session token), got {:?}",
            e
        ),
    }
}

#[tokio::test]
async fn test_integration_crud() {
    let mock_server = MockServer::start().await;
    let config = setup_mock_config(&mock_server).await;
    let client = SupabaseClientWrapper::new(config.clone()).unwrap();
    let mock_access_token = "mock_access_token_crud";
    let mock_user_id = Uuid::new_v4();

    // Simulate authentication
    let mock_session = AuthSession {
        access_token: mock_access_token.to_string(),
        refresh_token: "mock_refresh_token_crud".to_string(),
        expires_in: 3600,
        token_type: "bearer".to_string(),
        user: AuthUser {
            // Use actual User struct
            id: mock_user_id.to_string(),
            email: Some("crud@example.com".to_string()),
            phone: None,
            created_at: Utc::now().to_rfc3339(),
            updated_at: Utc::now().to_rfc3339(),
            app_metadata: json!({}),
            user_metadata: json!({ "crud_test": true }),
        },
    };
    client.set_session_for_test(Some(mock_session)).await;

    let new_item_data = Item {
        id: Uuid::nil(),
        user_id: mock_user_id,
        name: "CRUD Item".to_string(),
        description: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    let expected_created_item = Item {
        id: Uuid::new_v4(),
        user_id: new_item_data.user_id,
        name: new_item_data.name.clone(),
        description: new_item_data.description.clone(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    let auth_header_value = format!("Bearer {}", mock_access_token);

    Mock::given(method("POST"))
        .and(wiremock::matchers::path_regex(r"/rest/v1/items(?:\?.*)?"))
        .and(header("Authorization", auth_header_value.as_str()))
        .and(header("apikey", config.anon_key.as_str()))
        .and(header("Prefer", "return=representation"))
        .respond_with(ResponseTemplate::new(201).set_body_json(vec![expected_created_item.clone()]))
        .expect(1)
        .mount(&mock_server)
        .await;

    let create_result = client.create_item(new_item_data).await;
    assert!(
        create_result.is_ok(),
        "Create failed: {:?}",
        create_result.err()
    );
    let created_item = create_result.unwrap();
    assert_ne!(created_item.id, Uuid::nil());
    assert_eq!(created_item.user_id, mock_user_id);
    assert_eq!(created_item.name, "CRUD Item");

    // TODO: Add mocks and calls for fetch_item_by_id, update_item, delete_item
}
