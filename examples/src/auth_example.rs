use dotenv::dotenv;
use std::env;
use supabase_rust_client::client::SupabaseConfig;
use supabase_rust_client::SupabaseClientWrapper;
use uuid::Uuid;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // Load environment variables from .env file
    dotenv().ok();

    // Get Supabase URL and key from environment variables
    let supabase_url = env::var("SUPABASE_URL").expect("SUPABASE_URL must be set");
    let supabase_key = env::var("SUPABASE_KEY").expect("SUPABASE_KEY must be set");

    // Initialize the Supabase client
    let config = SupabaseConfig::new(supabase_url.as_str(), supabase_key)?;
    let supabase = SupabaseClientWrapper::new(config)?;

    println!("Starting Auth example");

    // Generate a unique email for testing
    let unique_id = Uuid::new_v4().to_string();
    let test_email = format!("test-user-{unique_id}@example.com");
    let test_password = "securePassword123!";

    // Create a new user
    let auth = supabase.auth;

    // Sign up a new user
    println!("Signing up a new user with email: {test_email}");

    let user_response = auth.sign_up(&test_email, test_password).await?;

    println!("User sign up response: {user_response:?}");

    // サインイン機能をテスト
    println!("\nTesting sign in with the new user");

    let sign_in_response = auth
        .sign_in_with_password(&test_email, test_password)
        .await?;

    println!("User sign in response: {sign_in_response:?}");

    // アクセストークンを取得
    let access_token = sign_in_response.access_token;
    println!("\nAccess token: {access_token}");

    // ユーザー情報を取得 - APIの変更に対応
    println!("\nGetting user information");

    let user_info = auth.get_user().await?;

    println!("User info: {user_info:?}");

    // メタデータ更新機能は現在のAPIでは使用できないためコメントアウト
    /*
    // メタデータを更新
    println!("\nUpdating user metadata");

    let metadata = json!({
        "preferred_language": "ja",
        "last_login_device": "rust-client"
    });

    let update_result = auth.update_user_metadata(&access_token, metadata).await?;

    println!("Update result: {:?}", update_result);
    */

    // サインアウト - APIの変更に対応
    println!("\nSigning out");

    auth.sign_out().await?;

    println!("User signed out successfully");

    println!("Auth example completed");

    Ok(())
}
