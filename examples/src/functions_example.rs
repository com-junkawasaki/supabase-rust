use dotenv::dotenv;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::env;
use supabase_rust_client::client::SupabaseConfig;
use supabase_rust_client::SupabaseClientWrapper;
use supabase_rust_functions::FunctionOptions;

#[derive(Debug, Serialize)]
struct HelloRequest {
    name: String,
}

#[derive(Debug, Deserialize)]
struct HelloResponse {
    // #[allow(dead_code)] // Add allow attribute for unused field
    // message: String, // Field is unused, comment out or allow dead_code
}

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

    println!("Starting Edge Functions example");

    // Access the Functions client
    let functions = supabase.functions;

    // This example assumes you have created an Edge Function named "hello-world"
    // that accepts a JSON payload with a "name" field and returns a JSON response
    // with a "message" field.

    // Example Edge Function in JavaScript might look like:
    //
    // Deno.serve(async (req) => {
    //   try {
    //     const { name } = await req.json();
    //     return new Response(
    //       JSON.stringify({
    //         message: `Hello, ${name || 'World'}!`,
    //       }),
    //       { headers: { 'Content-Type': 'application/json' } }
    //     );
    //   } catch (error) {
    //     return new Response(
    //       JSON.stringify({ error: error.message }),
    //       { status: 400, headers: { 'Content-Type': 'application/json' } }
    //     );
    //   }
    // });

    // Invoke the function without authentication
    println!("Invoking hello-world function without authentication");

    let request_data = HelloRequest {
        name: "Rust".to_string(),
    };

    let function_request = functions.create_request::<HelloResponse>("hello-world");

    match function_request
        .execute(Some(json!(request_data)), None)
        .await
    {
        Ok(response) => {
            println!("Function response: {response:?}");
        }
        Err(e) => {
            println!("Error invoking function: {e:?}");
            println!(
                "Note: This example assumes you have created an Edge Function named 'hello-world'"
            );
            println!("If the function doesn't exist, you'll need to create it in your Supabase dashboard.");
        }
    }

    // Now let's try with authentication
    // First, sign up a test user
    let test_email = format!("test-functions-{}@example.com", uuid::Uuid::new_v4());
    let test_password = "password123";

    let sign_up_result = supabase.auth.sign_up(&test_email, test_password).await?;

    let user_id = sign_up_result.user.id;
    println!("Created test user with ID: {user_id}");

    // Sign in to get a session
    let sign_in_result = supabase
        .auth
        .sign_in_with_password(&test_email, test_password)
        .await?;

    let access_token = sign_in_result.access_token;
    println!("Got access token for authenticated requests");

    // Invoke the function with authentication
    println!("Invoking hello-world function with authentication");

    let request_data = HelloRequest {
        name: "Authenticated Rust".to_string(),
    };

    let function_request = functions.create_request::<HelloResponse>("hello-world");

    // Create a HashMap for headers
    let mut headers = HashMap::new();
    headers.insert(
        "Authorization".to_string(),
        format!("Bearer {access_token}"),
    );

    let options = FunctionOptions {
        headers: Some(headers),
        ..Default::default()
    };

    match function_request
        .execute(Some(json!(request_data)), Some(options))
        .await
    {
        Ok(response) => {
            println!("Authenticated function response: {response:?}");
        }
        Err(e) => {
            println!("Error invoking authenticated function: {e:?}");
        }
    }

    // Example of invoking a function with custom headers
    println!("Invoking function with custom headers");

    let function_request = functions.create_request::<HelloResponse>("hello-world");

    // Create a HashMap for custom headers
    let mut headers = HashMap::new();
    headers.insert("x-custom-header".to_string(), "custom-value".to_string());
    headers.insert(
        "x-client-info".to_string(),
        "supabase-rust-client".to_string(),
    );

    let options = FunctionOptions {
        headers: Some(headers),
        ..Default::default()
    };

    match function_request
        .execute(Some(json!(request_data)), Some(options))
        .await
    {
        Ok(response) => {
            println!("Function response with custom headers: {response:?}");
        }
        Err(e) => {
            println!("Error invoking function with custom headers: {e:?}");
        }
    }

    println!("Edge Functions example completed");

    Ok(())
}
