use dotenv::dotenv;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::env;
use supabase_rust_client::client::SupabaseConfig;
use supabase_rust_client::SupabaseClientWrapper;
use supabase_rust_postgrest::{IsolationLevel, PostgrestError, SortOrder, TransactionMode};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Task {
    id: Option<i32>,
    title: String,
    description: Option<String>,
    is_complete: bool,
    created_at: Option<String>,
    user_id: String,
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // Load environment variables from .env file
    dotenv().ok();

    // Get Supabase URL and key from environment variables
    let supabase_url = env::var("SUPABASE_URL").expect("SUPABASE_URL must be set");
    let supabase_key = env::var("SUPABASE_KEY").expect("SUPABASE_KEY must be set");

    println!("Using Supabase URL: {supabase_url}");

    // Initialize the Supabase client
    let config = SupabaseConfig::new(supabase_url.as_str(), supabase_key)?;
    let supabase_client = SupabaseClientWrapper::new(config)?;

    println!("Starting PostgREST advanced example");

    // First, sign up a test user for our examples
    let test_email = format!("test-postgrest-{}@example.com", uuid::Uuid::new_v4());
    let test_password = "password123";

    let sign_up_result = supabase_client
        .auth
        .sign_up(&test_email, test_password)
        .await?;

    let user_id = sign_up_result.user.id.clone();
    let access_token = sign_up_result.access_token.clone();
    println!("Created test user with ID: {user_id}");

    // Example 1: Basic operations
    println!("Example 1: Basic operations");
    println!("Using user_id: {user_id}");

    // Example 2: INSERT and SELECT
    println!("\nExample 2: INSERT and SELECT");

    // Create tasks using basic insert with json!
    for i in 1..6 {
        // Create JSON directly, omitting id
        let task_json = json!({
            "title": format!("Task {}", i),
            "description": Some(format!("Description for task {i}")),
            "is_complete": i % 2 == 0,
            "user_id": user_id, // Use the correct user_id
        });
        // Use basic insert
        // We still need to figure out how to handle the expected response or add Prefer header
        let _insert_result = supabase_client
            .from("tasks")
            .with_auth(&access_token)?
            .with_header("Prefer", "return=representation")?
            .insert(task_json) // Use basic insert
            .await?;
    }
    println!("Created 5 tasks");

    // 未完了のタスクを取得
    let incomplete_tasks_json = supabase_client
        .from("tasks")
        .with_auth(&access_token)?
        .select("*")
        .eq("user_id", &user_id)
        .eq("is_complete", "false")
        .execute::<serde_json::Value>()
        .await?;

    let incomplete_tasks: Vec<Task> = incomplete_tasks_json
        .iter()
        .map(|task_json| serde_json::from_value(task_json.clone()).unwrap())
        .collect();

    println!("Found {} incomplete tasks:", incomplete_tasks.len());
    for task in &incomplete_tasks {
        println!("  - {}", task.title);
    }

    // Example 3: Complex filters and order
    println!("\nExample 3: Complex filters and order");

    let filtered_tasks_json = supabase_client
        .from("tasks")
        .with_auth(&access_token)?
        .select("*")
        .eq("user_id", &user_id)
        .in_list("title", &["Task 1", "Task 2"])
        .order("created_at", SortOrder::Ascending)
        .limit(10)
        .execute::<serde_json::Value>()
        .await?;

    let filtered_tasks: Vec<Task> = filtered_tasks_json
        .iter()
        .map(|task_json| serde_json::from_value(task_json.clone()).unwrap())
        .collect();

    println!("Filtered tasks (Task 1 or Task 2, sorted by created_at):");
    for task in &filtered_tasks {
        println!("  - {} (is_complete: {})", task.title, task.is_complete);
    }

    // Example 4: UPDATE with filters
    println!("\nExample 4: UPDATE with filters");

    let update_result = supabase_client
        .from("tasks")
        .with_auth(&access_token)?
        .with_header("Prefer", "return=representation")?
        .eq("user_id", &user_id)
        .eq("is_complete", "false")
        .update(json!({ "is_complete": true }))
        .await?;

    println!(
        "Updated {} tasks to be complete",
        update_result.as_array().unwrap_or(&vec![]).len()
    );

    // Example 5: Using range queries
    println!("\nExample 5: Using range queries");
    for i in 100..105 {
        let task_json = json!({
            "title": format!("Range Task {}", i),
            "description": Some(format!("Description for range task {i}")),
            "is_complete": false,
            "user_id": user_id,
        });
        // Use basic insert
        let _insert_result = supabase_client
            .from("tasks")
            .with_auth(&access_token)?
            .with_header("Prefer", "return=representation")?
            .insert(task_json)
            .await?;
    }

    let range_tasks_json = supabase_client
        .from("tasks")
        .with_auth(&access_token)?
        .select("*")
        .eq("user_id", &user_id)
        .gte("id", "100")
        .lte("id", "102")
        .execute::<serde_json::Value>()
        .await?;

    let range_tasks: Vec<Task> = range_tasks_json
        .iter()
        .map(|task_json| serde_json::from_value(task_json.clone()).unwrap())
        .collect();

    println!("Tasks with IDs between 100 and 102:");
    for task in &range_tasks {
        println!("  - ID: {:?}, Title: {}", task.id, task.title);
    }

    // Example 6: Using COUNT
    println!("\nExample 6: Using COUNT");

    let count_result = supabase_client
        .from("tasks")
        .with_auth(&access_token)?
        .eq("user_id", &user_id)
        .count(true)
        .execute::<serde_json::Value>()
        .await?;

    let count = if count_result.is_empty() {
        0
    } else {
        count_result[0]["count"].as_i64().unwrap_or(0)
    };

    println!("Total number of tasks for user {user_id}: {count}");

    // Example 7: DELETE with filters
    println!("\nExample 7: DELETE with filters");

    let delete_result = supabase_client
        .from("tasks")
        .with_auth(&access_token)?
        .with_header("Prefer", "return=representation")?
        .eq("user_id", &user_id)
        .gte("id", "100")
        .lte("id", "102")
        .delete()
        .await?;

    println!(
        "Deleted {} tasks",
        delete_result.as_array().unwrap_or(&vec![]).len()
    );

    // Example 8: Transaction with savepoints
    println!("\nExample 8: Transaction with savepoints");

    let transaction = supabase_client
        .from("tasks")
        .with_auth(&access_token)?
        .begin_transaction(
            Some(IsolationLevel::ReadCommitted),
            Some(TransactionMode::ReadWrite),
            Some(30),
        )
        .await?;
    println!("Transaction started...");

    // Create a task in the transaction using basic insert
    let transaction_task_json = json!({
        "title": "Transaction Task".to_string(),
        "description": Some("Created in a transaction".to_string()),
        "is_complete": false,
        "user_id": user_id,
    });
    let tasks_in_transaction = transaction.from("tasks");
    // Use basic insert within transaction
    let tx_insert_result = tasks_in_transaction
        .with_header("Prefer", "return=representation")?
        .insert(transaction_task_json)
        .await?;
    // Need to parse ID differently now if insert returns minimal response
    let tx_task_id: i64 = tx_insert_result // Assuming insert now returns something minimal or we handle it
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|obj| obj.get("id"))
        .and_then(|id_val| id_val.as_i64())
        .ok_or_else(|| {
            PostgrestError::DeserializationError(
                "Failed to get ID from insert response".to_string(),
            )
        })?;
    println!("Created task in transaction with ID: {tx_task_id}");
    transaction.savepoint("after_insert").await?;
    println!("Created savepoint 'after_insert'");
    let tasks_in_transaction_update1 = transaction.from("tasks");
    let _tx_update_result = tasks_in_transaction_update1
        .with_header("Prefer", "return=representation")?
        .eq("id", &tx_task_id.to_string())
        .update(json!({ "description": "Updated in transaction" }))
        .await?;
    println!(
        "Updated task in transaction: {}",
        _tx_update_result[0]["description"]
    );
    transaction.savepoint("after_update").await?;
    println!("Created savepoint 'after_update'");
    let tasks_in_transaction_update2 = transaction.from("tasks");
    let tx_update_result2 = tasks_in_transaction_update2
        .with_header("Prefer", "return=representation")?
        .eq("id", &tx_task_id.to_string())
        .update(json!({ "description": "This update will be rolled back" }))
        .await?;
    println!(
        "Updated task again in transaction: {}",
        tx_update_result2[0]["description"]
    );
    transaction.rollback_to_savepoint("after_update").await?;
    println!("Rolled back to savepoint 'after_update'");
    transaction.commit().await?;
    println!("Transaction committed");

    // Example 9: Select the task created in transaction
    println!("\nExample 9: Verify task created in transaction");

    let tx_tasks_json = supabase_client
        .from("tasks")
        .with_auth(&access_token)?
        .select("*")
        .eq("user_id", &user_id)
        .eq("title", "Transaction Task")
        .execute::<serde_json::Value>()
        .await?;

    let tx_tasks: Vec<Task> = tx_tasks_json
        .iter()
        .map(|task_json| serde_json::from_value(task_json.clone()).unwrap())
        .collect();

    println!("Tasks created in transaction:");
    for task in &tx_tasks {
        println!(
            "  - Title: {}, Description: {}",
            task.title,
            task.description.as_ref().unwrap()
        );
        assert_eq!(task.description.as_ref().unwrap(), "Updated in transaction");
    }

    // Example 10: Transaction with rollback
    println!("\nExample 10: Transaction with rollback");

    let transaction2 = supabase_client
        .from("tasks")
        .with_auth(&access_token)?
        .begin_transaction(
            Some(IsolationLevel::ReadCommitted),
            Some(TransactionMode::ReadWrite),
            None,
        )
        .await?;

    // Create a task that will be rolled back using basic insert
    let rollback_task_json = json!({
        "title": "Rollback Task".to_string(),
        "description": Some("This task should be rolled back".to_string()),
        "is_complete": false,
        "user_id": user_id,
    });
    let tasks_in_transaction2 = transaction2.from("tasks");
    let roll_insert_result = tasks_in_transaction2
        .with_header("Prefer", "return=representation")?
        .insert(rollback_task_json)
        .await?;
    // Parse ID from response (assuming minimal response for now)
    let roll_task_id: i64 = roll_insert_result
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|obj| obj.get("id"))
        .and_then(|id_val| id_val.as_i64())
        .ok_or_else(|| {
            PostgrestError::DeserializationError(
                "Failed to get ID from rollback insert response".to_string(),
            )
        })?;
    println!("Created task in transaction2 (will be rolled back) with ID: {roll_task_id}");
    transaction2.rollback().await?;
    println!("Transaction2 rolled back");

    // Example 11: Verify task was not created after rollback
    println!("\nExample 11: Verify task was not created after rollback");

    let rollback_tasks_json = supabase_client
        .from("tasks")
        .with_auth(&access_token)?
        .select("*")
        .eq("user_id", &user_id)
        .eq("title", "Rollback Task")
        .execute::<serde_json::Value>()
        .await?;

    println!(
        "Tasks with title 'Rollback Task' (should be 0): {}",
        rollback_tasks_json.len()
    );
    assert_eq!(
        rollback_tasks_json.len(),
        0,
        "Rollback wasn't successful, found tasks that should have been rolled back"
    );

    // Final cleanup: Use authenticated client to delete tasks for the specific user
    println!("\nFinal cleanup");
    let _ = supabase_client
        .from("tasks")
        .with_auth(&access_token)?
        .with_header("Prefer", "return=representation")?
        .eq("user_id", &user_id)
        .delete()
        .await?;

    println!("Cleaned up all test data for user {user_id}");

    println!("PostgREST example completed");

    Ok(())
}
