use dotenv::dotenv;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::env;
use std::io;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use supabase_rust_client::client::SupabaseConfig;
use supabase_rust_client::SupabaseClientWrapper;
use supabase_rust_realtime::{ChannelEvent, DatabaseChanges, DatabaseFilter, FilterOperator};
use tokio::time::sleep;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Task {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<i32>,
    title: String,
    description: Option<String>,
    is_complete: bool,
    created_at: Option<String>,
    user_id: String,
}

// The payload for Real-time messages
#[derive(Debug, Deserialize)]
struct RealtimePayload<T> {
    #[serde(rename = "type")]
    event_type: String,
    record: Option<T>,
    old_record: Option<T>,
}

/// 高度なフィルタリング機能の例
async fn run_advanced_filter_example(
    supabase: SupabaseClientWrapper,
    user_id: &str,
    access_token: &str,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    println!("\n=== 高度なリアルタイムフィルタリングの例 ===\n");

    // リアルタイムクライアントを取得
    let realtime = supabase.realtime.clone();

    // メッセージ受信カウンターを作成
    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();

    // 高度なフィルターを使用してチャンネル作成
    // 完了済みタスクだけを監視する例
    println!("完了済みタスク（is_complete = true）のみを監視するチャンネルを作成しています...");

    let channel = realtime
        .channel("filtered-tasks")
        .on(
            DatabaseChanges::new("tasks")
                .event(ChannelEvent::Insert)
                .event(ChannelEvent::Update)
                // is_completeがtrueのレコードだけを対象にする
                .eq("is_complete", true),
            move |payload| {
                let counter = counter_clone.clone();
                println!("フィルター付きチャンネルで受信: {payload:?}");
                counter.fetch_add(1, Ordering::SeqCst);

                if let Some(record) = payload.data.get("record") {
                    if let Some(title) = record.get("title") {
                        println!("フィルター付きチャンネルで受信: 完了済みタスク「{title}」");
                    }

                    if let Some(is_complete) = record.get("is_complete") {
                        // タスクは必ず完了済み (is_complete = true) のはず
                        if let Some(is_complete) = is_complete.as_bool() {
                            assert!(is_complete);
                        }
                    }
                }
            },
        )
        .subscribe()
        .await?;

    println!("フィルター付きチャンネルを作成しました");

    // 複数のタスクを作成し、一部だけを完了済みにする

    // 5つのタスクを作成（最初は全て未完了）
    for i in 1..6 {
        let task = Task {
            id: None,
            title: format!("フィルタリングテスト用タスク {i}"),
            description: Some(format!("フィルター機能テスト用 {i}")),
            is_complete: false,
            created_at: None,
            user_id: user_id.to_string(),
        };

        println!("タスクを作成: {}", task.title);
        let insert_result = supabase
            .from("tasks")
            .with_auth(access_token)?
            .insert(json!(task))
            .await?;

        let task_id = if let Some(task) = insert_result.get(0) {
            task["id"].as_i64().unwrap()
        } else {
            continue;
        };

        // 偶数番目のタスクを完了済みに更新（フィルターに合致）
        if i % 2 == 0 {
            println!("タスク {i} を完了済みに更新（フィルターに合致）");
            let update_result = supabase
                .from("tasks")
                .with_auth(access_token)?
                .eq("id", &task_id.to_string())
                .update(json!({ "is_complete": true }))
                .await?;

            println!("更新結果: {update_result:?}");
        }

        // 少し待機してイベントを処理させる
        sleep(Duration::from_millis(500)).await;
    }

    // 少し待機してすべてのイベントを処理させる
    sleep(Duration::from_secs(2)).await;

    // 複合条件を使用したフィルタリングを追加で試す
    println!("\n複合条件を使ったフィルタリングテスト:");

    // カウンターをリセット
    let counter2 = Arc::new(AtomicU32::new(0));
    let counter2_clone = counter2.clone();

    // タイトルに「3」または「5」を含み、かつ未完了のタスクに一致するフィルター
    let channel2 = realtime
        .channel("complex-filter")
        .on(
            DatabaseChanges::new("tasks")
                .event(ChannelEvent::Insert)
                .event(ChannelEvent::Update)
                // タイトルが「3」を含むタスク
                .filter(DatabaseFilter {
                    column: "title".to_string(),
                    operator: FilterOperator::Eq,
                    value: json!("3"),
                })
                // または「5」を含むタスク
                .filter(DatabaseFilter {
                    column: "title".to_string(),
                    operator: FilterOperator::Eq,
                    value: json!("5"),
                })
                // かつ未完了のタスク
                .eq("is_complete", false),
            move |payload| {
                let counter = counter2_clone.clone();
                println!("複合フィルターで受信: {payload:?}");
                counter.fetch_add(1, Ordering::SeqCst);

                if let Some(record) = payload.data.get("record") {
                    if let Some(title) = record.get("title") {
                        println!("複合フィルターで受信: 「{title}」");

                        if let Some(title_str) = title.as_str() {
                            // タスクのタイトルには「3」または「5」が含まれるはず
                            assert!(title_str.contains('3') || title_str.contains('5'));
                        }
                    }

                    if let Some(is_complete) = record.get("is_complete") {
                        // タスクは必ず未完了 (is_complete = false) のはず
                        if let Some(is_complete) = is_complete.as_bool() {
                            assert!(!is_complete);
                        }
                    }
                }
            },
        )
        .subscribe()
        .await?;

    println!("複合フィルター付きチャンネルを作成しました");

    // タスク3と5を更新
    for &i in &[3, 5] {
        let task_title = format!("フィルタリングテスト用タスク {i}");
        println!("タスク {i} の説明を更新（複合フィルターに合致）");

        // タスクを取得
        let task_list: Vec<serde_json::Value> = supabase
            .from("tasks")
            .select("*")
            .with_auth(access_token)?
            .eq("title", &task_title)
            .eq("user_id", user_id)
            .execute()
            .await?;

        if !task_list.is_empty() {
            let task_id = task_list[0]["id"].as_i64().unwrap();

            // タスクを更新
            let update_client = supabase.from("tasks");
            let update_result = update_client
                .with_auth(access_token)?
                .eq("id", &task_id.to_string())
                .update(json!({
                    "description": format!("複合フィルターテスト用に更新 {}", i)
                }))
                .await?;

            println!("更新結果: {update_result:?}");
        }

        // 少し待機
        sleep(Duration::from_millis(500)).await;
    }

    // 少し待機してすべてのイベントを処理させる
    sleep(Duration::from_secs(2)).await;

    // 結果を確認
    let received_count = counter.load(Ordering::SeqCst);
    println!("\n検証: 完了済みタスクフィルターで受信したイベント数: {received_count}");

    let received_count2 = counter2.load(Ordering::SeqCst);
    println!("検証: 複合フィルターで受信したイベント数: {received_count2}");

    // 購読を終了
    // トークンを使用してチャンネルの使用を終了
    // 注: unsubscribe メソッドがなくても、subscription がドロップされると内部的にクリーンアップされます
    println!("\n購読を終了します...");
    drop(channel);
    drop(channel2);

    println!("購読を終了しました");

    // テスト後のクリーンアップ
    println!("\nテスト後のクリーンアップ...");

    // クリーンアップ - 作成したすべてのタスクを削除
    println!("\nクリーンアップ - すべてのテストタスクを削除");

    let delete_client = supabase.from("tasks");
    let delete_result = delete_client
        .with_auth(access_token)?
        .eq("user_id", user_id)
        .delete()
        .await?;

    println!("削除結果: {delete_result:?}");
    println!("すべてのテストタスクを削除しました");

    Ok(())
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

    println!("Starting Realtime example");

    // First, sign up a test user for our examples
    let test_email = format!("test-realtime-{}@example.com", uuid::Uuid::new_v4());
    let test_password = "password123";

    let sign_up_result = supabase.auth.sign_up(&test_email, test_password).await?;

    let user_id = sign_up_result.user.id.clone();
    let access_token = sign_up_result.access_token.clone();

    println!("Created test user with ID: {user_id}");

    // --- Set Auth Token for Realtime ---
    let realtime = supabase.realtime.clone();
    realtime.set_auth(Some(access_token.clone())).await; // Set the token

    // タスクテーブルの準備
    println!("\n基本的なリアルタイム購読のデモを開始します");

    // シンプルなチャンネルを作成
    println!("Tasksテーブルに対する基本的なチャンネルを作成します...");

    // メッセージを受信するカウンター
    let message_counter = Arc::new(AtomicU32::new(0));
    let counter_clone = message_counter.clone();

    // チャンネルを作成し、tasksテーブルの変更を購読
    let channel = realtime
        .channel("tasks")
        .on(
            DatabaseChanges::new("tasks")
                .event(ChannelEvent::Insert)
                .event(ChannelEvent::Update)
                .event(ChannelEvent::Delete),
            move |payload| {
                let counter = counter_clone.clone();
                // 受信メッセージをカウント
                counter.fetch_add(1, Ordering::SeqCst);

                // JSONペイロードをパース
                if let Ok(payload) =
                    serde_json::from_value::<RealtimePayload<Task>>(json!(payload.data))
                {
                    match payload.event_type.as_str() {
                        "INSERT" => {
                            if let Some(record) = payload.record {
                                println!(
                                    "INSERT イベント: タスク「{}」が作成されました",
                                    record.title
                                );
                            }
                        }
                        "UPDATE" => {
                            if let (Some(record), Some(old)) = (payload.record, payload.old_record)
                            {
                                println!(
                                    "UPDATE イベント: タスク「{}」が更新されました",
                                    record.title
                                );
                                println!("  更新前: is_complete = {}", old.is_complete);
                                println!("  更新後: is_complete = {}", record.is_complete);
                            }
                        }
                        "DELETE" => {
                            if let Some(old) = payload.old_record {
                                println!(
                                    "DELETE イベント: タスク「{}」が削除されました",
                                    old.title
                                );
                            }
                        }
                        _ => println!("Unknown event type: {}", payload.event_type),
                    }
                } else {
                    println!("Failed to parse payload");
                }
            },
        )
        .subscribe()
        .await?;

    println!("チャンネルの購読を開始しました");

    // いくつかのタスクを作成して更新・削除し、リアルタイムイベントをテスト
    // PostgrestClientを直接取得せずに各操作で個別に.from("tasks")を使用

    // 1. タスクを作成
    println!("\nテスト用タスクを作成します");

    for i in 1..4 {
        let task = Task {
            id: None,
            title: format!("Realtime Task {i}"),
            description: Some(format!("Test task for realtime #{i}")),
            is_complete: false,
            created_at: None,
            user_id: user_id.clone(),
        };

        supabase
            .from("tasks")
            .with_auth(&access_token)?
            .insert(json!(task))
            .await?;

        println!("タスク {i} を作成しました");

        // イベントが処理される時間を確保
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // 2. タスクを更新
    println!("\nタスク1を「完了」に更新します");

    // タスク1を検索
    let task1_list = supabase
        .from("tasks")
        .select("*")
        .with_auth(&access_token)?
        .eq("title", "Realtime Task 1")
        .eq("user_id", &user_id)
        .execute::<serde_json::Value>()
        .await?;

    if !task1_list.is_empty() {
        let task1_id = task1_list[0]["id"].as_i64().unwrap();

        // タスクを更新
        let update_client = supabase.from("tasks");
        let update_result = update_client
            .with_auth(&access_token)?
            .eq("id", &task1_id.to_string())
            .update(json!({ "is_complete": true }))
            .await?;

        println!("更新結果: {update_result:?}");
    }

    println!("タスク1を更新しました");

    // イベントが処理される時間を確保
    tokio::time::sleep(Duration::from_secs(1)).await;

    // 3. タスクを削除
    println!("\nタスク2を削除します");

    // タスク2を検索
    let task2_list = supabase
        .from("tasks")
        .select("*")
        .with_auth(&access_token)?
        .eq("title", "Realtime Task 2")
        .eq("user_id", &user_id)
        .execute::<serde_json::Value>()
        .await?;

    if !task2_list.is_empty() {
        let task2_id = task2_list[0]["id"].as_i64().unwrap();

        // タスクを削除
        let delete_client = supabase.from("tasks");
        let delete_result = delete_client
            .with_auth(&access_token)?
            .eq("id", &task2_id.to_string())
            .delete()
            .await?;

        println!("削除結果: {delete_result:?}");
    }

    println!("タスク2を削除しました");

    // イベントが処理される時間を確保
    tokio::time::sleep(Duration::from_secs(1)).await;

    // 結果を表示
    let received_count = message_counter.load(Ordering::SeqCst);
    println!("\n受信したリアルタイムイベント数: {received_count}");
    println!("期待値: 5 (作成イベント3件 + 更新イベント1件 + 削除イベント1件)");

    // 高度なフィルタリングの例を実行
    if let Err(e) = run_advanced_filter_example(supabase.clone(), &user_id, &access_token).await {
        println!("フィルタリング例でエラーが発生しました: {e}");
    }

    // クリーンアップ - 作成したすべてのタスクを削除
    println!("\nクリーンアップ - すべてのテストタスクを削除");

    let delete_client = supabase.from("tasks");
    let delete_result = delete_client
        .with_auth(&access_token)?
        .eq("user_id", &user_id)
        .delete()
        .await?;

    println!("削除結果: {delete_result:?}");
    println!("すべてのテストタスクを削除しました");

    // チャンネルの購読を解除
    println!("\n続行するには何かキーを押してください...");
    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    // チャンネルを解放すると自動的に購読解除される
    drop(channel);
    println!("チャンネルの購読を解除しました");

    // --- Clear Auth Token (optional cleanup) ---
    realtime.set_auth(None).await; // Clear the token before exiting

    println!("Realtime example completed");

    Ok(())
}
