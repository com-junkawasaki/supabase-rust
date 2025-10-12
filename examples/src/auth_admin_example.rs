use dotenv::dotenv;
use reqwest::Client;
use serde_json::json;
use std::env;
use supabase_rust_auth::AdminAuth;
// use supabase_rust_gftd::Supabase; // Unused import

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // .envファイルから環境変数を読み込む
    dotenv().ok();

    // 環境変数からSupabase URLとキーを取得
    let supabase_url = env::var("SUPABASE_URL").expect("SUPABASE_URL must be set");
    // let supabase_key = env::var("SUPABASE_KEY").expect("SUPABASE_KEY must be set"); // Unused variable
    let service_role_key = env::var("SUPABASE_SERVICE_ROLE_KEY")
        .expect("SUPABASE_SERVICE_ROLE_KEY must be set for admin actions");

    // HTTPクライアントを初期化
    let http_client = Client::new();

    // AdminAuthクライアントを直接初期化
    let admin = AdminAuth::new(
        &format!("{supabase_url}/auth/v1"),
        &service_role_key,
        http_client,
    );

    println!("Auth Admin API の例を開始します");

    // ユーザー一覧の取得
    println!("\n1. ユーザー一覧を取得");
    match admin.list_users(Some(1), Some(10)).await {
        Ok(users) => {
            println!("取得成功: {} ユーザーを取得", users.len());
            for (i, user) in users.iter().enumerate() {
                println!("  {}. ID: {}, Email: {:?}", i + 1, user.id, user.email);
            }
        }
        Err(e) => {
            println!("ユーザー一覧の取得に失敗: {e:?}");
        }
    }

    // 新しいユーザーの作成
    println!("\n2. 新しいユーザーを作成");
    let test_email = format!("test-admin-{}@example.com", uuid::Uuid::new_v4());

    let user_metadata = json!({
        "first_name": "Test",
        "last_name": "User",
        "role": "tester"
    });

    match admin
        .create_user(
            &test_email,
            Some("password123"),
            Some(user_metadata),
            Some(true),
        )
        .await
    {
        Ok(user) => {
            println!("ユーザー作成成功:");
            println!("  ID: {}", user.id);
            println!("  Email: {:?}", user.email);
            println!("  メタデータ: {:?}", user.user_metadata);

            // 作成したユーザーのIDを保存
            let created_user_id = user.id.clone();

            // ユーザー情報の取得
            println!("\n3. 特定のユーザーを取得");
            match admin.get_user_by_id(&created_user_id).await {
                Ok(user) => {
                    println!("ユーザー取得成功:");
                    println!("  ID: {}", user.id);
                    println!("  Email: {:?}", user.email);
                    println!("  メタデータ: {:?}", user.user_metadata);
                }
                Err(e) => {
                    println!("ユーザーの取得に失敗: {e:?}");
                }
            }

            // ユーザー情報の更新
            println!("\n4. ユーザー情報を更新");
            let update_data = json!({
                "user_metadata": {
                    "first_name": "Updated",
                    "last_name": "User",
                    "role": "admin_tester"
                },
                "email_confirm": true
            });

            match admin.update_user(&created_user_id, update_data).await {
                Ok(updated_user) => {
                    println!("ユーザー更新成功:");
                    println!("  ID: {}", updated_user.id);
                    println!("  メタデータ: {:?}", updated_user.user_metadata);
                }
                Err(e) => {
                    println!("ユーザーの更新に失敗: {e:?}");
                }
            }

            // マジックリンクの生成
            println!("\n5. マジックリンクを生成");
            match admin
                .generate_link(
                    &test_email,
                    "magiclink",
                    Some("https://example.com/auth/callback"),
                )
                .await
            {
                Ok(link) => {
                    println!("マジックリンク生成成功:");
                    println!("  リンク: {link}");
                }
                Err(e) => {
                    println!("マジックリンクの生成に失敗: {e:?}");
                }
            }

            // ユーザーの削除
            println!("\n6. ユーザーを削除");
            match admin.delete_user(&created_user_id).await {
                Ok(_) => {
                    println!("ユーザー削除成功: ID {created_user_id}");
                }
                Err(e) => {
                    println!("ユーザーの削除に失敗: {e:?}");
                }
            }
        }
        Err(e) => {
            println!("ユーザーの作成に失敗: {e:?}");
        }
    }

    // 招待メールの送信
    println!("\n7. ユーザーに招待メールを送信");
    let invite_email = format!("invite-{}@example.com", uuid::Uuid::new_v4());

    match admin
        .invite_user_by_email(&invite_email, Some("https://example.com/welcome"))
        .await
    {
        Ok(user) => {
            println!("招待成功:");
            println!("  ID: {}", user.id);
            println!("  Email: {:?}", user.email);

            // ユーザーを削除（クリーンアップ）
            println!("  招待したユーザーを削除（クリーンアップ）");
            match admin.delete_user(&user.id).await {
                Ok(_) => {
                    println!("  ユーザー削除成功: ID {}", user.id);
                }
                Err(e) => {
                    println!("  ユーザーの削除に失敗: {e:?}");
                }
            }
        }
        Err(e) => {
            println!("招待の送信に失敗: {e:?}");
        }
    }

    println!("\nAuth Admin API の例が完了しました");

    Ok(())
}
