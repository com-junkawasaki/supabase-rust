use bytes::Bytes;
use dotenv::dotenv;
use std::env;
use std::fs::File as StdFile;
use std::io::{Read, Write};
use std::path::Path;
use supabase_rust_client::client::SupabaseConfig;
use supabase_rust_client::SupabaseClientWrapper;
use supabase_rust_storage::{FileObject, FileOptions, ImageTransformOptions, ListOptions};
use tempfile::NamedTempFile;

mod image_transform_examples {
    use std::env;
    use std::io::Write;
    use supabase_rust_client::client::SupabaseConfig;
    use supabase_rust_client::SupabaseClientWrapper;
    use supabase_rust_storage::{FileOptions, ImageTransformOptions};
    use tempfile::NamedTempFile;

    pub async fn run_image_transform_examples(
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        // Supabaseの認証情報を環境変数から取得
        let supabase_url = env::var("SUPABASE_URL").expect("SUPABASE_URL must be set");
        let supabase_key = env::var("SUPABASE_KEY").expect("SUPABASE_KEY must be set");

        // バケット名とテスト画像のパスを設定
        let bucket_name = "test-images";

        // テスト用の一時画像ファイルを作成
        let mut temp_file = NamedTempFile::new()?;
        // ダミー画像データを書き込む (1x1 PNG)
        let dummy_png_data: &[u8] = &[
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48,
            0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00,
            0x00, 0x1F, 0x15, 0xC4, 0x89, 0x00, 0x00, 0x00, 0x0A, 0x49, 0x44, 0x41, 0x54, 0x08,
            0xD7, 0x63, 0x60, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01, 0xE2, 0x21, 0xBC, 0x33, 0x00,
            0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
        ];
        temp_file.write_all(dummy_png_data)?;
        let local_image_path = temp_file.path().to_path_buf();
        let upload_path = "test-transform-image.jpg";

        // Supabaseクライアントの初期化
        let config = SupabaseConfig::new(supabase_url.as_str(), supabase_key)?;
        let supabase = SupabaseClientWrapper::new(config)?;

        let storage = supabase.storage;

        println!("\n=== 画像変換機能の例 ===\n");

        // バケットが存在するか確認し、なければ作成
        let buckets = storage.list_buckets().await?;
        let bucket_exists = buckets.iter().any(|b| b.name == bucket_name);

        if !bucket_exists {
            println!("バケット '{bucket_name}' を作成します...");
            storage.create_bucket(bucket_name, true).await?;
            println!("バケットを作成しました。");
        }

        // 画像をアップロード
        println!("画像をアップロードしています...");
        // アップロードオプションを追加して、正しいMIMEタイプを設定
        let upload_options = FileOptions::new().with_content_type("image/png");
        let upload_result = storage
            .from(bucket_name)
            .upload(
                upload_path,
                local_image_path.as_path(),
                Some(upload_options),
            )
            .await?;

        println!("画像をアップロードしました: {}", upload_result.name);

        // 画像変換オプションを作成
        let transform_options = vec![
            (
                "サムネイル (小)",
                ImageTransformOptions::new()
                    .with_width(100)
                    .with_height(100)
                    .with_resize("cover"),
            ),
            (
                "中サイズ (WebP)",
                ImageTransformOptions::new()
                    .with_width(300)
                    .with_height(200)
                    .with_resize("contain")
                    .with_format("webp"),
            ),
            (
                "大サイズ (低画質)",
                ImageTransformOptions::new()
                    .with_width(800)
                    .with_height(600)
                    .with_quality(50),
            ),
        ];

        // 各変換オプションで画像を変換
        println!("\n画像変換の例:");

        for (name, options) in transform_options {
            println!("\n{name} の変換:");

            // 変換画像を取得
            let transformed_image = storage
                .from(bucket_name)
                .transform_image(upload_path, options.clone())
                .await?;

            println!("変換後の画像サイズ: {} バイト", transformed_image.len());

            // 変換画像の公開URLを取得
            let public_url = storage
                .from(bucket_name)
                .get_public_transform_url(upload_path, options.clone());

            println!("公開URL: {public_url}");

            // 変換画像の署名付きURLを取得
            let signed_url = storage
                .from(bucket_name)
                .create_signed_transform_url(upload_path, options, 60)
                .await?;

            println!("署名付きURL (有効期限60秒): {signed_url}");
        }

        // クリーンアップ
        println!("\nテスト画像を削除します...");
        storage.from(bucket_name).remove(vec![upload_path]).await?;

        println!("テスト画像を削除しました。");

        Ok(())
    }
}

/// S3互換APIの例を実行
async fn run_s3_compatible_example(
    supabase: SupabaseClientWrapper,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    println!("\n=== S3互換APIの例 ===\n");

    // S3互換オプションを設定
    let s3_options = supabase_rust_storage::s3::S3Options {
        access_key_id: "your-access-key".to_string(), // 実際の環境では適切な値に置き換えてください
        secret_access_key: "your-secret-key".to_string(), // 実際の環境では適切な値に置き換えてください
        region: Some("auto".to_string()),
        ..Default::default()
    };

    // S3互換クライアントを取得
    let storage_client = supabase.storage;
    let bucket_client = storage_client.from("test-bucket");
    let s3_client = bucket_client.s3_compatible(s3_options);

    // テキストファイルをアップロード
    let text_content = "This is a test file uploaded via S3 compatible API";

    // 一時ファイルを作成してテキストを書き込む
    let mut temp_file = NamedTempFile::new()?;
    temp_file.write_all(text_content.as_bytes())?;
    let file_path = temp_file.path();

    println!("Uploading text file via S3 compatible API...");

    // 一時ファイルのパスから読み込んだデータをバイトに変換
    let mut file_data = Vec::new();
    let mut file = StdFile::open(file_path)?;
    file.read_to_end(&mut file_data)?;

    let text_bytes = Bytes::from(file_data);

    s3_client
        .put_object(
            "s3-test/test.txt",
            text_bytes.clone(),
            Some("text/plain".to_string()),
            Some({
                let mut metadata = std::collections::HashMap::new();
                metadata.insert(
                    "description".to_string(),
                    "Test file for S3 API".to_string(),
                );
                metadata
            }),
        )
        .await?;
    println!("Text file uploaded successfully");

    // ファイルをダウンロード
    println!("Downloading file via S3 compatible API...");
    let downloaded_data = s3_client.get_object("s3-test/test.txt").await?;
    let downloaded_text = String::from_utf8_lossy(&downloaded_data);
    println!("Downloaded content: {downloaded_text}");

    // メタデータを取得
    println!("Retrieving file metadata...");
    let metadata = s3_client.head_object("s3-test/test.txt").await?;
    println!("File metadata: {metadata:?}");

    // 複数のファイルをアップロード
    println!("Uploading multiple files...");
    for i in 1..4 {
        let content = format!("This is file number {i}");
        let bytes = Bytes::from(content.into_bytes());
        s3_client
            .put_object(
                &format!("s3-test/multiple/file{i}.txt"),
                bytes,
                Some("text/plain".to_string()),
                None,
            )
            .await?;
    }

    // オブジェクト一覧を取得
    println!("Listing objects...");
    let objects = s3_client
        .list_objects(Some("s3-test/multiple/"), None, None)
        .await?;
    println!("Objects in directory: {objects:?}");

    // オブジェクトを削除
    println!("Deleting objects...");
    s3_client.delete_object("s3-test/test.txt").await?;
    println!("Object deleted");

    Ok(())
}

/// 基本的なストレージ操作の例を実行
async fn run_basic_storage_operations(
    storage: &supabase_rust_storage::StorageClient,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    println!("\n=== 基本的なストレージ操作の例 ===\n");

    // バケット名とテストファイルのパスを設定
    let bucket_name = "test-bucket";
    let upload_path = "test-file.txt";
    let local_file_path = "local-test-file.txt";

    // テスト用の一時ファイルを作成
    let mut file = StdFile::create(local_file_path)?;
    writeln!(file, "これはテストファイルです。")?;

    // バケット一覧を取得
    println!("バケット一覧を取得中...");
    let buckets = storage.list_buckets().await?;
    println!("利用可能なバケット:");
    for bucket in &buckets {
        println!("- {} (Public: {})", bucket.name, bucket.public);
    }

    // バケットが存在するか確認し、なければ作成
    let bucket_exists = buckets.iter().any(|b| b.name == bucket_name);
    if !bucket_exists {
        println!("バケット '{bucket_name}' を作成します...");
        storage.create_bucket(bucket_name, true).await?;
        println!("バケットを作成しました。");
    }

    // ファイルをアップロード
    println!("ファイルをアップロードしています...");
    let upload_options = FileOptions::new().with_content_type("text/plain");
    let upload_result = storage
        .from(bucket_name)
        .upload(
            upload_path,
            std::path::Path::new(local_file_path),
            Some(upload_options),
        )
        .await?;
    println!("ファイルをアップロードしました: {}", upload_result.name);

    // バケット内のファイル一覧を取得
    println!("\nバケット '{bucket_name}' のファイル一覧を取得中...");
    let list_options = ListOptions::new().limit(10).offset(0);
    let files = storage
        .from(bucket_name)
        .list("", Some(list_options))
        .await?;
    println!("ファイル一覧:");
    for file in &files {
        let size = file
            .metadata
            .as_ref()
            .and_then(|m| m.get("size"))
            .and_then(|s| s.as_u64())
            .unwrap_or(0);
        println!("- {} (サイズ: {} バイト)", file.name, size);
    }

    // ファイルの公開URLを取得
    let public_url = storage.from(bucket_name).get_public_url(upload_path);
    println!("\n公開URL: {public_url}");

    // ファイルの署名付きURLを取得
    let signed_url = storage
        .from(bucket_name)
        .create_signed_url(upload_path, 60) // 60秒間有効なURL
        .await?;
    println!("署名付きURL (有効期限60秒): {signed_url}");

    // ファイルをダウンロード
    println!("\nファイルをダウンロードしています...");
    let download_data = storage.from(bucket_name).download(upload_path).await?;
    println!(
        "ダウンロードしたファイルサイズ: {} バイト",
        download_data.len()
    );
    // ダウンロードした内容をファイルに保存（オプション）
    // let mut download_file = StdFile::create("downloaded-test-file.txt")?;\n    // download_file.write_all(&download_data)?;\n\n    // ファイルを移動
    let move_destination = "moved/test-file.txt";
    println!("\nファイルを移動しています: {upload_path} -> {move_destination}");
    storage
        .from(bucket_name)
        .move_object(upload_path, move_destination)
        .await?;
    println!("ファイルを移動しました。");

    // 移動後のファイルを確認
    let moved_files_result = storage.from(bucket_name).list("moved/", None).await;
    println!("移動先のディレクトリの内容:");
    match moved_files_result {
        Ok(moved_files) => {
            for file in moved_files {
                println!("- {}", file.name);
            }
        }
        Err(e) => println!("移動先ディレクトリのリスト取得エラー: {e}"),
    }

    // ファイルを削除
    println!("\nファイルを削除しています...");
    let delete_result = storage
        .from(bucket_name)
        .remove(vec![move_destination])
        .await;

    match delete_result {
        Ok(_) => {
            println!("指定されたファイルが正常に削除されました。");
        }
        Err(e) => println!("ファイル削除エラー: {e}"),
    }

    // バケットを空にする
    // println!("\nバケット '{}' を空にしています...", bucket_name);
    // storage.empty_bucket(bucket_name).await?;
    // println!("バケットを空にしました。");

    // バケットを削除
    // println!("\nバケット '{}' を削除しています...", bucket_name);
    // storage.delete_bucket(bucket_name).await?;
    // println!("バケットを削除しました。");

    // ローカルテストファイルを削除
    std::fs::remove_file(local_file_path)?;

    Ok(())
}

/// 大容量ファイルのアップロード例を実行
async fn run_large_file_upload(
    storage: &supabase_rust_storage::StorageClient,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    println!("\n=== 大容量ファイルのアップロード例 ===\n");

    // テストバケットの名前
    let bucket_name = "test-bucket";

    // バケットが存在するか確認し、なければ作成
    let buckets = storage.list_buckets().await?;
    let bucket_exists = buckets.iter().any(|b| b.name == bucket_name);

    if !bucket_exists {
        println!("バケット '{bucket_name}' を作成します...");
        storage.create_bucket(bucket_name, true).await?;
        println!("バケットを作成しました: {bucket_name}");
    }

    // 大きなテストファイルを作成 (5MB)
    println!("テスト用の大容量ファイルを作成しています...");
    let file_size = 5 * 1024 * 1024; // 5MB

    // 一時ファイルを作成
    let mut temp_file = NamedTempFile::new()?;

    // ランダムデータを生成して書き込む
    let mut large_data = Vec::with_capacity(file_size);
    for i in 0..file_size {
        large_data.push((i % 256) as u8);
    }
    temp_file.write_all(&large_data)?;

    // マルチパートアップロードでファイルをアップロード
    println!("\nマルチパートアップロードを開始しています...");

    let file_path = temp_file.path();

    // アップロードオプションを設定
    let upload_options = FileOptions::new().with_content_type("application/octet-stream");

    // マルチパートアップロードを実行（大きなファイルのため）
    let upload_result = storage
        .from(bucket_name)
        .upload_large_file(
            "large-file.bin",
            file_path,
            1024 * 1024, // 1MBチャンク
            Some(upload_options),
        )
        .await?;

    println!(
        "大容量ファイルのアップロードが完了しました: {}",
        upload_result.name
    );

    // ファイルを削除（クリーンアップ）
    println!("\nファイルを削除しています...");
    storage
        .from(bucket_name)
        .remove(vec!["large-file.bin"])
        .await?;

    println!("ファイルを削除しました");

    Ok(())
}

// Utility function to handle results consistently
#[allow(dead_code)] // Allow dead code for example utility
fn handle_result<T: std::fmt::Debug>(
    operation: &str,
    result: Result<T, Box<dyn std::error::Error>>,
) {
    match result {
        Ok(data) => println!("[OK] {operation}: {data:?}"),
        Err(e) => println!("[FAIL] {operation}: {e}"),
    }
}

// Function to test public bucket operations
#[allow(dead_code)] // Allow dead code for example test function
async fn test_public_operations(
    storage: &supabase_rust_storage::StorageClient,
) -> Result<(), Box<dyn std::error::Error>> {
    let bucket_name = "public-example-bucket";
    println!("\n--- Testing Public Operations in bucket: {bucket_name} ---");

    // Ensure bucket exists (publicly accessible bucket creation)
    match storage.create_bucket(bucket_name, true).await {
        Ok(_) => println!("Public Bucket '{bucket_name}' created or already exists."),
        Err(e) => {
            if !e.to_string().contains("Duplicate") {
                return Err(format!("Failed to ensure public bucket exists: {e}").into());
            }
            println!("Public Bucket '{bucket_name}' already exists.");
        }
    }

    // 1. Public Upload (if bucket allows anonymous uploads)
    let upload_path = "public_file.txt";
    let local_file_path_str = "public_temp.txt";
    let local_file_path = Path::new(local_file_path_str);
    tokio::fs::write(local_file_path, "This is a public file.").await?;
    let upload_options = FileOptions::new().with_content_type("text/plain");

    println!("Attempting public upload to: {upload_path}");
    let upload_result = storage
        .from(bucket_name)
        .upload(upload_path, local_file_path, Some(upload_options))
        .await;
    handle_result(
        "Public Upload",
        upload_result
            .map(|_| "Upload Success".to_string())
            .map_err(Box::from),
    );
    tokio::fs::remove_file(local_file_path).await?;

    // 2. Public List
    println!("Listing files in public bucket");
    let list_options = ListOptions::new().limit(10).offset(0);
    let files_result: Result<Vec<FileObject>, _> =
        storage.from(bucket_name).list("", Some(list_options)).await;

    if let Ok(files) = &files_result {
        println!("Found {} files:", files.len());
        for file in files {
            let size = file
                .metadata
                .as_ref()
                .and_then(|m| m.get("size").and_then(|s| s.as_i64()))
                .unwrap_or(0);
            println!("- {} (サイズ: {} バイト)", file.name, size);
        }
    }
    println!("[INFO] Public List Result: {files_result:?}");

    // 3. Get Public URL
    println!("Getting public URL for: {upload_path}");
    let public_url = storage.from(bucket_name).get_public_url(upload_path);
    println!("Public URL: {public_url}");

    // 4. Public Download (using public URL is typical, but testing direct download)
    println!("Attempting public download of: {upload_path}");
    let download_result = storage.from(bucket_name).download(upload_path).await;
    handle_result(
        "Public Download",
        download_result
            .map(|d| format!("Downloaded {} bytes", d.len()))
            .map_err(Box::from),
    );

    // 5. Public Remove (if permissions allow)
    println!("Attempting public remove of: {upload_path}");
    let remove_result = storage.from(bucket_name).remove(vec![upload_path]).await;
    println!("[INFO] Public Remove Result: {remove_result:?}");

    Ok(())
}

// Function to test image transformations
#[allow(dead_code)] // Allow dead code for example test function
async fn test_image_transformations(
    storage: &supabase_rust_storage::StorageClient,
) -> Result<(), Box<dyn std::error::Error>> {
    let bucket_name = "image-transform-bucket";
    println!("\n--- Testing Image Transformations in bucket: {bucket_name} ---");

    // Ensure bucket exists (public for easy access in example)
    match storage.create_bucket(bucket_name, true).await {
        Ok(_) => println!("Image Bucket '{bucket_name}' created or already exists."),
        Err(e) => {
            if !e.to_string().contains("Duplicate") {
                return Err(format!("Failed to ensure image bucket exists: {e}").into());
            }
            println!("Image Bucket '{bucket_name}' already exists.");
        }
    }

    // Create a dummy image file
    let image_path = "logo.png";
    let local_image_path_str = "temp_logo.png";
    let local_image_path = Path::new(local_image_path_str);
    let mut temp_file = StdFile::create(local_image_path)?;
    // Write some dummy PNG-like data (not a real PNG)
    temp_file.write_all(&[137, 80, 78, 71, 13, 10, 26, 10])?;
    drop(temp_file);

    // Upload the image
    println!("Uploading image: {image_path}");
    let upload_options = FileOptions::new().with_content_type("image/png");
    storage
        .from(bucket_name)
        .upload(image_path, local_image_path, Some(upload_options))
        .await?;
    tokio::fs::remove_file(local_image_path).await?;

    // 1. Get Public Transform URL
    let transform_options = ImageTransformOptions::new()
        .with_width(100)
        .with_height(100)
        .with_resize("cover");
    println!("Getting public transform URL for: {image_path}");
    let transform_url = storage
        .from(bucket_name)
        .get_public_transform_url(image_path, transform_options.clone());
    println!("Public Transform URL: {transform_url}");

    // 2. Create Signed Transform URL (requires auth if bucket is private)
    // Since bucket is public, let's test creating it anyway
    let signed_transform_options = ImageTransformOptions::new()
        .with_width(50)
        .with_quality(75)
        .with_format("webp");
    println!("Creating signed transform URL for: {image_path}");
    let signed_transform_result = storage
        .from(bucket_name)
        .create_signed_transform_url(image_path, signed_transform_options, 3600) // 1 hour expiry
        .await;
    handle_result(
        "Create Signed Transform URL",
        signed_transform_result.map_err(Box::from),
    );

    // 3. Transform Image (direct download, requires auth if private)
    println!("Attempting direct image transformation download");
    let transform_download_result = storage
        .from(bucket_name)
        .transform_image(image_path, transform_options)
        .await;
    handle_result(
        "Direct Transform Download",
        transform_download_result
            .map(|d| format!("Transformed to {} bytes", d.len()))
            .map_err(Box::from),
    );

    // Cleanup: Remove the image
    println!("Cleaning up: Removing image {image_path}");
    storage.from(bucket_name).remove(vec![image_path]).await?;

    Ok(())
}

async fn test_authenticated_operations(
    supabase: SupabaseClientWrapper,
) -> Result<(), Box<dyn std::error::Error>> {
    let bucket_name = "authenticated-bucket";
    println!("\n--- Testing Authenticated Operations in bucket: {bucket_name} ---");
    let storage = supabase.storage;

    // Ensure bucket exists (authenticated bucket creation)
    match storage.create_bucket(bucket_name, false).await {
        Ok(_) => println!("Bucket '{bucket_name}' created or already exists."),
        Err(e) => {
            if !e.to_string().contains("Duplicate") {
                return Err(format!("Failed to ensure bucket exists: {e}").into());
            }
            println!("Bucket '{bucket_name}' already exists.");
        }
    }

    // 1. Authenticated Upload
    let upload_path = "private/secret_file.txt";
    let temp_path_str = "secret_file_temp.txt";
    let temp_path = Path::new(temp_path_str);
    tokio::fs::write(temp_path, "This is a secret file.").await?;
    let file_options = FileOptions::new().with_content_type("text/plain");

    println!("Attempting authenticated upload to: {upload_path}");
    let upload_result = storage
        .from(bucket_name)
        .upload(upload_path, temp_path, Some(file_options))
        .await;
    println!("[INFO] Authenticated Upload Result: {upload_result:?}");
    tokio::fs::remove_file(temp_path).await?;

    // 2. Authenticated List
    println!("Listing files in authenticated bucket with prefix 'private/'");
    let list_options = ListOptions::new().limit(10).offset(0);
    let files_result: Result<Vec<FileObject>, _> = storage
        .from(bucket_name)
        .list("private/", Some(list_options))
        .await;
    println!("[INFO] Authenticated List Result: {files_result:?}");

    // 3. Create Signed URL (for authenticated download)
    let expires_in = 60;
    println!("Creating signed URL for '{upload_path}' expiring in {expires_in} seconds");
    let signed_url_result = storage
        .from(bucket_name)
        .create_signed_url(upload_path, expires_in)
        .await;
    println!("[INFO] Create Signed URL Result: {signed_url_result:?}");

    // 4. Authenticated Download
    println!("Attempting authenticated download of: {upload_path}");
    let download_result = storage.from(bucket_name).download(upload_path).await;
    println!("[INFO] Authenticated Download Result: {download_result:?}");

    // 5. Authenticated Move (Simulated)
    let source_path = "private/move_source.txt";
    let dest_path = "private/moved_destination.txt";
    let temp_source_path_str = "move_source_temp.txt";
    let temp_source_path = Path::new(temp_source_path_str);
    tokio::fs::write(temp_source_path, "File to be moved.").await?;
    println!("Uploading source file for move test: {source_path}");
    storage
        .from(bucket_name)
        .upload(source_path, temp_source_path, None)
        .await?;
    tokio::fs::remove_file(temp_source_path).await?;

    println!("Attempting simulated move: {source_path} -> {dest_path}");
    let remove_result = storage.from(bucket_name).remove(vec![source_path]).await;
    println!("[INFO] Simulated Move (Remove Source) Result: {remove_result:?}");

    // 6. Authenticated Delete
    let delete_path = "private/to_delete.txt";
    let temp_delete_path_str = "to_delete_temp.txt";
    let temp_delete_path = Path::new(temp_delete_path_str);
    tokio::fs::write(temp_delete_path, "Delete me.").await?;
    println!("Uploading file for delete test: {delete_path}");
    storage
        .from(bucket_name)
        .upload(delete_path, temp_delete_path, None)
        .await?;
    tokio::fs::remove_file(temp_delete_path).await?;

    println!("Attempting authenticated delete of: {delete_path}");
    let delete_result = storage.from(bucket_name).remove(vec![delete_path]).await;
    println!("[INFO] Authenticated Delete Result: {delete_result:?}");

    // Cleanup: Delete the authenticated bucket
    println!("Cleaning up: Deleting bucket '{bucket_name}'");
    match storage.delete_bucket(bucket_name).await {
        Ok(_) => println!("Bucket '{bucket_name}' deleted successfully."),
        Err(e) => println!("Failed to delete bucket '{bucket_name}': {e}"),
    }

    Ok(())
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    // Supabaseの認証情報を環境変数から取得
    let supabase_url = env::var("SUPABASE_URL").expect("SUPABASE_URL must be set");
    let supabase_key = env::var("SUPABASE_ANON_KEY").expect("SUPABASE_ANON_KEY must be set");

    println!("Using Supabase URL: {supabase_url}");
    println!("==== Supabase Storage Examples ====");

    // Supabaseクライアントの初期化
    let config = SupabaseConfig::new(supabase_url.as_str(), supabase_key)?;
    let supabase = SupabaseClientWrapper::new(config)?;

    let storage = supabase.storage.clone();

    // 各サンプル関数を呼び出す
    match run_basic_storage_operations(&storage).await {
        Ok(_) => println!("基本的なストレージ操作の例が正常に完了しました。"),
        Err(e) => eprintln!("基本的なストレージ操作の例でエラーが発生しました: {e}"),
    }

    match run_large_file_upload(&storage).await {
        Ok(_) => println!("大容量ファイルのアップロード例が正常に完了しました。"),
        Err(e) => eprintln!("大容量ファイルのアップロード例でエラーが発生しました: {e}"),
    }

    // Note: These examples still create their own unauthenticated clients internally.
    // To fix their auth errors, they need refactoring similar to the above.
    match image_transform_examples::run_image_transform_examples().await {
        Ok(_) => println!("画像変換機能の例が正常に完了しました。"),
        Err(e) => eprintln!("画像変換機能の例でエラーが発生しました: {e}"),
    }

    match run_s3_compatible_example(supabase.clone()).await {
        Ok(_) => println!("S3互換APIの例が正常に完了しました。"),
        Err(e) => eprintln!("S3互換APIの例でエラーが発生しました: {e}"),
    }

    // Ensure authenticated tests run if applicable (they might need adjustment depending on actual auth flow)
    if let Err(e) = test_authenticated_operations(supabase).await {
        println!("Error in authenticated storage operations: {e}");
    }

    println!("\n全ての例が実行されました。");

    Ok(())
}
