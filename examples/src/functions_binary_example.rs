use dotenv::dotenv;
use futures::StreamExt;
use serde_json::json;
use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use supabase_rust_client::client::SupabaseConfig;
use supabase_rust_client::SupabaseClientWrapper;
use supabase_rust_functions::FunctionOptions;

// このサンプルは、Supabase Edge Functionsからバイナリデータを
// ストリーミングで取得し処理する例を示しています

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // .envファイルから環境変数を読み込む
    dotenv().ok();

    // 環境変数からSupabase URLとキーを取得
    let supabase_url = env::var("SUPABASE_URL").expect("SUPABASE_URL must be set");
    let supabase_key = env::var("SUPABASE_KEY").expect("SUPABASE_KEY must be set");

    // Supabaseクライアントを初期化
    let config = SupabaseConfig::new(supabase_url.as_str(), supabase_key)?;
    let supabase = SupabaseClientWrapper::new(config)?;
    println!("バイナリ・ストリーミングの例を開始します");

    // Functionsクライアントにアクセス
    let functions = supabase.functions;

    // 画像を生成するEdge Function (例: generate-image)を呼び出す
    // このEdge Functionはクライアントに画像データをストリーミングで返す想定です
    //
    // Edge Functionの実装例 (JavaScript):
    // Deno.serve(async (req) => {
    //   try {
    //     const { width, height, format } = await req.json();
    //
    //     // 画像生成ロジック...
    //     const imageData = generateImage(width, height);
    //
    //     return new Response(
    //       imageData,
    //       {
    //         headers: {
    //           'Content-Type': format === 'png' ? 'image/png' : 'image/jpeg'
    //         }
    //       }
    //     );
    //   } catch (error) {
    //     return new Response(
    //       JSON.stringify({ error: error.message }),
    //       { status: 400, headers: { 'Content-Type': 'application/json' } }
    //     );
    //   }
    // });

    println!("バイナリデータを取得する例");

    // 1. 小さなバイナリデータを一度に取得
    let image_params = json!({
        "width": 100,
        "height": 100,
        "format": "png"
    });

    match functions
        .invoke_binary("generate-image", Some(image_params), None)
        .await
    {
        Ok(binary_data) => {
            println!("バイナリデータを受信しました: {} バイト", binary_data.len());

            // 受信したデータをファイルに保存
            let output_path = "received_image.png";

            // ファイルに書き込み
            let mut file = File::create(output_path)?;
            file.write_all(&binary_data)?;

            println!("バイナリデータを {output_path} に保存しました");
        }
        Err(e) => {
            println!("バイナリデータの取得でエラーが発生しました: {e:?}");
            println!("注意: この例では 'generate-image' という名前のEdge Functionが必要です");
        }
    }

    // 2. 大きなバイナリデータをストリーミングで取得
    println!("\n大きなバイナリデータをストリーミングで取得する例");

    let large_image_params = json!({
        "width": 2000,
        "height": 2000,
        "format": "png"
    });

    let options = FunctionOptions {
        timeout_seconds: Some(60), // 大きなデータのため、タイムアウトを60秒に設定
        ..Default::default()
    };

    match functions
        .invoke_binary_stream(
            "generate-large-image",
            Some(large_image_params),
            Some(options),
        )
        .await
    {
        Ok(stream) => {
            println!("バイナリストリームの受信を開始しました");

            // 出力ファイルを準備
            let output_path = "received_large_image.png";
            let output_file = Path::new(output_path);
            let mut file = File::create(output_file)?;

            // カウンター
            let mut bytes_received = 0;
            let mut chunks_received = 0;

            // ストリームを処理
            let mut stream = stream;
            while let Some(chunk_result) = stream.next().await {
                match chunk_result {
                    Ok(chunk) => {
                        bytes_received += chunk.len();
                        chunks_received += 1;

                        // 進捗表示
                        if chunks_received % 10 == 0 {
                            println!(
                                "  進捗: {chunks_received} チャンク, {bytes_received} バイト受信"
                            );
                        }

                        // ファイルに書き込み
                        file.write_all(&chunk)?;
                    }
                    Err(e) => {
                        println!("ストリーム処理中にエラーが発生しました: {e:?}");
                        break;
                    }
                }
            }

            println!(
                "ストリーム受信完了: {chunks_received} チャンク, 合計 {bytes_received} バイト"
            );
            println!("データを {output_path} に保存しました");
        }
        Err(e) => {
            println!("バイナリストリームの開始にエラーが発生しました: {e:?}");
        }
    }

    // 3. バイナリデータをチャンクで処理する高度な例
    println!("\nバイナリデータをチャンク単位で処理する例");

    let video_params = json!({
        "duration": 10,
        "format": "mp4"
    });

    match functions
        .invoke_binary_stream("generate-video", Some(video_params), None)
        .await
    {
        Ok(stream) => {
            println!("動画ストリームの受信を開始しました");

            // 出力ファイルを準備
            let output_path = "processed_video.mp4";
            let mut file = File::create(output_path)?;

            // チャンクサイズを定義（例: 64KB）
            let chunk_size = 64 * 1024;

            // チャンク処理関数
            // 実際のアプリケーションでは、ここでフレーム解析や変換などを行うことができます
            let processor = |data: &[u8]| -> std::result::Result<bytes::Bytes, String> {
                // このサンプルでは単純に元のデータを返しますが、
                // 実際のアプリケーションでは何らかの処理を行うことができます
                Ok(bytes::Bytes::copy_from_slice(data))
            };

            // ストリームを処理
            let processed_stream = functions.process_binary_chunks(stream, chunk_size, processor);

            // 処理済みデータを保存
            let mut bytes_processed = 0;
            let mut chunks_processed = 0;

            tokio::pin!(processed_stream);
            while let Some(chunk_result) = processed_stream.next().await {
                match chunk_result {
                    Ok(processed_chunk) => {
                        bytes_processed += processed_chunk.len();
                        chunks_processed += 1;

                        // 進捗表示
                        if chunks_processed % 5 == 0 {
                            println!(
                                "  進捗: {chunks_processed} チャンク, {bytes_processed} バイト処理済み"
                            );
                        }

                        // ファイルに書き込み
                        file.write_all(&processed_chunk)?;
                    }
                    Err(e) => {
                        println!("処理中にエラーが発生しました: {e:?}");
                        break;
                    }
                }
            }

            println!("処理完了: {chunks_processed} チャンク, 合計 {bytes_processed} バイト");
            println!("処理済みデータを {output_path} に保存しました");
        }
        Err(e) => {
            println!("動画ストリームの開始にエラーが発生しました: {e:?}");
        }
    }

    println!("バイナリ・ストリーミングの例が完了しました");

    Ok(())
}
