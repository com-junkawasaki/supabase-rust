//! Supabase Edge Functions client for Rust
//!
//! This crate provides functionality for invoking Supabase Edge Functions.

use base64::Engine;
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::{Stream, StreamExt};
use reqwest::{Client, Response, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::time::Duration;
use thiserror::Error;
use url::Url;

/// エラー型の詳細
#[derive(Debug, Clone, Deserialize)]
pub struct FunctionErrorDetails {
    pub message: Option<String>,
    pub status: Option<u16>,
    pub code: Option<String>,
    pub details: Option<Value>,
}

/// エラー型
#[derive(Debug, Error)]
pub enum FunctionsError {
    #[error("Request error: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("URL parse error: {0}")]
    UrlError(#[from] url::ParseError),

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Function error (status: {status}): {message}")]
    FunctionError {
        message: String,
        status: StatusCode,
        details: Option<FunctionErrorDetails>,
    },

    #[error("Timeout error: Function execution exceeded timeout limit")]
    TimeoutError,

    #[error("Invalid response: {0}")]
    InvalidResponse(String),
}

impl FunctionsError {
    pub fn new(message: String) -> Self {
        Self::FunctionError {
            message,
            status: StatusCode::INTERNAL_SERVER_ERROR,
            details: None,
        }
    }

    pub fn from_response(response: &Response) -> Self {
        Self::FunctionError {
            message: format!("Function returned error status: {}", response.status()),
            status: response.status(),
            details: None,
        }
    }

    pub fn with_details(response: &Response, details: FunctionErrorDetails) -> Self {
        Self::FunctionError {
            message: details.message.as_ref().map_or_else(
                || format!("Function returned error status: {}", response.status()),
                |msg| msg.clone(),
            ),
            status: response.status(),
            details: Some(details),
        }
    }
}

pub type Result<T> = std::result::Result<T, FunctionsError>;

/// 関数呼び出しオプション
#[derive(Clone, Debug)]
pub struct FunctionOptions {
    /// カスタムHTTPヘッダー
    pub headers: Option<HashMap<String, String>>,

    /// 関数タイムアウト（秒）
    pub timeout_seconds: Option<u64>,

    /// レスポンスのコンテンツタイプを指定（デフォルトはJSONとして処理）
    pub response_type: ResponseType,

    /// リクエストのコンテンツタイプ
    pub content_type: Option<String>,
}

impl Default for FunctionOptions {
    fn default() -> Self {
        Self {
            headers: None,
            timeout_seconds: None,
            response_type: ResponseType::Json,
            content_type: None,
        }
    }
}

/// レスポンスの処理方法
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResponseType {
    /// JSONとしてパースする（デフォルト）
    Json,

    /// テキストとして処理する
    Text,

    /// バイトデータとして処理する
    Binary,

    /// ストリームとして処理する
    Stream,
}

/// ストリーミングレスポンス用の型
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>;

/// 関数レスポンス
#[derive(Debug, Clone)]
pub struct FunctionResponse<T> {
    /// レスポンスデータ
    pub data: T,

    /// HTTPステータスコード
    pub status: StatusCode,

    /// レスポンスヘッダー
    pub headers: HashMap<String, String>,
}

/// Edge Functions クライアント
pub struct FunctionsClient {
    base_url: String,
    api_key: String,
    http_client: Client,
}

/// 関数リクエストを表す構造体
pub struct FunctionRequest<'a, T> {
    client: &'a FunctionsClient,
    function_name: String,
    _response_type: std::marker::PhantomData<T>,
}

impl<'a, T: DeserializeOwned> FunctionRequest<'a, T> {
    /// 関数を実行する
    pub async fn execute<B: Serialize>(
        &self,
        body: Option<B>,
        options: Option<FunctionOptions>,
    ) -> Result<T> {
        let result = self
            .client
            .invoke::<T, B>(&self.function_name, body, options)
            .await?;
        Ok(result.data)
    }
}

impl FunctionsClient {
    /// 新しい Edge Functions クライアントを作成
    pub fn new(supabase_url: &str, supabase_key: &str, http_client: Client) -> Self {
        Self {
            base_url: supabase_url.to_string(),
            api_key: supabase_key.to_string(),
            http_client,
        }
    }

    /// Edge Function を呼び出す
    pub async fn invoke<T: DeserializeOwned, B: Serialize>(
        &self,
        function_name: &str,
        body: Option<B>,
        options: Option<FunctionOptions>,
    ) -> Result<FunctionResponse<T>> {
        let opts = options.unwrap_or_default();

        // URLの構築
        let mut url = Url::parse(&self.base_url)?;
        url.path_segments_mut()
            .map_err(|_| FunctionsError::UrlError(url::ParseError::EmptyHost))?
            .push("functions")
            .push("v1")
            .push(function_name);

        // リクエストの構築
        let mut request_builder = self
            .http_client
            .post(url)
            .header("apikey", &self.api_key)
            .header("Authorization", format!("Bearer {}", &self.api_key));

        // リクエストタイムアウトの設定
        if let Some(timeout) = opts.timeout_seconds {
            request_builder = request_builder.timeout(Duration::from_secs(timeout));
        }

        // コンテンツタイプの設定
        if let Some(content_type) = opts.content_type {
            request_builder = request_builder.header("Content-Type", content_type);
        }

        // カスタムヘッダーの追加
        if let Some(headers) = opts.headers {
            for (key, value) in headers {
                request_builder = request_builder.header(key, value);
            }
        }

        // リクエストボディの追加
        if let Some(body_data) = body {
            request_builder = request_builder.json(&body_data);
        }

        // リクエストの送信
        let response = request_builder.send().await.map_err(|e| {
            if e.is_timeout() {
                FunctionsError::TimeoutError
            } else {
                FunctionsError::from(e)
            }
        })?;

        // ステータスコードの確認
        let status = response.status();
        if !status.is_success() {
            // レスポンスのクローンを作成してエラー処理に使用
            let status_copy = status;

            // エラーレスポンスのパース
            let error_body = response
                .text()
                .await
                .unwrap_or_else(|_| "Failed to read error response".to_string());

            if let Ok(error_details) = serde_json::from_str::<FunctionErrorDetails>(&error_body) {
                return Err(FunctionsError::FunctionError {
                    message: error_details.message.as_ref().map_or_else(
                        || format!("Function returned error status: {status_copy}"),
                        |msg| msg.clone(),
                    ),
                    status: status_copy,
                    details: Some(error_details),
                });
            } else {
                return Err(FunctionsError::FunctionError {
                    message: error_body,
                    status: status_copy,
                    details: None,
                });
            }
        }

        // レスポンスヘッダーの抽出
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_str().unwrap_or("").to_string()))
            .collect::<HashMap<String, String>>();

        // レスポンスタイプに応じた処理
        match opts.response_type {
            ResponseType::Json => {
                let data = response.json::<T>().await.map_err(|e| {
                    FunctionsError::JsonError(serde_json::from_str::<T>("{}").err().unwrap_or_else(
                        || {
                            serde_json::Error::io(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                e.to_string(),
                            ))
                        },
                    ))
                })?;

                Ok(FunctionResponse {
                    data,
                    status,
                    headers,
                })
            }
            ResponseType::Text => {
                // テキスト処理
                let text = response.text().await?;

                // テキストからデシリアライズを試みる
                let data: T = serde_json::from_str(&text).unwrap_or_else(|_| {
                    panic!("Failed to deserialize text response as requested type")
                });

                Ok(FunctionResponse {
                    data,
                    status,
                    headers,
                })
            }
            ResponseType::Binary => {
                // バイナリデータ処理
                let bytes = response.bytes().await?;

                // Base64エンコード（非推奨API対応）
                let binary_str = base64::engine::general_purpose::STANDARD.encode(&bytes);

                // バイナリデータをデシリアライズ
                let data: T =
                    serde_json::from_str(&format!("\"{binary_str}\"")).unwrap_or_else(|_| {
                        panic!("Failed to deserialize binary response as requested type")
                    });

                Ok(FunctionResponse {
                    data,
                    status,
                    headers,
                })
            }
            ResponseType::Stream => {
                // ストリームレスポンスの場合、通常のデシリアライズではなく
                // 別のストリーム処理用のメソッドを使用する必要がある
                Err(FunctionsError::InvalidResponse(
                    "Stream response type cannot be handled by invoke(). Use invoke_stream() instead.".to_string()
                ))
            }
        }
    }

    /// JSONを返すファンクションを呼び出す（シンプルなラッパー）
    pub async fn invoke_json<T: DeserializeOwned, B: Serialize>(
        &self,
        function_name: &str,
        body: Option<B>,
    ) -> Result<T> {
        let options = FunctionOptions {
            response_type: ResponseType::Json,
            ..Default::default()
        };

        let response = self
            .invoke::<T, B>(function_name, body, Some(options))
            .await?;
        Ok(response.data)
    }

    /// テキストを返すファンクションを呼び出す（シンプルなラッパー）
    pub async fn invoke_text<B: Serialize>(
        &self,
        function_name: &str,
        body: Option<B>,
    ) -> Result<String> {
        let options = FunctionOptions {
            response_type: ResponseType::Text,
            ..Default::default()
        };

        // URLの構築 (invokeからコピー＆修正)
        let mut url = Url::parse(&self.base_url)?;
        url.path_segments_mut()
            .map_err(|_| FunctionsError::UrlError(url::ParseError::EmptyHost))?
            .push("functions")
            .push("v1")
            .push(function_name);

        // リクエストの構築 (invokeからコピー＆修正)
        let mut request_builder = self
            .http_client
            .post(url)
            .header("apikey", &self.api_key)
            .header("Authorization", format!("Bearer {}", &self.api_key));

        // リクエストタイムアウトの設定
        if let Some(timeout) = options.timeout_seconds {
            request_builder = request_builder.timeout(Duration::from_secs(timeout));
        }

        // コンテンツタイプの設定
        if let Some(content_type) = options.content_type {
            request_builder = request_builder.header("Content-Type", content_type);
        } else {
            request_builder = request_builder.header("Content-Type", "application/json");
        }

        // Accept ヘッダーを設定 (テキストを期待)
        request_builder = request_builder.header("Accept", "text/plain, */*;q=0.9");

        // カスタムヘッダーの追加
        if let Some(headers) = options.headers {
            for (key, value) in headers {
                request_builder = request_builder.header(key, value);
            }
        }

        // リクエストボディの追加
        if let Some(body_data) = body {
            request_builder = request_builder.json(&body_data);
        }

        // リクエストの送信
        let response = request_builder.send().await.map_err(|e| {
            if e.is_timeout() {
                FunctionsError::TimeoutError
            } else {
                FunctionsError::from(e)
            }
        })?;

        // ステータスコードの確認
        let status = response.status();
        if !status.is_success() {
            // エラーレスポンスのパース (invokeからコピー)
            let error_body = response
                .text()
                .await
                .unwrap_or_else(|_| "Failed to read error response".to_string());
            if let Ok(error_details) = serde_json::from_str::<FunctionErrorDetails>(&error_body) {
                return Err(FunctionsError::FunctionError {
                    message: error_details.message.as_ref().map_or_else(
                        || format!("Function returned error status: {status}"),
                        |msg| msg.clone(),
                    ),
                    status,
                    details: Some(error_details),
                });
            } else {
                return Err(FunctionsError::FunctionError {
                    message: error_body,
                    status,
                    details: None,
                });
            }
        }

        // テキストを直接取得
        response.text().await.map_err(FunctionsError::from)
    }

    /// バイナリ形式で関数レスポンスを取得
    pub async fn invoke_binary<B: Serialize>(
        &self,
        function_name: &str,
        body: Option<B>,
        options: Option<FunctionOptions>,
    ) -> Result<Bytes> {
        let options = options.unwrap_or_else(|| FunctionOptions {
            response_type: ResponseType::Binary,
            ..Default::default()
        });

        // URLの構築
        let mut url = Url::parse(&self.base_url)?;
        url.path_segments_mut()
            .map_err(|_| FunctionsError::UrlError(url::ParseError::EmptyHost))?
            .push("functions")
            .push("v1")
            .push(function_name);

        // リクエストの構築
        let mut request_builder = self
            .http_client
            .post(url)
            .header("apikey", &self.api_key)
            .header("Authorization", format!("Bearer {}", &self.api_key));

        // リクエストタイムアウトの設定
        if let Some(timeout) = options.timeout_seconds {
            request_builder = request_builder.timeout(Duration::from_secs(timeout));
        }

        // コンテンツタイプの設定
        if let Some(content_type) = options.content_type {
            request_builder = request_builder.header("Content-Type", content_type);
        } else {
            // デフォルトはJSON
            request_builder = request_builder.header("Content-Type", "application/json");
        }

        // Accept ヘッダーを設定
        request_builder = request_builder.header("Accept", "application/octet-stream");

        // カスタムヘッダーの追加
        if let Some(headers) = options.headers {
            for (key, value) in headers {
                request_builder = request_builder.header(key, value);
            }
        }

        // リクエストボディの追加
        if let Some(body_data) = body {
            request_builder = request_builder.json(&body_data);
        }

        // リクエストの送信
        let response = request_builder.send().await.map_err(|e| {
            if e.is_timeout() {
                FunctionsError::TimeoutError
            } else {
                FunctionsError::from(e)
            }
        })?;

        // ステータスコードの確認
        let status = response.status();
        if !status.is_success() {
            // エラーレスポンスのパース
            let error_body = response
                .text()
                .await
                .unwrap_or_else(|_| "Failed to read error response".to_string());

            if let Ok(error_details) = serde_json::from_str::<FunctionErrorDetails>(&error_body) {
                return Err(FunctionsError::FunctionError {
                    message: error_details.message.as_ref().map_or_else(
                        || format!("Function returned error status: {status}"),
                        |msg| msg.clone(),
                    ),
                    status,
                    details: Some(error_details),
                });
            } else {
                return Err(FunctionsError::FunctionError {
                    message: error_body,
                    status,
                    details: None,
                });
            }
        }

        // バイナリデータを返す
        response.bytes().await.map_err(FunctionsError::from)
    }

    /// バイナリストリームを取得するメソッド（大きなバイナリデータに最適）
    pub async fn invoke_binary_stream<B: Serialize>(
        &self,
        function_name: &str,
        body: Option<B>,
        options: Option<FunctionOptions>,
    ) -> Result<ByteStream> {
        let opts = options.unwrap_or_else(|| FunctionOptions {
            response_type: ResponseType::Stream,
            content_type: Some("application/octet-stream".to_string()),
            ..Default::default()
        });

        let mut custom_opts = opts;
        let mut headers = custom_opts.headers.unwrap_or_default();
        headers.insert("Accept".to_string(), "application/octet-stream".to_string());
        custom_opts.headers = Some(headers);

        self.invoke_stream(function_name, body, Some(custom_opts))
            .await
    }

    /// チャンク単位でバイナリを処理する補助メソッド
    pub fn process_binary_chunks<F>(
        &self,
        stream: ByteStream,
        chunk_size: usize,
        mut processor: F,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes>> + Send + '_>>
    where
        F: FnMut(&[u8]) -> std::result::Result<Bytes, String> + Send + 'static,
    {
        Box::pin(async_stream::stream! {
            let mut buffer = BytesMut::new();

            tokio::pin!(stream);
            while let Some(chunk_result) = stream.next().await {
                match chunk_result {
                    Ok(chunk) => {
                        // バッファに追加
                        buffer.extend_from_slice(&chunk);

                        // chunk_sizeを超えたら処理
                        while buffer.len() >= chunk_size {
                            let chunk_to_process = buffer.split_to(chunk_size);
                            match processor(&chunk_to_process) {
                                Ok(processed) => yield Ok(processed),
                                Err(err) => {
                                    yield Err(FunctionsError::InvalidResponse(err));
                                    return;
                                }
                            }
                        }
                    },
                    Err(e) => {
                        yield Err(e);
                        return;
                    }
                }
            }

            // 残りのバッファを処理
            if !buffer.is_empty() {
                match processor(&buffer) {
                    Ok(processed) => yield Ok(processed),
                    Err(err) => yield Err(FunctionsError::InvalidResponse(err)),
                }
            }
        })
    }

    /// ストリーミングレスポンスを取得するメソッド
    pub async fn invoke_stream<B: Serialize>(
        &self,
        function_name: &str,
        body: Option<B>,
        options: Option<FunctionOptions>,
    ) -> Result<ByteStream> {
        let opts = options.unwrap_or_else(|| FunctionOptions {
            response_type: ResponseType::Stream,
            ..Default::default()
        });

        // URLの構築
        let mut url = Url::parse(&self.base_url)?;
        url.path_segments_mut()
            .map_err(|_| FunctionsError::UrlError(url::ParseError::EmptyHost))?
            .push("functions")
            .push("v1")
            .push(function_name);

        // リクエストの構築
        let mut request_builder = self
            .http_client
            .post(url)
            .header("apikey", &self.api_key)
            .header("Authorization", format!("Bearer {}", &self.api_key));

        // リクエストタイムアウトの設定
        if let Some(timeout) = opts.timeout_seconds {
            request_builder = request_builder.timeout(Duration::from_secs(timeout));
        }

        // コンテンツタイプの設定
        if let Some(content_type) = opts.content_type {
            request_builder = request_builder.header("Content-Type", content_type);
        } else {
            // デフォルトはJSON
            request_builder = request_builder.header("Content-Type", "application/json");
        }

        // カスタムヘッダーの追加
        if let Some(headers) = opts.headers {
            for (key, value) in headers {
                request_builder = request_builder.header(key, value);
            }
        }

        // リクエストボディの追加
        if let Some(body_data) = body {
            request_builder = request_builder.json(&body_data);
        }

        // リクエストの送信
        let response = request_builder.send().await.map_err(|e| {
            if e.is_timeout() {
                FunctionsError::TimeoutError
            } else {
                FunctionsError::from(e)
            }
        })?;

        // ステータスコードの確認
        let status = response.status();
        if !status.is_success() {
            // ステータスコードのコピーを保持
            let status_copy = status;

            // エラーレスポンスのパース
            let error_body = response
                .text()
                .await
                .unwrap_or_else(|_| "Failed to read error response".to_string());

            if let Ok(error_details) = serde_json::from_str::<FunctionErrorDetails>(&error_body) {
                return Err(FunctionsError::FunctionError {
                    message: error_details.message.as_ref().map_or_else(
                        || format!("Function returned error status: {status_copy}"),
                        |msg| msg.clone(),
                    ),
                    status: status_copy,
                    details: Some(error_details),
                });
            } else {
                return Err(FunctionsError::FunctionError {
                    message: error_body,
                    status: status_copy,
                    details: None,
                });
            }
        }

        // ストリームを返す
        Ok(Box::pin(
            response
                .bytes_stream()
                .map(|result| result.map_err(FunctionsError::from)),
        ))
    }

    /// JSONストリームを取得するメソッド（SSE形式のJSONイベントを扱う）
    pub async fn invoke_json_stream<B: Serialize>(
        &self,
        function_name: &str,
        body: Option<B>,
        options: Option<FunctionOptions>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Value>> + Send + '_>>> {
        let byte_stream = self.invoke_stream(function_name, body, options).await?;
        let json_stream = self.byte_stream_to_json(byte_stream);
        Ok(json_stream)
    }

    /// バイトストリームをJSONストリームに変換する
    fn byte_stream_to_json(
        &self,
        stream: ByteStream,
    ) -> Pin<Box<dyn Stream<Item = Result<Value>> + Send + '_>> {
        Box::pin(async_stream::stream! {
            let mut line_stream = self.stream_to_lines(stream);

            while let Some(line_result) = line_stream.next().await {
                match line_result {
                    Ok(line) => {
                        // 空行はスキップ
                        if line.trim().is_empty() {
                            continue;
                        }

                        // JSON解析を試みる
                        match serde_json::from_str::<Value>(&line) {
                            Ok(json_value) => {
                                yield Ok(json_value);
                            },
                            Err(err) => {
                                yield Err(FunctionsError::JsonError(err));
                            }
                        }
                    },
                    Err(err) => {
                        yield Err(err);
                        break;
                    }
                }
            }
        })
    }

    /// ストリームを行に変換する
    pub fn stream_to_lines(
        &self,
        stream: ByteStream,
    ) -> Pin<Box<dyn Stream<Item = Result<String>> + Send + '_>> {
        Box::pin(async_stream::stream! {
            let mut buf = BytesMut::new();

            // 行ごとに処理
            tokio::pin!(stream);
            while let Some(chunk_result) = stream.next().await {
                match chunk_result {
                    Ok(chunk) => {
                        buf.extend_from_slice(&chunk);

                        // バッファから完全な行を探して処理
                        while let Some(i) = buf.iter().position(|&b| b == b'\n') {
                            let line = if i > 0 && buf[i - 1] == b'\r' {
                                // CRLF改行の処理
                                let line = String::from_utf8_lossy(&buf[..i - 1]).to_string();
                                unsafe { buf.advance_mut(i + 1); }
                                line
                            } else {
                                // LF改行の処理
                                let line = String::from_utf8_lossy(&buf[..i]).to_string();
                                unsafe { buf.advance_mut(i + 1); }
                                line
                            };

                            yield Ok(line);
                        }
                    },
                    Err(e) => {
                        yield Err(e);
                        break;
                    }
                }
            }

            // 最後の行が改行で終わっていない場合も処理
            if !buf.is_empty() {
                let line = String::from_utf8_lossy(&buf).to_string();
                yield Ok(line);
            }
        })
    }

    /// 関数リクエストを作成する
    pub fn create_request<T: DeserializeOwned>(
        &self,
        function_name: &str,
    ) -> FunctionRequest<'_, T> {
        FunctionRequest {
            client: self,
            function_name: function_name.to_string(),
            _response_type: std::marker::PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*; // Import necessary items from parent module
    use serde_json::json;
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // Helper struct for testing
    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestPayload {
        message: String,
    }

    #[tokio::test]
    async fn test_invoke() {
        // TODO: モック実装を用いたテスト
    }

    // Test successful JSON invocation
    #[tokio::test]
    async fn test_invoke_json_success() {
        // Arrange: Start mock server
        let server = MockServer::start().await;
        let mock_uri = server.uri();
        let api_key = "test-key";
        let function_name = "hello-world";

        // Arrange: Prepare request and expected response
        let request_body = json!({ "name": "Rust" });
        let expected_response = TestPayload {
            message: "Hello Rust".to_string(),
        };

        // Arrange: Mock the API endpoint
        Mock::given(method("POST"))
            .and(path(format!("/functions/v1/{function_name}")))
            .and(header("apikey", api_key))
            .and(header(
                "Authorization",
                format!("Bearer {api_key}").as_str(),
            ))
            .and(header("Content-Type", "application/json"))
            .and(body_json(&request_body))
            .respond_with(ResponseTemplate::new(200).set_body_json(&expected_response))
            .mount(&server)
            .await;

        // Act: Create client and invoke function
        let client = FunctionsClient::new(&mock_uri, api_key, reqwest::Client::new());
        let result = client
            .invoke_json::<TestPayload, Value>(function_name, Some(request_body))
            .await;

        // Assert: Check the result
        assert!(result.is_ok());
        let data = result.unwrap();
        assert_eq!(data, expected_response);
        server.verify().await;
    }

    // Test error response with details
    #[tokio::test]
    async fn test_invoke_json_error_with_details() {
        // Arrange: Start mock server
        let server = MockServer::start().await;
        let mock_uri = server.uri();
        let api_key = "test-key";
        let function_name = "error-func";

        // Arrange: Prepare request and error response
        let request_body = json!({ "input": "invalid" });
        let error_response_body = json!({
            "message": "Something went wrong!",
            "code": "FUNC_ERROR",
            "details": { "reason": "Internal failure" }
        });

        // Arrange: Mock the API endpoint to return an error
        Mock::given(method("POST"))
            .and(path(format!("/functions/v1/{function_name}")))
            .and(header("apikey", api_key))
            .and(header(
                "Authorization",
                format!("Bearer {api_key}").as_str(),
            ))
            .and(body_json(&request_body))
            .respond_with(
                ResponseTemplate::new(500)
                    .set_body_json(&error_response_body)
                    .insert_header("Content-Type", "application/json"),
            )
            .mount(&server)
            .await;

        // Act: Create client and invoke function
        let client = FunctionsClient::new(&mock_uri, api_key, reqwest::Client::new());
        // Use a placeholder type like Value for the expected success type T,
        // as we expect an error anyway.
        let result = client
            .invoke_json::<Value, Value>(function_name, Some(request_body))
            .await;

        // Assert: Check the error result
        assert!(result.is_err());
        match result.err().unwrap() {
            FunctionsError::FunctionError {
                message,
                status,
                details,
            } => {
                assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
                assert_eq!(message, "Something went wrong!");
                assert!(details.is_some());
                let details_unwrapped = details.unwrap();
                assert_eq!(
                    details_unwrapped.message,
                    Some("Something went wrong!".to_string())
                );
                assert_eq!(details_unwrapped.code, Some("FUNC_ERROR".to_string()));
                assert!(details_unwrapped.details.is_some());
                assert_eq!(
                    details_unwrapped.details.unwrap(),
                    json!({ "reason": "Internal failure" })
                );
            }
            _ => panic!("Expected FunctionError, got different error type"),
        }
        server.verify().await;
    }

    // Test successful text invocation
    #[tokio::test]
    async fn test_invoke_text_success() {
        // Arrange: Start mock server
        let server = MockServer::start().await;
        let mock_uri = server.uri();
        let api_key = "test-key";
        let function_name = "plain-text-func";

        // Arrange: Prepare request and expected response
        let request_body = json!({ "format": "text" });
        let expected_response_text = "This is a plain text response.";

        // Arrange: Mock the API endpoint
        Mock::given(method("POST"))
            .and(path(format!("/functions/v1/{function_name}")))
            .and(header("apikey", api_key))
            .and(header(
                "Authorization",
                format!("Bearer {api_key}").as_str(),
            ))
            .and(header("Content-Type", "application/json")) // Default for invoke_text wrapper
            .and(body_json(&request_body))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(expected_response_text)
                    .insert_header("Content-Type", "text/plain"), // Server responds with text
            )
            .mount(&server)
            .await;

        // Act: Create client and invoke function
        let client = FunctionsClient::new(&mock_uri, api_key, reqwest::Client::new());
        let result = client
            .invoke_text::<Value>(function_name, Some(request_body)) // Body type is generic
            .await;

        // Assert: Check the result
        assert!(result.is_ok());
        let data = result.unwrap();
        assert_eq!(data, expected_response_text);
        server.verify().await;
    }
}
