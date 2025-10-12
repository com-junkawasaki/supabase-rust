//! Supabase Storage client for Rust
//!
//! This crate provides storage functionality for Supabase,
//! allowing for uploading, downloading, and managing files.

use bytes::Bytes;
use reqwest::multipart::{Form, Part};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::Path;
use thiserror::Error;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use url::Url;

/// 結果型
pub type Result<T> = std::result::Result<T, StorageError>;

/// エラー型
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("API error: {0}")]
    ApiError(String),

    #[error("Network error: {0}")]
    NetworkError(#[from] reqwest::Error),

    #[error("JSON serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("URL parse error: {0}")]
    UrlParseError(#[from] url::ParseError),

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("File not found: {0}")]
    FileNotFound(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Request error: {0}")]
    RequestError(String),

    #[error("Deserialization error: {0}")]
    DeserializationError(String),
}

impl StorageError {
    pub fn new(message: String) -> Self {
        Self::StorageError(message)
    }
}

/// ファイルアップロードオプション
#[derive(Debug, Clone, Serialize, Default)]
pub struct FileOptions {
    pub cache_control: Option<String>,
    pub content_type: Option<String>,
    pub upsert: Option<bool>,
}

impl FileOptions {
    /// 新しいファイルオプションを作成
    pub fn new() -> Self {
        Self::default()
    }

    /// キャッシュコントロールを設定
    pub fn with_cache_control(mut self, cache_control: &str) -> Self {
        self.cache_control = Some(cache_control.to_string());
        self
    }

    /// コンテンツタイプを設定
    pub fn with_content_type(mut self, content_type: &str) -> Self {
        self.content_type = Some(content_type.to_string());
        self
    }

    /// アップサートを設定
    pub fn with_upsert(mut self, upsert: bool) -> Self {
        self.upsert = Some(upsert);
        self
    }
}

/// ファイル一覧取得オプション
#[derive(Debug, Clone, Serialize, Default)]
pub struct ListOptions {
    pub limit: Option<i32>,
    pub offset: Option<i32>,
    pub sort_by: Option<SortBy>,
    pub search: Option<String>,
}

impl ListOptions {
    /// 新しい一覧オプションを作成
    pub fn new() -> Self {
        Self::default()
    }

    /// 取得上限を設定
    pub fn limit(mut self, limit: i32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// オフセットを設定
    pub fn offset(mut self, offset: i32) -> Self {
        self.offset = Some(offset);
        self
    }

    /// ソート順を設定
    pub fn sort_by(mut self, column: &str, order: SortOrder) -> Self {
        self.sort_by = Some(SortBy {
            column: column.to_string(),
            order,
        });
        self
    }

    /// 検索キーワードを設定
    pub fn search(mut self, search: &str) -> Self {
        self.search = Some(search.to_string());
        self
    }
}

/// ソート設定
#[derive(Debug, Clone, Serialize)]
pub struct SortBy {
    pub column: String,
    pub order: SortOrder,
}

impl std::fmt::Display for SortBy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{:?}", self.column, self.order).map(|_| ())
    }
}

/// ソート順
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    Asc,
    Desc,
}

/// 画像変換オプション
#[derive(Debug, Clone, Serialize, Default)]
pub struct ImageTransformOptions {
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub resize: Option<String>,
    pub format: Option<String>,
    pub quality: Option<u32>,
}

impl ImageTransformOptions {
    /// 新しい画像変換オプションを作成
    pub fn new() -> Self {
        Self::default()
    }

    /// 幅を設定
    pub fn with_width(mut self, width: u32) -> Self {
        self.width = Some(width);
        self
    }

    /// 高さを設定
    pub fn with_height(mut self, height: u32) -> Self {
        self.height = Some(height);
        self
    }

    /// リサイズモードを設定 (cover, contain, fill)
    pub fn with_resize(mut self, resize: &str) -> Self {
        self.resize = Some(resize.to_string());
        self
    }

    /// 出力フォーマットを設定 (webp, png, jpeg, etc)
    pub fn with_format(mut self, format: &str) -> Self {
        self.format = Some(format.to_string());
        self
    }

    /// 画質を設定 (1-100)
    pub fn with_quality(mut self, quality: u32) -> Self {
        self.quality = Some(quality.min(100));
        self
    }

    /// URLクエリパラメータに変換 (No leading '?')
    fn to_query_params(&self) -> String {
        let mut params = Vec::new();

        if let Some(width) = self.width {
            params.push(format!("width={width}"));
        }

        if let Some(height) = self.height {
            params.push(format!("height={height}"));
        }

        if let Some(resize) = &self.resize {
            params.push(format!("resize={resize}"));
        }

        if let Some(format) = &self.format {
            params.push(format!("format={format}"));
        }

        if let Some(quality) = self.quality {
            params.push(format!("quality={quality}"));
        }

        params.join("&")
    }
}

/// ファイル情報
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileObject {
    pub name: String,
    pub bucket_id: String,
    pub owner: String,
    pub id: String,
    pub updated_at: String,
    pub created_at: String,
    pub last_accessed_at: String,
    pub metadata: Option<serde_json::Value>,
    pub mime_type: Option<String>,
    pub size: i64,
}

/// バケット情報
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bucket {
    pub id: String,
    pub name: String,
    pub owner: String,
    pub public: bool,
    pub created_at: String,
    pub updated_at: String,
}

/// チャンクアップロードの初期化結果
#[derive(Debug, Clone, Deserialize)]
pub struct InitiateMultipartUploadResponse {
    pub id: String,
    #[serde(rename = "uploadId")]
    pub upload_id: String,
    pub key: String,
    pub bucket: String,
}

/// アップロードされたチャンク情報
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadedPartInfo {
    #[serde(rename = "partNumber")]
    pub part_number: u32,
    pub etag: String,
}

/// チャンクアップロードの完了リクエスト
#[derive(Debug, Clone, Serialize)]
struct CompleteMultipartUploadRequest {
    #[serde(rename = "uploadId")]
    pub upload_id: String,
    pub parts: Vec<UploadedPartInfo>,
}

/// ストレージバケットクライアント
pub struct StorageBucketClient<'a> {
    parent: &'a StorageClient,
    bucket_id: String,
}

/// ストレージクライアント
pub struct StorageClient {
    base_url: String,
    api_key: String,
    http_client: Client,
}

impl StorageClient {
    /// 新しいストレージクライアントを作成
    pub fn new(base_url: &str, api_key: &str, http_client: Client) -> Self {
        Self {
            base_url: base_url.to_string(),
            api_key: api_key.to_string(),
            http_client,
        }
    }

    /// バケットを指定
    pub fn from<'a>(&'a self, bucket_id: &str) -> StorageBucketClient<'a> {
        StorageBucketClient {
            parent: self,
            bucket_id: bucket_id.to_string(),
        }
    }

    /// バケット一覧を取得
    pub async fn list_buckets(&self) -> Result<Vec<Bucket>> {
        let url = format!("{}/storage/v1/bucket", self.base_url);

        let response = self
            .http_client
            .get(&url)
            .header("apikey", &self.api_key)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(StorageError::ApiError(error_text));
        }

        let buckets = response.json::<Vec<Bucket>>().await?;

        Ok(buckets)
    }

    /// バケットを作成
    pub async fn create_bucket(&self, bucket_id: &str, is_public: bool) -> Result<Bucket> {
        let url = format!("{}/storage/v1/bucket", self.base_url);

        let payload = serde_json::json!({
            "id": bucket_id,
            "name": bucket_id,
            "public": is_public
        });

        let response = self
            .http_client
            .post(&url)
            .header("apikey", &self.api_key)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(StorageError::ApiError(error_text));
        }

        let bucket = response.json::<Bucket>().await?;

        Ok(bucket)
    }

    /// バケットを削除
    pub async fn delete_bucket(&self, bucket_id: &str) -> Result<()> {
        let url = format!("{}/storage/v1/bucket/{}", self.base_url, bucket_id);

        let response = self
            .http_client
            .delete(&url)
            .header("apikey", &self.api_key)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(StorageError::ApiError(error_text));
        }

        Ok(())
    }

    /// バケット情報を更新
    pub async fn update_bucket(&self, bucket_id: &str, is_public: bool) -> Result<Bucket> {
        let url = format!("{}/storage/v1/bucket/{}", self.base_url, bucket_id);

        let payload = serde_json::json!({
            "id": bucket_id,
            "public": is_public
        });

        let response = self
            .http_client
            .put(&url)
            .header("apikey", &self.api_key)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(StorageError::ApiError(error_text));
        }

        let bucket = response.json::<Bucket>().await?;

        Ok(bucket)
    }
}

impl<'a> StorageBucketClient<'a> {
    /// ファイルをアップロード
    pub async fn upload(
        &self,
        path: &str,
        file_path: &Path,
        options: Option<FileOptions>,
    ) -> Result<FileObject> {
        let mut url = Url::parse(&self.parent.base_url)?;
        url.set_path(&format!("/storage/v1/object/{}/{}", self.bucket_id, path));

        // オプションをURLクエリとして設定
        if let Some(opts) = &options {
            let mut query_pairs = url.query_pairs_mut();
            if let Some(cache_control) = &opts.cache_control {
                query_pairs.append_pair("cache_control", cache_control);
            }
            if let Some(upsert) = &opts.upsert {
                query_pairs.append_pair("upsert", &upsert.to_string());
            }
        }

        // ファイルの内容を読み込む
        let mut file = File::open(file_path).await?;
        let mut contents = Vec::new();
        file.read_to_end(&mut contents).await?;

        // マルチパートフォームデータの作成
        let part = Part::bytes(contents)
            .file_name(file_path.file_name().unwrap().to_string_lossy().to_string());

        let form = Form::new().part("file", part);

        let response = self
            .parent
            .http_client
            .post(url)
            .header("apikey", &self.parent.api_key)
            .header("Authorization", format!("Bearer {}", &self.parent.api_key))
            .multipart(form)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(StorageError::ApiError(error_text));
        }

        let file_object = response.json::<FileObject>().await?;

        Ok(file_object)
    }

    /// ファイルをダウンロード
    pub async fn download(&self, path: &str) -> Result<Bytes> {
        let mut url = Url::parse(&self.parent.base_url)?;
        url.set_path(&format!("/storage/v1/object/{}/{}", self.bucket_id, path));

        let response = self
            .parent
            .http_client
            .get(url)
            .header("apikey", &self.parent.api_key)
            .header("Authorization", format!("Bearer {}", &self.parent.api_key))
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(StorageError::ApiError(error_text));
        }

        let bytes = response.bytes().await?;

        Ok(bytes)
    }

    /// ファイル一覧を取得
    pub async fn list(
        &self,
        prefix: &str,
        options: Option<ListOptions>,
    ) -> Result<Vec<FileObject>> {
        let mut url = Url::parse(&self.parent.base_url)?;
        url.set_path(&format!("/storage/v1/object/list/{}", self.bucket_id));

        // プレフィックスと検索オプションをクエリとして設定
        {
            let mut query_pairs = url.query_pairs_mut();
            query_pairs.append_pair("prefix", prefix);

            if let Some(opts) = &options {
                if let Some(limit) = opts.limit {
                    query_pairs.append_pair("limit", &limit.to_string());
                }
                if let Some(offset) = opts.offset {
                    query_pairs.append_pair("offset", &offset.to_string());
                }
                if let Some(sort_by) = &opts.sort_by {
                    query_pairs.append_pair("sortBy", &sort_by.to_string());
                }
                if let Some(search) = &opts.search {
                    query_pairs.append_pair("search", search);
                }
            }
        } // query_pairsのスコープはここで終了

        let response = self
            .parent
            .http_client
            .get(url)
            .header("apikey", &self.parent.api_key)
            .header("Authorization", format!("Bearer {}", &self.parent.api_key))
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(StorageError::ApiError(error_text));
        }

        let files = response.json::<Vec<FileObject>>().await?;

        Ok(files)
    }

    /// ファイルを削除
    pub async fn remove(&self, paths: Vec<&str>) -> Result<()> {
        let url = format!(
            "{}/storage/v1/object/{}",
            self.parent.base_url, self.bucket_id
        );

        let payload = serde_json::json!({
            "prefixes": paths
        });

        let response = self
            .parent
            .http_client
            .delete(&url)
            .header("apikey", &self.parent.api_key)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(StorageError::ApiError(error_text));
        }

        Ok(())
    }

    /// 公開URLを取得
    pub fn get_public_url(&self, path: &str) -> String {
        format!(
            "{}/storage/v1/object/public/{}/{}",
            self.parent.base_url, self.bucket_id, path
        )
    }

    /// 署名付きURLを作成
    pub async fn create_signed_url(&self, path: &str, expires_in: i32) -> Result<String> {
        let url = format!(
            "{}/storage/v1/object/sign/{}/{}",
            self.parent.base_url, self.bucket_id, path
        );

        let payload = serde_json::json!({
            "expiresIn": expires_in
        });

        let response = self
            .parent
            .http_client
            .post(&url)
            .header("apikey", &self.parent.api_key)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(StorageError::ApiError(error_text));
        }

        #[derive(Deserialize)]
        struct SignedUrlResponse {
            signed_url: String,
        }

        let signed_url = response.json::<SignedUrlResponse>().await?;

        Ok(signed_url.signed_url)
    }

    /// マルチパートアップロードを初期化
    pub async fn initiate_multipart_upload(
        &self,
        path: &str,
        options: Option<FileOptions>,
    ) -> Result<InitiateMultipartUploadResponse> {
        let url = format!("{}/storage/v1/upload/initiate", self.parent.base_url);

        let options = options.unwrap_or_default();

        let cache_control = options
            .cache_control
            .unwrap_or_else(|| "max-age=3600".to_string());
        let content_type = options
            .content_type
            .unwrap_or_else(|| "application/octet-stream".to_string());
        let upsert = options.upsert.unwrap_or(false);

        let payload = serde_json::json!({
            "bucket": self.bucket_id,
            "name": path,
            "cacheControl": cache_control,
            "contentType": content_type,
            "upsert": upsert,
        });

        let response = self
            .parent
            .http_client
            .post(&url)
            .header("apikey", &self.parent.api_key)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(StorageError::ApiError(error_text));
        }

        let initiate_response: InitiateMultipartUploadResponse = response.json().await?;

        Ok(initiate_response)
    }

    /// チャンクをアップロード
    pub async fn upload_part(
        &self,
        upload_id: &str,
        part_number: u32,
        data: Bytes,
    ) -> Result<UploadedPartInfo> {
        let url = format!("{}/storage/v1/upload/part", self.parent.base_url);

        let body = reqwest::Body::from(data);

        let response = self
            .parent
            .http_client
            .post(&url)
            .header("apikey", &self.parent.api_key)
            .query(&[
                ("uploadId", upload_id),
                ("partNumber", &part_number.to_string()),
                ("bucket", &self.bucket_id),
            ])
            .body(body)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(StorageError::ApiError(error_text));
        }

        let etag = response
            .headers()
            .get("etag")
            .ok_or_else(|| StorageError::new("ETag header not found in response".to_string()))?
            .to_str()
            .map_err(|e| StorageError::new(format!("Invalid ETag header: {e}")))?
            .to_string();

        let part_info = UploadedPartInfo { part_number, etag };

        Ok(part_info)
    }

    /// マルチパートアップロードを完了
    pub async fn complete_multipart_upload(
        &self,
        upload_id: &str,
        path: &str,
        parts: Vec<UploadedPartInfo>,
    ) -> Result<FileObject> {
        let url = format!("{}/storage/v1/upload/complete", self.parent.base_url);

        let payload = CompleteMultipartUploadRequest {
            upload_id: upload_id.to_string(),
            parts,
        };

        let response = self
            .parent
            .http_client
            .post(&url)
            .header("apikey", &self.parent.api_key)
            .header("Content-Type", "application/json")
            .query(&[("bucket", &self.bucket_id), ("key", &path.to_string())])
            .json(&payload)
            .send()
            .await
            .map_err(StorageError::NetworkError)?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(StorageError::ApiError(error_text));
        }

        let file_object: FileObject = response.json().await?;

        Ok(file_object)
    }

    /// マルチパートアップロードを中止
    pub async fn abort_multipart_upload(&self, upload_id: &str, path: &str) -> Result<()> {
        let url = format!("{}/storage/v1/upload/abort", self.parent.base_url);

        let payload = serde_json::json!({
            "uploadId": upload_id,
            "bucket": self.bucket_id,
            "key": path,
        });

        let response = self
            .parent
            .http_client
            .post(&url)
            .header("apikey", &self.parent.api_key)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(StorageError::ApiError(error_text));
        }

        Ok(())
    }

    /// 大容量ファイルをチャンクでアップロード
    ///
    /// このメソッドは大きなファイルを自動的にチャンクに分割してアップロードします。
    /// 各チャンクは非同期でアップロードされ、すべてのチャンクがアップロードされると
    /// 自動的にマルチパートアップロードを完了します。
    pub async fn upload_large_file(
        &self,
        path: &str,
        file_path: &Path,
        chunk_size: usize,
        options: Option<FileOptions>,
    ) -> Result<FileObject> {
        // ファイルを開く
        let mut file = File::open(file_path).await?;

        // ファイルサイズを取得
        let file_size = file.metadata().await?.len() as usize;

        // チャンク数を計算
        let chunk_count = file_size.div_ceil(chunk_size);

        if chunk_count == 0 {
            return Err(StorageError::new("File is empty".to_string()));
        }

        // マルチパートアップロードを初期化
        let init_response = self.initiate_multipart_upload(path, options).await?;

        // 部分アップロード情報を保持するベクター
        let mut uploaded_parts = Vec::with_capacity(chunk_count);

        // バッファを準備
        let mut buffer = vec![0u8; chunk_size];

        // 各チャンクをアップロード
        for part_number in 1..=chunk_count as u32 {
            // バッファにデータを読み込む
            let n = file.read(&mut buffer).await?;

            if n == 0 {
                break;
            }

            // 実際に読み込んだサイズに合わせてバッファを調整
            let chunk_data = Bytes::from(buffer[0..n].to_vec());

            // チャンクをアップロード
            let part_info = self
                .upload_part(&init_response.upload_id, part_number, chunk_data)
                .await?;

            // アップロードした部分情報を保存
            uploaded_parts.push(part_info);
        }

        // マルチパートアップロードを完了
        let file_object = self
            .complete_multipart_upload(&init_response.upload_id, path, uploaded_parts)
            .await?;

        Ok(file_object)
    }

    /// 画像に変換を適用して取得する
    pub async fn transform_image(
        &self,
        path: &str,
        options: ImageTransformOptions,
    ) -> Result<Bytes> {
        let url = format!(
            "{}/object/transform/authenticated/{}/{}",
            self.parent.base_url, self.bucket_id, path
        );

        // クエリパラメータに変換オプションを追加
        let query_params = options.to_query_params();
        let request_url = if query_params.is_empty() {
            url
        } else {
            format!("{url}?{query_params}")
        };

        let res = self
            .parent
            .http_client
            .get(&request_url)
            .header("apikey", &self.parent.api_key)
            .header("Authorization", format!("Bearer {}", self.parent.api_key))
            .send()
            .await
            .map_err(StorageError::NetworkError)?;

        // ステータスコードを事前に取得
        let status = res.status();

        if !status.is_success() {
            let error_text = res
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(StorageError::ApiError(format!(
                "Failed to transform image: {error_text} (Status: {status})",
            )));
        }

        let bytes = res.bytes().await.map_err(StorageError::NetworkError)?;
        Ok(bytes)
    }

    /// 画像の公開変換URLを取得
    pub fn get_public_transform_url(&self, path: &str, options: ImageTransformOptions) -> String {
        let base_url = format!(
            "{}/object/public/{}/{}",
            self.parent.base_url, self.bucket_id, path
        );

        // クエリパラメータに変換オプションを追加
        let query_params = options.to_query_params();
        if query_params.is_empty() {
            base_url
        } else {
            format!("{base_url}?{query_params}")
        }
    }

    /// 画像の署名付き変換URLを作成
    pub async fn create_signed_transform_url(
        &self,
        path: &str,
        options: ImageTransformOptions,
        expires_in: i32,
    ) -> Result<String> {
        let url = format!(
            "{}/object/sign/{}/{}",
            self.parent.base_url, self.bucket_id, path
        );

        // クエリパラメータに変換オプションを追加
        let transform_params = options.to_query_params();

        let payload = json!({
            "expiresIn": expires_in,
            "transform": transform_params,
        });

        let res = self
            .parent
            .http_client
            .post(&url)
            .header("apikey", &self.parent.api_key)
            .header("Authorization", format!("Bearer {}", self.parent.api_key))
            .json(&payload)
            .send()
            .await
            .map_err(StorageError::NetworkError)?;

        // ステータスコードを事前に取得
        let status = res.status();

        if !status.is_success() {
            let error_text = res
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(StorageError::ApiError(format!(
                "Failed to create signed transform URL: {error_text} (Status: {status})",
            )));
        }

        #[derive(Debug, Deserialize)]
        struct SignedUrlResponse {
            signed_url: String,
        }

        let response = res
            .json::<SignedUrlResponse>()
            .await
            .map_err(|e| StorageError::DeserializationError(e.to_string()))?;

        Ok(response.signed_url)
    }

    /// S3互換クライアントを作成
    pub fn s3_compatible(&self, options: s3::S3Options) -> s3::S3BucketClient {
        s3::S3BucketClient::new(
            &self.parent.base_url,
            &self.parent.api_key,
            &self.bucket_id,
            self.parent.http_client.clone(),
            options,
        )
    }

    /// オブジェクトをバケット内で移動または名前変更します。
    ///
    /// # 引数
    ///
    /// * `source_path` - 移動元のオブジェクトのパス。
    /// * `destination_path` - 移動先のオブジェクトのパス。
    ///
    /// # 戻り値
    ///
    /// 移動が成功した場合は `Ok(())`、失敗した場合は `StorageError` を返します。
    pub async fn move_object(&self, source_path: &str, destination_path: &str) -> Result<()> {
        let url_str = format!("{}/object/move", self.parent.base_url);
        let url = Url::parse(&url_str).map_err(StorageError::UrlParseError)?;

        let body = json!({
            "bucketId": self.bucket_id,
            "sourceKey": source_path,
            "destinationKey": destination_path
        });

        let response = self
            .parent
            .http_client
            .post(url)
            .header("apikey", &self.parent.api_key)
            .header("Authorization", format!("Bearer {}", self.parent.api_key))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(StorageError::NetworkError)?;

        if response.status().is_success() {
            Ok(())
        } else {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Failed to read error body".to_string());
            // Try parsing Supabase error format
            let error_message =
                if let Ok(json_err) = serde_json::from_str::<serde_json::Value>(&error_text) {
                    json_err
                        .get("message")
                        .and_then(|v| v.as_str())
                        .unwrap_or(&error_text)
                        .to_string()
                } else {
                    error_text
                };
            Err(StorageError::ApiError(format!(
                "Failed to move object: {error_message} (Status: {status})",
            )))
        }
    }
}

// S3互換API用のモジュールを追加
pub mod s3 {
    use crate::{Result, StorageError};
    use bytes::Bytes;
    use reqwest::Client;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    /// S3互換APIのオプション
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct S3Options {
        /// アクセスキー
        pub access_key_id: String,
        /// シークレットキー
        pub secret_access_key: String,
        /// リージョン（デフォルトは「auto」）
        #[serde(skip_serializing_if = "Option::is_none")]
        pub region: Option<String>,
        /// エンドポイントURL（デフォルトはSupabaseのストレージURL）
        #[serde(skip_serializing_if = "Option::is_none")]
        pub endpoint: Option<String>,
        /// フォースパス形式を使用するかどうか
        #[serde(skip_serializing_if = "Option::is_none")]
        pub force_path_style: Option<bool>,
    }

    impl Default for S3Options {
        fn default() -> Self {
            Self {
                access_key_id: String::new(),
                secret_access_key: String::new(),
                region: Some("auto".to_string()),
                endpoint: None,
                force_path_style: Some(true),
            }
        }
    }

    /// S3 API互換クライアント
    pub struct S3Client {
        pub options: S3Options,
        pub base_url: String,
        pub api_key: String,
        pub http_client: Client,
    }

    impl S3Client {
        /// 新しいS3互換クライアントを作成
        pub fn new(base_url: &str, api_key: &str, http_client: Client, options: S3Options) -> Self {
            Self {
                options,
                base_url: base_url.to_string(),
                api_key: api_key.to_string(),
                http_client,
            }
        }

        /// バケットの作成
        pub async fn create_bucket(&self, bucket_name: &str, is_public: bool) -> Result<()> {
            let url = format!("{}/storage/v1/bucket", self.base_url);

            let payload = serde_json::json!({
                "name": bucket_name,
                "public": is_public,
                "file_size_limit": null,
                "allowed_mime_types": null
            });

            let response = self
                .http_client
                .post(&url)
                .header("apikey", &self.api_key)
                .header("Authorization", format!("Bearer {}", &self.api_key))
                .json(&payload)
                .send()
                .await
                .map_err(|e| StorageError::RequestError(e.to_string()))?;

            if !response.status().is_success() {
                let error_text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unknown error".to_string());
                return Err(StorageError::ApiError(error_text));
            }

            Ok(())
        }

        /// バケットの削除
        pub async fn delete_bucket(&self, bucket_name: &str) -> Result<()> {
            let url = format!("{}/storage/v1/bucket/{}", self.base_url, bucket_name);

            let response = self
                .http_client
                .delete(&url)
                .header("apikey", &self.api_key)
                .header("Authorization", format!("Bearer {}", &self.api_key))
                .send()
                .await
                .map_err(|e| StorageError::RequestError(e.to_string()))?;

            if !response.status().is_success() {
                let error_text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unknown error".to_string());
                return Err(StorageError::ApiError(error_text));
            }

            Ok(())
        }

        /// バケットの一覧を取得
        pub async fn list_buckets(&self) -> Result<Vec<serde_json::Value>> {
            let url = format!("{}/storage/v1/bucket", self.base_url);

            let response = self
                .http_client
                .get(&url)
                .header("apikey", &self.api_key)
                .header("Authorization", format!("Bearer {}", &self.api_key))
                .send()
                .await
                .map_err(|e| StorageError::RequestError(e.to_string()))?;

            if !response.status().is_success() {
                let error_text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unknown error".to_string());
                return Err(StorageError::ApiError(error_text));
            }

            let buckets = response
                .json::<Vec<serde_json::Value>>()
                .await
                .map_err(|e| StorageError::DeserializationError(e.to_string()))?;

            Ok(buckets)
        }

        /// バケットを取得し、S3互換操作のためのクライアントを返す
        pub fn bucket(&self, bucket_name: &str) -> S3BucketClient {
            S3BucketClient::new(
                &self.base_url,
                &self.api_key,
                bucket_name,
                self.http_client.clone(),
                self.options.clone(),
            )
        }
    }

    /// S3バケット操作用クライアント
    pub struct S3BucketClient {
        pub base_url: String,
        pub api_key: String,
        pub bucket_name: String,
        pub http_client: Client,
        pub options: S3Options,
    }

    impl S3BucketClient {
        /// 新しいS3バケットクライアントを作成
        pub fn new(
            base_url: &str,
            api_key: &str,
            bucket_name: &str,
            http_client: Client,
            options: S3Options,
        ) -> Self {
            Self {
                base_url: base_url.to_string(),
                api_key: api_key.to_string(),
                bucket_name: bucket_name.to_string(),
                http_client,
                options,
            }
        }

        /// オブジェクトをアップロード（S3互換API）
        pub async fn put_object(
            &self,
            path: &str,
            data: Bytes,
            content_type: Option<String>,
            metadata: Option<HashMap<String, String>>,
        ) -> Result<()> {
            let url = format!(
                "{}/storage/v1/object/{}/{}",
                self.base_url,
                self.bucket_name,
                path.trim_start_matches('/')
            );

            let content_type =
                content_type.unwrap_or_else(|| "application/octet-stream".to_string());

            let mut request = self
                .http_client
                .put(&url)
                .header("apikey", &self.api_key)
                .header("Authorization", format!("Bearer {}", &self.api_key))
                .header("Content-Type", content_type)
                .body(data);

            // メタデータがある場合は追加
            if let Some(metadata) = metadata {
                for (key, value) in metadata {
                    request = request.header(&format!("x-amz-meta-{key}"), value);
                }
            }

            let response = request
                .send()
                .await
                .map_err(|e| StorageError::RequestError(e.to_string()))?;

            if !response.status().is_success() {
                let error_text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unknown error".to_string());
                return Err(StorageError::ApiError(error_text));
            }

            Ok(())
        }

        /// オブジェクトをダウンロード（S3互換API）
        pub async fn get_object(&self, path: &str) -> Result<Bytes> {
            let url = format!(
                "{}/storage/v1/object/{}/{}",
                self.base_url,
                self.bucket_name,
                path.trim_start_matches('/')
            );

            let response = self
                .http_client
                .get(&url)
                .header("apikey", &self.api_key)
                .header("Authorization", format!("Bearer {}", &self.api_key))
                .send()
                .await
                .map_err(|e| StorageError::RequestError(e.to_string()))?;

            if !response.status().is_success() {
                let error_text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unknown error".to_string());
                return Err(StorageError::ApiError(error_text));
            }

            let data = response
                .bytes()
                .await
                .map_err(|e| StorageError::RequestError(e.to_string()))?;

            Ok(data)
        }

        /// オブジェクトのメタデータを取得（S3互換API）
        pub async fn head_object(&self, path: &str) -> Result<HashMap<String, String>> {
            let url = format!(
                "{}/storage/v1/object/{}/{}",
                self.base_url,
                self.bucket_name,
                path.trim_start_matches('/')
            );

            let response = self
                .http_client
                .head(&url)
                .header("apikey", &self.api_key)
                .header("Authorization", format!("Bearer {}", &self.api_key))
                .send()
                .await
                .map_err(|e| StorageError::RequestError(e.to_string()))?;

            if !response.status().is_success() {
                return Err(StorageError::ApiError("Object not found".to_string()));
            }

            let mut metadata = HashMap::new();

            // レスポンスヘッダーからメタデータを抽出
            for (key, value) in response.headers() {
                let key_str = key.to_string();
                if key_str.starts_with("x-amz-meta-") {
                    let meta_key = key_str.trim_start_matches("x-amz-meta-").to_string();
                    metadata.insert(meta_key, value.to_str().unwrap_or_default().to_string());
                }
            }

            Ok(metadata)
        }

        /// オブジェクトを削除（S3互換API）
        pub async fn delete_object(&self, path: &str) -> Result<()> {
            let url = format!(
                "{}/storage/v1/object/{}/{}",
                self.base_url,
                self.bucket_name,
                path.trim_start_matches('/')
            );

            let response = self
                .http_client
                .delete(&url)
                .header("apikey", &self.api_key)
                .header("Authorization", format!("Bearer {}", &self.api_key))
                .send()
                .await
                .map_err(|e| StorageError::RequestError(e.to_string()))?;

            if !response.status().is_success() {
                let error_text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unknown error".to_string());
                return Err(StorageError::ApiError(error_text));
            }

            Ok(())
        }

        /// オブジェクトの一覧を取得（S3互換API）
        pub async fn list_objects(
            &self,
            prefix: Option<&str>,
            delimiter: Option<&str>,
            max_keys: Option<i32>,
        ) -> Result<serde_json::Value> {
            let mut url = format!(
                "{}/storage/v1/object/list/{}",
                self.base_url, self.bucket_name
            );

            // クエリパラメータを追加
            let mut query_params = Vec::new();

            if let Some(prefix) = prefix {
                query_params.push(format!("prefix={prefix}"));
            }

            if let Some(delimiter) = delimiter {
                query_params.push(format!("delimiter={delimiter}"));
            }

            if let Some(max_keys) = max_keys {
                query_params.push(format!("max-keys={max_keys}"));
            }

            if !query_params.is_empty() {
                url = format!("{}?{}", url, query_params.join("&"));
            }

            let response = self
                .http_client
                .get(&url)
                .header("apikey", &self.api_key)
                .header("Authorization", format!("Bearer {}", &self.api_key))
                .send()
                .await
                .map_err(|e| StorageError::RequestError(e.to_string()))?;

            if !response.status().is_success() {
                let error_text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unknown error".to_string());
                return Err(StorageError::ApiError(error_text));
            }

            let objects = response
                .json::<serde_json::Value>()
                .await
                .map_err(|e| StorageError::DeserializationError(e.to_string()))?;

            Ok(objects)
        }

        /// オブジェクトのコピー（S3互換API）
        pub async fn copy_object(&self, source_path: &str, destination_path: &str) -> Result<()> {
            let url = format!("{}/storage/v1/object/copy", self.base_url);

            let payload = serde_json::json!({
                "bucketId": self.bucket_name,
                "sourceKey": source_path,
                "destinationKey": destination_path
            });

            let response = self
                .http_client
                .post(&url)
                .header("apikey", &self.api_key)
                .header("Authorization", format!("Bearer {}", &self.api_key))
                .json(&payload)
                .send()
                .await
                .map_err(|e| StorageError::RequestError(e.to_string()))?;

            if !response.status().is_success() {
                let error_text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unknown error".to_string());
                return Err(StorageError::ApiError(error_text));
            }

            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_list_buckets() {
        // モックサーバーを起動
        let mock_server = MockServer::start().await;

        // --- 成功ケースのモック ---
        let buckets_response = json!([
            {
                "id": "bucket1",
                "name": "bucket1",
                "owner": "owner-uuid",
                "public": false,
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z"
            },
            {
                "id": "bucket2",
                "name": "bucket2",
                "owner": "owner-uuid",
                "public": true,
                "created_at": "2024-01-02T00:00:00Z",
                "updated_at": "2024-01-02T00:00:00Z"
            }
        ]);
        Mock::given(method("GET"))
            .and(path("/storage/v1/bucket"))
            .respond_with(ResponseTemplate::new(200).set_body_json(buckets_response.clone()))
            .mount(&mock_server)
            .await;

        // クライアントを作成
        let http_client = reqwest::Client::new();
        let storage_client =
            StorageClient::new(&mock_server.uri(), "fake-key", http_client.clone());

        // list_buckets を呼び出し、成功することを確認
        let result = storage_client.list_buckets().await;
        assert!(result.is_ok());
        let buckets = result.unwrap();
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].id, "bucket1");
        assert!(buckets[1].public);

        // モックをリセット
        mock_server.reset().await;

        // --- エラーケースのモック (例: 401 Unauthorized) ---
        let error_response = json!({ "message": "Unauthorized" });
        Mock::given(method("GET"))
            .and(path("/storage/v1/bucket"))
            .respond_with(ResponseTemplate::new(401).set_body_json(error_response))
            .mount(&mock_server)
            .await;

        // list_buckets を呼び出し、エラーになることを確認
        let result = storage_client.list_buckets().await;
        assert!(result.is_err());
        if let Err(StorageError::ApiError(msg)) = result {
            assert!(msg.contains("Unauthorized"));
        } else {
            panic!("Expected ApiError, got {result:?}");
        }
    }

    #[tokio::test]
    async fn test_create_bucket() {
        // モックサーバーを起動
        let mock_server = MockServer::start().await;

        // --- 成功ケースのモック ---
        let bucket_id = "new-bucket";
        let request_body = json!({ "id": bucket_id, "name": bucket_id, "public": true });
        let response_body = json!({
            "id": bucket_id,
            "name": bucket_id,
            "owner": "owner-uuid",
            "public": true,
            "created_at": "2024-01-03T00:00:00Z",
            "updated_at": "2024-01-03T00:00:00Z"
        });

        Mock::given(method("POST"))
            .and(path("/storage/v1/bucket"))
            .and(wiremock::matchers::body_json(request_body.clone())) // リクエストボディを検証
            .respond_with(ResponseTemplate::new(200).set_body_json(response_body.clone()))
            .mount(&mock_server)
            .await;

        // クライアントを作成
        let http_client = reqwest::Client::new();
        let storage_client =
            StorageClient::new(&mock_server.uri(), "fake-key", http_client.clone());

        // create_bucket を呼び出し、成功することを確認
        let result = storage_client.create_bucket(bucket_id, true).await;
        assert!(result.is_ok(), "create_bucket failed: {:?}", result.err());
        let bucket = result.unwrap();
        assert_eq!(bucket.id, bucket_id);
        assert_eq!(bucket.name, bucket_id);
        assert!(bucket.public);

        // モックをリセット
        mock_server.reset().await;

        // --- エラーケースのモック (例: 409 Conflict) ---
        let error_response = json!({ "message": "Bucket already exists" });
        Mock::given(method("POST"))
            .and(path("/storage/v1/bucket"))
            .and(wiremock::matchers::body_json(request_body.clone()))
            .respond_with(ResponseTemplate::new(409).set_body_json(error_response))
            .mount(&mock_server)
            .await;

        // create_bucket を呼び出し、エラーになることを確認
        let result = storage_client.create_bucket(bucket_id, true).await;
        assert!(result.is_err());
        if let Err(StorageError::ApiError(msg)) = result {
            assert!(msg.contains("Bucket already exists"));
        } else {
            panic!("Expected ApiError, got {result:?}");
        }
    }

    #[tokio::test]
    async fn test_delete_bucket() {
        // モックサーバーを起動
        let mock_server = MockServer::start().await;
        let bucket_id = "bucket-to-delete";

        // --- 成功ケースのモック ---
        Mock::given(method("DELETE"))
            .and(path(format!("/storage/v1/bucket/{bucket_id}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({ "message": "Successfully deleted" })),
            )
            .mount(&mock_server)
            .await;

        // クライアントを作成
        let http_client = reqwest::Client::new();
        let storage_client =
            StorageClient::new(&mock_server.uri(), "fake-key", http_client.clone());

        // delete_bucket を呼び出し、成功することを確認
        let result = storage_client.delete_bucket(bucket_id).await;
        assert!(result.is_ok(), "delete_bucket failed: {:?}", result.err());

        // モックをリセット
        mock_server.reset().await;

        // --- エラーケースのモック (例: 404 Not Found) ---
        let error_response = json!({ "message": "Bucket not found" });
        Mock::given(method("DELETE"))
            .and(path(format!("/storage/v1/bucket/{bucket_id}")))
            .respond_with(ResponseTemplate::new(404).set_body_json(error_response))
            .mount(&mock_server)
            .await;

        // delete_bucket を呼び出し、エラーになることを確認
        let result = storage_client.delete_bucket(bucket_id).await;
        assert!(result.is_err());
        if let Err(StorageError::ApiError(msg)) = result {
            assert!(msg.contains("Bucket not found"));
        } else {
            panic!("Expected ApiError, got {result:?}");
        }
    }

    #[tokio::test]
    async fn test_update_bucket() {
        // モックサーバーを起動
        let mock_server = MockServer::start().await;
        let bucket_id = "bucket-to-update";
        let updated_public_status = false;

        // --- 成功ケースのモック ---
        let request_body = json!({ "id": bucket_id, "public": updated_public_status });
        let response_body = json!({
            "id": bucket_id,
            "name": bucket_id,
            "owner": "owner-uuid",
            "public": updated_public_status, // 更新後のステータス
            "created_at": "2024-01-04T00:00:00Z",
            "updated_at": "2024-01-04T01:00:00Z"
        });

        Mock::given(method("PUT"))
            .and(path(format!("/storage/v1/bucket/{bucket_id}")))
            .and(wiremock::matchers::body_json(request_body.clone()))
            .respond_with(ResponseTemplate::new(200).set_body_json(response_body.clone()))
            .mount(&mock_server)
            .await;

        // クライアントを作成
        let http_client = reqwest::Client::new();
        let storage_client =
            StorageClient::new(&mock_server.uri(), "fake-key", http_client.clone());

        // update_bucket を呼び出し、成功することを確認
        let result = storage_client
            .update_bucket(bucket_id, updated_public_status)
            .await;
        assert!(result.is_ok(), "update_bucket failed: {:?}", result.err());
        let bucket = result.unwrap();
        assert_eq!(bucket.id, bucket_id);
        assert_eq!(bucket.public, updated_public_status);

        // モックをリセット
        mock_server.reset().await;

        // --- エラーケースのモック (例: 404 Not Found) ---
        let error_response = json!({ "message": "Bucket not found for update" });
        Mock::given(method("PUT"))
            .and(path(format!("/storage/v1/bucket/{bucket_id}")))
            .and(wiremock::matchers::body_json(request_body.clone())) // ボディも検証
            .respond_with(ResponseTemplate::new(404).set_body_json(error_response))
            .mount(&mock_server)
            .await;

        // update_bucket を呼び出し、エラーになることを確認
        let result = storage_client
            .update_bucket(bucket_id, updated_public_status)
            .await;
        assert!(result.is_err());
        if let Err(StorageError::ApiError(msg)) = result {
            assert!(msg.contains("Bucket not found for update"));
        } else {
            panic!("Expected ApiError, got {result:?}");
        }
    }

    #[tokio::test]
    async fn test_upload_file() {
        // モックサーバーを起動
        let mock_server = MockServer::start().await;
        let bucket_id = "upload-bucket";
        let object_path = "test_file.txt";
        let file_content = "Hello, Supabase Storage!";

        // 一時ファイルを作成して書き込む
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join(object_path);
        tokio::fs::write(&file_path, file_content).await.unwrap();

        // --- 成功ケースのモック ---
        let response_body = json!({
            "name": object_path,
            "bucket_id": bucket_id,
            "owner": "owner-uuid",
            "id": "file-id-upload",
            "updated_at": "2024-01-05T00:00:00Z",
            "created_at": "2024-01-05T00:00:00Z",
            "last_accessed_at": "2024-01-05T00:00:00Z",
            "metadata": { "size": file_content.len(), "mimetype": "text/plain" },
            "size": file_content.len(),
            "mime_type": "text/plain",
        });
        // multipart/form-data のリクエストボディを厳密に検証するのは wiremock では難しい場合があるため、
        // path と method のみでマッチング（必要ならヘッダーも）
        Mock::given(method("POST"))
            .and(path(format!(
                "/storage/v1/object/{bucket_id}/{object_path}"
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(response_body.clone()))
            .mount(&mock_server)
            .await;

        // クライアントを作成
        let http_client = reqwest::Client::new();
        let storage_client =
            StorageClient::new(&mock_server.uri(), "fake-key", http_client.clone());
        let bucket_client = storage_client.from(bucket_id);

        // upload を呼び出し、成功することを確認
        let result = bucket_client.upload(object_path, &file_path, None).await;
        assert!(result.is_ok(), "upload failed: {:?}", result.err());
        let file_object = result.unwrap();
        assert_eq!(file_object.name, object_path);
        assert_eq!(file_object.size, file_content.len() as i64);

        // モックをリセット
        mock_server.reset().await;

        // --- エラーケースのモック (例: 400 Bad Request) ---
        let error_response = json!({ "message": "Invalid upload parameters" });
        Mock::given(method("POST"))
            .and(path(format!(
                "/storage/v1/object/{bucket_id}/{object_path}",
            )))
            .respond_with(ResponseTemplate::new(400).set_body_json(error_response))
            .mount(&mock_server)
            .await;

        // upload を呼び出し、エラーになることを確認
        let result = bucket_client.upload(object_path, &file_path, None).await;
        assert!(result.is_err());
        if let Err(StorageError::ApiError(msg)) = result {
            assert!(msg.contains("Invalid upload parameters"));
        } else {
            panic!("Expected ApiError, got {result:?}");
        }

        // 一時ファイルをクリーンアップ (temp_dir がスコープを抜けるときに自動で行われる)
    }

    #[tokio::test]
    async fn test_download_file() {
        // モックサーバーを起動
        let mock_server = MockServer::start().await;
        let bucket_id = "download-bucket";
        let object_path = "download_me.txt";
        let file_content = Bytes::from_static(b"File content to download");

        // --- 成功ケースのモック ---
        Mock::given(method("GET"))
            .and(path(format!(
                "/storage/v1/object/{bucket_id}/{object_path}",
            )))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(file_content.clone()))
            .mount(&mock_server)
            .await;

        // クライアントを作成
        let http_client = reqwest::Client::new();
        let storage_client =
            StorageClient::new(&mock_server.uri(), "fake-key", http_client.clone());
        let bucket_client = storage_client.from(bucket_id);

        // download を呼び出し、成功することを確認
        let result = bucket_client.download(object_path).await;
        assert!(result.is_ok(), "download failed: {:?}", result.err());
        let downloaded_content = result.unwrap();
        assert_eq!(downloaded_content, file_content);

        // モックをリセット
        mock_server.reset().await;

        // --- エラーケースのモック (例: 404 Not Found) ---
        let error_response = json!({ "message": "File not found" });
        Mock::given(method("GET"))
            .and(path(format!(
                "/storage/v1/object/{bucket_id}/{object_path}",
            )))
            .respond_with(ResponseTemplate::new(404).set_body_json(error_response))
            .mount(&mock_server)
            .await;

        // download を呼び出し、エラーになることを確認
        let result = bucket_client.download(object_path).await;
        assert!(result.is_err());
        if let Err(StorageError::ApiError(msg)) = result {
            assert!(msg.contains("File not found"));
        } else {
            panic!("Expected ApiError, got {result:?}");
        }
    }

    #[tokio::test]
    async fn test_list_files() {
        // モックサーバーを起動
        let mock_server = MockServer::start().await;
        let bucket_id = "list-bucket";
        let prefix = "folder/";

        // --- 成功ケースのモック ---
        let list_options = ListOptions::new()
            .limit(10)
            .offset(0)
            .sort_by("name", SortOrder::Asc);
        let response_body = json!([
            {
                "name": "folder/file1.txt",
                "id": "uuid1",
                "updated_at": "2024-01-05T00:00:00Z",
                "created_at": "2024-01-05T00:00:00Z",
                "last_accessed_at": "2024-01-05T00:00:00Z",
                "metadata": { "size": 100, "mimetype": "text/plain" },
                "bucket_id": bucket_id,
                "owner": "owner-uuid",
                "size": 100,
                "mime_type": "text/plain",
            },
            {
                "name": "folder/file2.png",
                "id": "uuid2",
                "updated_at": "2024-01-05T01:00:00Z",
                "created_at": "2024-01-05T01:00:00Z",
                "last_accessed_at": "2024-01-05T01:00:00Z",
                "metadata": { "size": 2048, "mimetype": "image/png" },
                "bucket_id": bucket_id,
                "owner": "owner-uuid",
                "size": 2048,
                "mime_type": "image/png",
            }
        ]);

        // Fix: Expect GET request with query parameters
        Mock::given(method("GET")) // Changed from POST to GET
            .and(path(format!("/storage/v1/object/list/{bucket_id}")))
            // Add query parameter matchers
            .and(wiremock::matchers::query_param("prefix", prefix))
            .and(wiremock::matchers::query_param("limit", "10"))
            .and(wiremock::matchers::query_param("offset", "0"))
            .and(wiremock::matchers::query_param(
                "sortBy", "name:Asc", // Match the format from ListOptions::to_string()
            ))
            // Remove body matcher
            // .and(wiremock::matchers::body_json(request_body.clone()))
            .respond_with(ResponseTemplate::new(200).set_body_json(response_body.clone()))
            .mount(&mock_server)
            .await;

        // クライアントを作成
        let http_client = reqwest::Client::new();
        let storage_client =
            StorageClient::new(&mock_server.uri(), "fake-key", http_client.clone());
        let bucket_client = storage_client.from(bucket_id);

        // list を呼び出し、成功することを確認
        let result = bucket_client.list(prefix, Some(list_options)).await;
        assert!(result.is_ok(), "list failed: {:?}", result.err());
        let files = result.unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].name, "folder/file1.txt");
        assert_eq!(files[1].mime_type, Some("image/png".to_string()));

        // モックをリセット
        mock_server.reset().await;

        // --- エラーケースのモック (例: 400 Bad Request) ---
        let error_response = json!({ "message": "Invalid list parameters" });
        // Fix: Expect GET request for error case as well
        Mock::given(method("GET")) // Changed from POST to GET
            .and(path(format!("/storage/v1/object/list/{bucket_id}")))
            // Add query param matchers for the specific error case call
            .and(wiremock::matchers::query_param("prefix", prefix)) // Assuming same prefix
            // No other options are sent in the error test call: bucket_client.list(prefix, Some(ListOptions::new()))
            .respond_with(ResponseTemplate::new(400).set_body_json(error_response))
            .mount(&mock_server)
            .await;

        // list を呼び出し、エラーになることを確認
        let result = bucket_client.list(prefix, Some(ListOptions::new())).await; // エラーケース用のオプション
        assert!(result.is_err());
        if let Err(StorageError::ApiError(msg)) = result {
            assert!(msg.contains("Invalid list parameters"));
        } else {
            panic!("Expected ApiError, got {result:?}");
        }
    }

    #[tokio::test]
    async fn test_remove_files() {
        // モックサーバーを起動
        let mock_server = MockServer::start().await;
        let bucket_id = "remove-bucket";
        let paths_to_remove = vec!["file_a.txt", "folder/file_b.log"];

        // --- 成功ケースのモック ---
        let request_body = json!({ "prefixes": paths_to_remove });
        let response_body = json!([]); // 成功時は空の配列か特定のメッセージを返す場合がある

        Mock::given(method("DELETE"))
            .and(path(format!("/storage/v1/object/{bucket_id}")))
            .and(wiremock::matchers::body_json(request_body.clone()))
            .respond_with(ResponseTemplate::new(200).set_body_json(response_body.clone()))
            .mount(&mock_server)
            .await;

        // クライアントを作成
        let http_client = reqwest::Client::new();
        let storage_client =
            StorageClient::new(&mock_server.uri(), "fake-key", http_client.clone());
        let bucket_client = storage_client.from(bucket_id);

        // remove を呼び出し、成功することを確認
        let result = bucket_client.remove(paths_to_remove).await;
        assert!(result.is_ok(), "remove failed: {:?}", result.err());

        // モックをリセット
        mock_server.reset().await;

        // --- エラーケースのモック (例: 400 Bad Request) ---
        let error_response = json!({ "message": "Invalid paths provided" });
        Mock::given(method("DELETE"))
            .and(path(format!("/storage/v1/object/{bucket_id}")))
            .and(wiremock::matchers::body_json(request_body.clone())) // 同じボディを期待
            .respond_with(ResponseTemplate::new(400).set_body_json(error_response))
            .mount(&mock_server)
            .await;

        // remove を呼び出し、エラーになることを確認
        let paths_for_error = vec!["file_a.txt", "folder/file_b.log"]; // エラーケース用
        let result = bucket_client.remove(paths_for_error).await;
        assert!(result.is_err());
        if let Err(StorageError::ApiError(msg)) = result {
            assert!(msg.contains("Invalid paths provided"));
        } else {
            panic!("Expected ApiError, got {result:?}");
        }
    }

    #[tokio::test]
    async fn test_create_signed_url() {
        // モックサーバーを起動
        let mock_server = MockServer::start().await;
        let bucket_id = "signed-url-bucket";
        let object_path = "private/doc.pdf";
        let expires_in = 3600;
        let expected_signed_url = format!(
            "{}/storage/v1/object/sign/{}/{}?token=test-token",
            mock_server.uri(),
            bucket_id,
            object_path
        );

        // --- 成功ケースのモック ---
        let request_body = json!({ "expiresIn": expires_in });
        let response_body = json!({ "signed_url": expected_signed_url }); // APIはsigned_urlを返す

        Mock::given(method("POST"))
            .and(path(format!(
                "/storage/v1/object/sign/{bucket_id}/{object_path}"
            )))
            .and(wiremock::matchers::body_json(request_body.clone()))
            .respond_with(ResponseTemplate::new(200).set_body_json(response_body.clone()))
            .mount(&mock_server)
            .await;

        // クライアントを作成
        let http_client = reqwest::Client::new();
        let storage_client =
            StorageClient::new(&mock_server.uri(), "fake-key", http_client.clone());
        let bucket_client = storage_client.from(bucket_id);

        // create_signed_url を呼び出し、成功することを確認
        let result = bucket_client
            .create_signed_url(object_path, expires_in)
            .await;
        assert!(
            result.is_ok(),
            "create_signed_url failed: {:?}",
            result.err()
        );
        let signed_url = result.unwrap();
        // モックのレスポンスに含まれる URL と一致するか検証 (実際の token は異なる)
        assert!(signed_url.contains(&format!(
            "/storage/v1/object/sign/{bucket_id}/{object_path}"
        )));

        // モックをリセット
        mock_server.reset().await;

        // --- エラーケースのモック (例: 404 Not Found) ---
        let error_response = json!({ "message": "Object not found" });
        Mock::given(method("POST"))
            .and(path(format!(
                "/storage/v1/object/sign/{bucket_id}/{object_path}"
            )))
            .and(wiremock::matchers::body_json(request_body.clone()))
            .respond_with(ResponseTemplate::new(404).set_body_json(error_response))
            .mount(&mock_server)
            .await;

        // create_signed_url を呼び出し、エラーになることを確認
        let result = bucket_client
            .create_signed_url(object_path, expires_in)
            .await;
        assert!(result.is_err());
        if let Err(StorageError::ApiError(msg)) = result {
            assert!(msg.contains("Object not found"));
        } else {
            panic!("Expected ApiError, got {result:?}");
        }
    }

    #[tokio::test]
    async fn test_get_public_url() {
        let http_client = reqwest::Client::new();
        let storage_client =
            StorageClient::new("https://test.supabase.co", "anon-key", http_client);
        let bucket_client = storage_client.from("public-images");
        let object_path = "logos/supabase.png";

        let public_url = bucket_client.get_public_url(object_path);

        assert_eq!(
            public_url,
            "https://test.supabase.co/storage/v1/object/public/public-images/logos/supabase.png"
        );
    }

    #[tokio::test]
    async fn test_multipart_upload() {
        // モックサーバーを起動
        let mock_server = MockServer::start().await;
        let bucket_id = "multipart-bucket";
        let object_path = "large_file.dat";
        // チャンクサイズより大きいファイル内容
        let file_content = Bytes::from_static(b"Part1ContentPart2MoreContent");
        let chunk_size = 10; // 小さなチャンクサイズでテスト

        // 一時ファイルを作成
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join(object_path);
        tokio::fs::write(&file_path, &file_content).await.unwrap();

        let upload_id = "test-upload-id-complex";

        // 1. Initiate Multipart Upload のモック
        let initiate_request_body = json!({
            "bucket": bucket_id,
            "name": object_path,
            "cacheControl": "max-age=3600",
            "contentType": "application/octet-stream",
            "upsert": false
        });
        let initiate_response_body = json!({
            "id": "file-id-multi",
            "uploadId": upload_id,
            "key": object_path,
            "bucket": bucket_id
        });
        Mock::given(method("POST"))
            .and(path("/storage/v1/upload/initiate"))
            .and(wiremock::matchers::body_json(initiate_request_body))
            .respond_with(ResponseTemplate::new(200).set_body_json(initiate_response_body))
            .mount(&mock_server)
            .await;

        // 2. Upload Part のモック (2つのチャンクを想定)
        let etag1 = "etag-part-1";
        Mock::given(method("POST"))
            .and(path("/storage/v1/upload/part"))
            .and(wiremock::matchers::query_param("uploadId", upload_id))
            .and(wiremock::matchers::query_param("partNumber", "1"))
            .and(wiremock::matchers::query_param("bucket", bucket_id))
            .respond_with(ResponseTemplate::new(200).insert_header("ETag", etag1))
            .mount(&mock_server)
            .await;

        let etag2 = "etag-part-2";
        Mock::given(method("POST"))
            .and(path("/storage/v1/upload/part"))
            .and(wiremock::matchers::query_param("uploadId", upload_id))
            .and(wiremock::matchers::query_param("partNumber", "2"))
            .and(wiremock::matchers::query_param("bucket", bucket_id))
            .respond_with(ResponseTemplate::new(200).insert_header("ETag", etag2))
            .mount(&mock_server)
            .await;

        let etag3 = "etag-part-3"; // 3つ目のチャンク用
        Mock::given(method("POST"))
            .and(path("/storage/v1/upload/part"))
            .and(wiremock::matchers::query_param("uploadId", upload_id))
            .and(wiremock::matchers::query_param("partNumber", "3"))
            .and(wiremock::matchers::query_param("bucket", bucket_id))
            .respond_with(ResponseTemplate::new(200).insert_header("ETag", etag3))
            .mount(&mock_server)
            .await;

        // 3. Complete Multipart Upload のモック
        let expected_parts = vec![
            json!({ "partNumber": 1, "etag": etag1 }),
            json!({ "partNumber": 2, "etag": etag2 }),
            json!({ "partNumber": 3, "etag": etag3 }), // 3つ目のチャンクを追加
        ];
        let complete_request_body = json!({
            "uploadId": upload_id,
            "parts": expected_parts
        });
        let complete_response_body = json!({
            "name": object_path,
            "bucket_id": bucket_id,
            "owner": "owner-uuid",
            "id": "file-id-multi-complete",
            "updated_at": "2024-01-07T00:00:00Z",
            "created_at": "2024-01-07T00:00:00Z",
            "last_accessed_at": "2024-01-07T00:00:00Z",
            "metadata": { "size": file_content.len(), "mimetype": "application/octet-stream" },
            "size": file_content.len(),
            "mime_type": "application/octet-stream",
        });
        Mock::given(method("POST"))
            .and(path("/storage/v1/upload/complete"))
            .and(wiremock::matchers::query_param("bucket", bucket_id))
            .and(wiremock::matchers::query_param("key", object_path))
            .and(wiremock::matchers::body_json(complete_request_body))
            .respond_with(ResponseTemplate::new(200).set_body_json(complete_response_body))
            .mount(&mock_server)
            .await;

        // クライアントを作成
        let http_client = reqwest::Client::new();
        let storage_client =
            StorageClient::new(&mock_server.uri(), "fake-key", http_client.clone());
        let bucket_client = storage_client.from(bucket_id);

        // upload_large_file を呼び出し、成功することを確認
        let result = bucket_client
            .upload_large_file(object_path, &file_path, chunk_size, None)
            .await;
        assert!(
            result.is_ok(),
            "upload_large_file failed: {:?}",
            result.err()
        );
        let file_object = result.unwrap();
        assert_eq!(file_object.name, object_path);
        assert_eq!(file_object.size, file_content.len() as i64);
    }

    #[tokio::test]
    async fn test_transform_image() {
        // モックサーバーを起動
        let mock_server = MockServer::start().await;
        let bucket_id = "transform-bucket";
        let object_path = "images/logo_to_transform.png";
        let transform_options = ImageTransformOptions::new()
            .with_width(100)
            .with_height(100)
            .with_resize("contain")
            .with_format("webp");
        let expected_image_bytes = Bytes::from_static(b"transformed_image_data");

        // クライアントを作成
        let http_client = reqwest::Client::new();
        let storage_client = StorageClient::new(&mock_server.uri(), "fake-anon-key", http_client);
        let bucket_client = storage_client.from(bucket_id);

        // --- 成功ケースのモック ---
        Mock::given(method("GET"))
            .and(path(format!(
                "/object/transform/authenticated/{bucket_id}/{object_path}"
            )))
            .and(wiremock::matchers::query_param("width", "100"))
            .and(wiremock::matchers::query_param("height", "100"))
            .and(wiremock::matchers::query_param("resize", "contain"))
            .and(wiremock::matchers::query_param("format", "webp"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(expected_image_bytes.clone()))
            .mount(&mock_server)
            .await;

        // transform_image を呼び出し、成功することを確認
        let result = bucket_client
            .transform_image(object_path, transform_options.clone())
            .await;
        assert!(result.is_ok(), "transform_image failed: {:?}", result.err());
        let image_bytes = result.unwrap();
        assert_eq!(image_bytes, expected_image_bytes);

        // モックをリセット
        mock_server.reset().await;

        // --- エラーケースのモック (例: 400 Bad Request) ---
        let error_response = json!({ "statusCode": "400", "error": "BadRequest", "message": "Invalid transform options" });
        Mock::given(method("GET"))
            .and(path(format!(
                "/object/transform/authenticated/{bucket_id}/{object_path}"
            )))
            // クエリパラメータの検証は省略（パスのみで判断）
            .respond_with(ResponseTemplate::new(400).set_body_json(error_response))
            .mount(&mock_server)
            .await;

        // transform_image を呼び出し、エラーになることを確認
        let result = bucket_client
            .transform_image(object_path, transform_options) // transform_options を再利用
            .await;
        assert!(result.is_err());
        if let Err(StorageError::ApiError(msg)) = result {
            assert!(msg.contains("Invalid transform options") || msg.contains("BadRequest"));
        } else {
            panic!("Expected ApiError, got {result:?}");
        }
    }

    #[tokio::test]
    async fn test_create_signed_transform_url() {
        // 関数名を修正
        // モックサーバーを起動
        let mock_server = MockServer::start().await;
        let bucket_id = "transform-bucket";
        let object_path = "images/logo.png";
        let expires_in = 3600; // 1 hour
        let transform_options = ImageTransformOptions::new()
            .with_width(50)
            .with_height(50)
            .with_resize("cover")
            .with_format("jpeg")
            .with_quality(80);

        // クライアントを作成
        let http_client = reqwest::Client::new();
        let storage_client = StorageClient::new(&mock_server.uri(), "fake-anon-key", http_client);
        let bucket_client = storage_client.from(bucket_id);

        // --- 成功ケースのモック ---
        let expected_transform_string = transform_options.to_query_params();
        let expected_request_body = json!({
            "expiresIn": expires_in,
            "transform": expected_transform_string // Expect transform string in the body
        });
        let expected_signed_url = format!(
            "{}/storage/v1/object/sign/{}/{}?token=test-token&transform={}",
            mock_server.uri(),
            bucket_id,
            object_path,
            expected_transform_string
        );
        let response_body = json!({ "signed_url": expected_signed_url });

        Mock::given(method("POST"))
            .and(path(format!(
                "/object/sign/{bucket_id}/{object_path}" // Removed /storage/v1 prefix
            )))
            .and(wiremock::matchers::body_json(expected_request_body.clone()))
            .respond_with(ResponseTemplate::new(200).set_body_json(response_body))
            .mount(&mock_server)
            .await;

        // create_signed_transform_url を呼び出し、成功することを確認
        let result = bucket_client
            .create_signed_transform_url(object_path, transform_options.clone(), expires_in)
            .await;
        assert!(
            result.is_ok(),
            "create_signed_transform_url failed: {:?}",
            result.err()
        );
        let signed_url = result.unwrap();
        // 返されたURLが期待通りか検証
        assert!(signed_url.contains(&format!(
            "/storage/v1/object/sign/{bucket_id}/{object_path}"
        )));
        assert!(signed_url.contains("token=test-token")); // モックに基づいたトークン
        assert!(signed_url.contains(&expected_transform_string));

        // モックをリセット
        mock_server.reset().await;

        // --- エラーケースのモック (例: 400 Bad Request) ---
        let error_response = json!({ "statusCode": "400", "error": "BadRequest", "message": "Invalid transform parameters" });
        let expected_request_body_err = json!({
            "expiresIn": expires_in,
            "transform": expected_transform_string
        });

        Mock::given(method("POST"))
            .and(path(format!(
                "/object/sign/{bucket_id}/{object_path}" // Removed /storage/v1 prefix
            )))
            .and(wiremock::matchers::body_json(
                expected_request_body_err.clone(),
            ))
            .respond_with(ResponseTemplate::new(400).set_body_json(error_response))
            .mount(&mock_server)
            .await;

        // create_signed_transform_url を呼び出し、エラーになることを確認
        let result = bucket_client
            .create_signed_transform_url(object_path, transform_options, expires_in) // transform_options を再利用
            .await;
        assert!(result.is_err());
        if let Err(StorageError::ApiError(msg)) = result {
            // エラーメッセージはAPIの実装により異なる可能性がある
            assert!(
                msg.contains("Invalid transform parameters") || msg.contains("BadRequest") // statusCode や error フィールドを含むか確認
            );
        } else {
            panic!("Expected ApiError, got {result:?}");
        }
    }
}
