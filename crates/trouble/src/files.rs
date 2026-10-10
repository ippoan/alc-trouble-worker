//! 添付ファイル (rust の `alc-trouble` の files.rs・tasks.rs のファイルの口と repo/trouble_files.rs の写し)。
//!
//! - `POST`/`GET /trouble/tickets/{ticket_id}/files` — multipart の最初の 1 つを R2 に置き、行を作る (201)。
//!   一覧は論理削除していない・タスクに付いていないものを作成の順
//! - `GET /trouble/tickets/{ticket_id}/files/trash` — 論理削除したもの (チケットのものだけ) を削除の新しい順
//! - `POST`/`GET /trouble/tasks/{task_id}/files` — タスクの添付 (行の `ticket_id` はタスクのチケット)
//! - `GET /trouble/files/{id}/download`・`GET /trouble/task-files/{id}/download` — 本体と
//!   `Content-Type` (保存した値)・`Content-Disposition: attachment; filename="<名前の " を _ に>"` (rust と同じく
//!   名前はそのまま。日本語も符号化しない)。論理削除したものも落とせる (rust と同じ)
//! - `DELETE /trouble/files/{id}`・`DELETE /trouble/task-files/{id}` — 論理削除 (204)。R2 の実体は消さない
//! - `POST /trouble/files/{id}/restore` — 論理削除を戻す (204)
//!
//! 無いもの・別の tenant のものは 404 (本文なし)。R2 の binding が無ければ 503 (rust の storage 無しと同じ)。
//! 上限は rust と同じ 20MB ([`crate::router`] の `DefaultBodyLimit`)。超えると multipart の読みが失敗して 400。
//! 拡張子・Content-Type の許可リストは rust にも無い。
//!
//! アップロードは先に `tenant_tx` で親 (チケット / タスク) がこの tenant に在るか確かめ (RLS + `WHERE tenant_id`)、
//! 無ければ (無い・別の tenant) R2 に書かずに 404 (rust の tickets.get / tasks.get と同じ)。外部キーの検査は
//! RLS を通らないので、この確かめが無いと別の tenant の親に行を付けられる。
//! R2 のキーは rust と同じ (`{tenant}/trouble/{ticket}/{uuid}.{ext}`・`{tenant}/trouble/tasks/{task}/{uuid}.{ext}`) で、
//! uuid はその確かめの文の `gen_random_uuid()` で取る (wasm で乱数の crate を持たないため。ここだけ rust と違う)。
//!
//! R2 の await は `tenant_tx` の中に入れない (親の確かめ → R2 → 行の作成 を別々の短い transaction に分ける)。
//! ログは固定の語だけ (ファイル名・キー・tenant を出さない)。

use std::sync::Arc;

use alc_core_wasm::TenantId;
use alc_worker_db::{PgClient, TxOutput};
use axum::extract::{Multipart, Path, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use serde::Serialize;
use tokio_postgres::types::Type;
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::AppState;

/// R2 の段の失敗。持つのは段の名前だけ (キーやランタイムのエラー文を持たない)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreError(pub &'static str);

/// ファイルの置き場 (Worker では R2 の binding `TROUBLE_R2`、テストではモック)。
/// axum の state と handler が要るので `Send + Sync` で `Send` な future を返す
pub trait FileStore: Send + Sync {
    fn put<'a>(
        &'a self,
        key: &'a str,
        bytes: Vec<u8>,
        content_type: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// 無いキーも失敗 (rust の download と同じく 500 にする)
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Vec<u8>, StoreError>>;
}

pub type SharedStore = Arc<dyn FileStore>;

/// rust の `TroubleFile` と同じ形・同じキーの順
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct File {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub ticket_id: Uuid,
    pub task_id: Option<Uuid>,
    pub filename: String,
    pub content_type: String,
    pub size_bytes: i64,
    pub storage_key: String,
    pub created_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

impl TxOutput for File {}

/// 添付の親
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Parent {
    Ticket(Uuid),
    Task(Uuid),
}

/// R2 のキー (rust と同じ。拡張子は最後の `.` より後 = `.` が無ければ名前の全体)
pub fn storage_key(tenant_id: Uuid, parent: Parent, file_uuid: Uuid, filename: &str) -> String {
    let ext = filename.rsplit('.').next().unwrap_or("bin");
    match parent {
        Parent::Ticket(ticket_id) => format!("{tenant_id}/trouble/{ticket_id}/{file_uuid}.{ext}"),
        Parent::Task(task_id) => format!("{tenant_id}/trouble/tasks/{task_id}/{file_uuid}.{ext}"),
    }
}

/// rust と同じ `Content-Disposition` (`"` だけを `_` に替える)
pub fn content_disposition(filename: &str) -> String {
    format!("attachment; filename=\"{}\"", filename.replace('"', "_"))
}

macro_rules! columns {
    () => {
        "id, tenant_id, ticket_id, task_id, filename, content_type, size_bytes, storage_key, \
         created_at, deleted_at"
    };
}

pub mod sql {
    /// チケットがこの tenant に在るか (rust の tickets.get と同じく論理削除したものは無い扱い) と、新しいファイルの uuid
    pub const TICKET_PARENT: &str = "SELECT id, gen_random_uuid() FROM alc_api.trouble_tickets \
         WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL";

    /// タスクがこの tenant に在るか (rust の tasks.get) と、そのチケット・新しいファイルの uuid
    pub const TASK_PARENT: &str = "SELECT ticket_id, gen_random_uuid() FROM alc_api.trouble_tasks \
         WHERE id = $1 AND tenant_id = $2";

    /// rust の create (`task_id` = NULL) と create_for_task の 1 本
    pub const INSERT: &str = concat!(
        "INSERT INTO alc_api.trouble_files \
         (tenant_id, ticket_id, task_id, filename, content_type, size_bytes, storage_key) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING ",
        columns!()
    );

    pub const LIST_BY_TICKET: &str = concat!(
        "SELECT ",
        columns!(),
        " FROM alc_api.trouble_files WHERE ticket_id = $1 AND tenant_id = $2 \
         AND task_id IS NULL AND deleted_at IS NULL ORDER BY created_at"
    );

    pub const LIST_BY_TASK: &str = concat!(
        "SELECT ",
        columns!(),
        " FROM alc_api.trouble_files WHERE task_id = $1 AND tenant_id = $2 \
         AND deleted_at IS NULL ORDER BY created_at"
    );

    pub const LIST_TRASH: &str = concat!(
        "SELECT ",
        columns!(),
        " FROM alc_api.trouble_files WHERE ticket_id = $1 AND tenant_id = $2 \
         AND task_id IS NULL AND deleted_at IS NOT NULL ORDER BY deleted_at DESC"
    );

    /// 落とすのに要る列だけ (rust の get と同じく論理削除したものも返す)
    pub const GET_FOR_DOWNLOAD: &str = "SELECT filename, content_type, storage_key \
         FROM alc_api.trouble_files WHERE id = $1 AND tenant_id = $2";

    pub const SOFT_DELETE: &str = "UPDATE alc_api.trouble_files SET deleted_at = now() \
         WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL";

    pub const RESTORE: &str = "UPDATE alc_api.trouble_files SET deleted_at = NULL \
         WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NOT NULL";
}

fn file(r: &Row) -> File {
    File {
        id: r.get(0),
        tenant_id: r.get(1),
        ticket_id: r.get(2),
        task_id: r.get(3),
        filename: r.get(4),
        content_type: r.get(5),
        size_bytes: r.get(6),
        storage_key: r.get(7),
        created_at: r.get(8),
        deleted_at: r.get(9),
    }
}

/// 親がこの tenant に在れば、行に入れる `ticket_id` と新しいファイルの uuid。無ければ (無い・別の tenant) None
pub async fn parent_ticket(
    pg: &mut PgClient,
    tenant_id: Uuid,
    parent: Parent,
) -> Result<Option<(Uuid, Uuid)>, Error> {
    let (stmt, id) = match parent {
        Parent::Ticket(id) => (sql::TICKET_PARENT, id),
        Parent::Task(id) => (sql::TASK_PARENT, id),
    };
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(stmt, &[(&id, Type::UUID), (&tenant_id, Type::UUID)])
                .await?;
            Ok(row.map(|r| (r.get(0), r.get(1))))
        })
    })
    .await
}

/// 作る行 (R2 に置いた後に [`insert`] へ渡す)
pub struct NewFile {
    pub ticket_id: Uuid,
    pub task_id: Option<Uuid>,
    pub filename: String,
    pub content_type: String,
    pub size_bytes: i64,
    pub storage_key: String,
}

pub async fn insert(pg: &mut PgClient, tenant_id: Uuid, new: NewFile) -> Result<File, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let n = &new;
            let row = tx
                .query_typed_one(
                    sql::INSERT,
                    &[
                        (&tenant_id, Type::UUID),
                        (&n.ticket_id, Type::UUID),
                        (&n.task_id, Type::UUID),
                        (&n.filename, Type::TEXT),
                        (&n.content_type, Type::TEXT),
                        (&n.size_bytes, Type::INT8),
                        (&n.storage_key, Type::TEXT),
                    ],
                )
                .await?;
            Ok(file(&row))
        })
    })
    .await
}

/// 一覧 (`stmt` は [`sql::LIST_BY_TICKET`]・[`sql::LIST_BY_TASK`]・[`sql::LIST_TRASH`] のどれか)
pub async fn list(
    pg: &mut PgClient,
    tenant_id: Uuid,
    stmt: &'static str,
    parent_id: Uuid,
) -> Result<Vec<File>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(stmt, &[(&parent_id, Type::UUID), (&tenant_id, Type::UUID)])
                .await?;
            Ok(rows.iter().map(file).collect())
        })
    })
    .await
}

/// 落とすのに要る (名前, Content-Type, キー)
pub async fn get_for_download(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
) -> Result<Option<(String, String, String)>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(
                    sql::GET_FOR_DOWNLOAD,
                    &[(&id, Type::UUID), (&tenant_id, Type::UUID)],
                )
                .await?;
            Ok(row.map(|r| (r.get(0), r.get(1), r.get(2))))
        })
    })
    .await
}

/// 論理削除・復元 (`stmt` は [`sql::SOFT_DELETE`] か [`sql::RESTORE`])。変えた行が在れば true
pub async fn set_deleted(
    pg: &mut PgClient,
    tenant_id: Uuid,
    stmt: &'static str,
    id: Uuid,
) -> Result<bool, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let n = tx
                .execute_typed(stmt, &[(&id, Type::UUID), (&tenant_id, Type::UUID)])
                .await?;
            Ok(n > 0)
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/trouble/tickets/{ticket_id}/files",
            post(upload_ticket_file).get(list_ticket_files),
        )
        .route("/trouble/tickets/{ticket_id}/files/trash", get(list_trash))
        .route(
            "/trouble/files/{file_id}",
            axum::routing::delete(delete_file),
        )
        .route("/trouble/files/{file_id}/download", get(download_file))
        .route("/trouble/files/{file_id}/restore", post(restore_file))
        .route(
            "/trouble/tasks/{task_id}/files",
            post(upload_task_file).get(list_task_files),
        )
        .route("/trouble/task-files/{file_id}/download", get(download_file))
        .route(
            "/trouble/task-files/{file_id}",
            axum::routing::delete(delete_file),
        )
}

impl AppState {
    /// R2 の失敗を固定の語でログに出し、500 にする
    fn store_fail(&self, stage: &str, e: StoreError) -> StatusCode {
        (self.log)(&format!("trouble: {stage} failed: r2 ({})", e.0));
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

/// rust と同じ順: 親の確かめ (404) → R2 の binding (503) → multipart の読み (400) → R2 → 行の作成
async fn upload(
    state: AppState,
    tenant_id: Uuid,
    parent: Parent,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<File>), StatusCode> {
    let found = {
        let mut pg = state.pg.lock().await;
        parent_ticket(&mut pg, tenant_id, parent).await
    };
    let (ticket_id, file_uuid) = found
        .map_err(|e| state.db_fail("file upload parent", &e))?
        .ok_or(StatusCode::NOT_FOUND)?;
    let store = state.files.clone().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let field = multipart
        .next_field()
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .ok_or(StatusCode::BAD_REQUEST)?;
    let filename = field.file_name().unwrap_or("unknown").to_owned();
    let content_type = field
        .content_type()
        .unwrap_or("application/octet-stream")
        .to_owned();
    let data = field.bytes().await.map_err(|_| StatusCode::BAD_REQUEST)?;
    let size_bytes = data.len() as i64;

    let key = storage_key(tenant_id, parent, file_uuid, &filename);
    store
        .put(&key, data.to_vec(), &content_type)
        .await
        .map_err(|e| state.store_fail("file upload", e))?;

    let task_id = match parent {
        Parent::Ticket(_) => None,
        Parent::Task(id) => Some(id),
    };
    let new = NewFile {
        ticket_id,
        task_id,
        filename,
        content_type,
        size_bytes,
        storage_key: key,
    };
    let mut pg = state.pg.lock().await;
    let file = insert(&mut pg, tenant_id, new)
        .await
        .map_err(|e| state.db_fail("file create", &e))?;
    Ok((StatusCode::CREATED, Json(file)))
}

async fn upload_ticket_file(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(ticket_id): Path<Uuid>,
    multipart: Multipart,
) -> Result<(StatusCode, Json<File>), StatusCode> {
    upload(state, tenant_id, Parent::Ticket(ticket_id), multipart).await
}

async fn upload_task_file(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(task_id): Path<Uuid>,
    multipart: Multipart,
) -> Result<(StatusCode, Json<File>), StatusCode> {
    upload(state, tenant_id, Parent::Task(task_id), multipart).await
}

async fn list_with(
    state: AppState,
    tenant_id: Uuid,
    stmt: &'static str,
    parent_id: Uuid,
) -> Result<Json<Vec<File>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    list(&mut pg, tenant_id, stmt, parent_id)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("file list", &e))
}

async fn list_ticket_files(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(ticket_id): Path<Uuid>,
) -> Result<Json<Vec<File>>, StatusCode> {
    list_with(state, tenant_id, sql::LIST_BY_TICKET, ticket_id).await
}

async fn list_trash(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(ticket_id): Path<Uuid>,
) -> Result<Json<Vec<File>>, StatusCode> {
    list_with(state, tenant_id, sql::LIST_TRASH, ticket_id).await
}

async fn list_task_files(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(task_id): Path<Uuid>,
) -> Result<Json<Vec<File>>, StatusCode> {
    list_with(state, tenant_id, sql::LIST_BY_TASK, task_id).await
}

/// rust と同じ順: 行 (404) → R2 の binding (503) → R2 から読む
async fn download_file(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(file_id): Path<Uuid>,
) -> Result<impl IntoResponse, StatusCode> {
    let found = {
        let mut pg = state.pg.lock().await;
        get_for_download(&mut pg, tenant_id, file_id).await
    };
    let (filename, content_type, key) = found
        .map_err(|e| state.db_fail("file download", &e))?
        .ok_or(StatusCode::NOT_FOUND)?;
    let store = state.files.clone().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let data = store
        .get(&key)
        .await
        .map_err(|e| state.store_fail("file download", e))?;
    Ok((
        [
            (header::CONTENT_TYPE, content_type),
            (header::CONTENT_DISPOSITION, content_disposition(&filename)),
        ],
        data,
    ))
}

async fn set_deleted_with(
    state: AppState,
    tenant_id: Uuid,
    stmt: &'static str,
    file_id: Uuid,
    stage: &str,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    let changed = set_deleted(&mut pg, tenant_id, stmt, file_id)
        .await
        .map_err(|e| state.db_fail(stage, &e))?;
    if changed {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

async fn delete_file(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(file_id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    set_deleted_with(state, tenant_id, sql::SOFT_DELETE, file_id, "file delete").await
}

async fn restore_file(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(file_id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    set_deleted_with(state, tenant_id, sql::RESTORE, file_id, "file restore").await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_the_rust_layout() {
        let t = Uuid::from_u128(1);
        let p = Uuid::from_u128(2);
        let f = Uuid::from_u128(3);
        assert_eq!(
            storage_key(t, Parent::Ticket(p), f, "見積.v2.pdf"),
            format!("{t}/trouble/{p}/{f}.pdf")
        );
        assert_eq!(
            storage_key(t, Parent::Task(p), f, "photo.JPG"),
            format!("{t}/trouble/tasks/{p}/{f}.JPG")
        );
        // `.` が無ければ名前の全体 (rust の rsplit と同じ)
        assert_eq!(
            storage_key(t, Parent::Ticket(p), f, "README"),
            format!("{t}/trouble/{p}/{f}.README")
        );
    }

    #[test]
    fn disposition_is_the_rust_string() {
        assert_eq!(
            content_disposition("報告書 \"最終\".pdf"),
            "attachment; filename=\"報告書 _最終_.pdf\""
        );
    }
}
