//! チケットの入力欄の表示設定 `/api/trouble/field-layout` (rust の `alc-trouble` の field_layouts.rs と
//! repo/trouble_field_layouts.rs の写し)。
//!
//! - `GET` — tenant の設定。行が無ければ `{"settings": []}`
//! - `PUT` — 丸ごと置き換える (tenant ごとに 1 行。無ければ作る)。応答は保存した設定
//!
//! 列 `settings` (jsonb) は [`FieldLayoutEntry`] の配列として読み書きする (rust の `sqlx::types::Json` と同じ。
//! 読めない形なら 500)。

use alc_core_wasm::TenantId;
use alc_worker_db::{PgClient, TxOutput};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use tokio_postgres::types::{Json as PgJson, Type};
use tokio_postgres::Error;
use uuid::Uuid;

use crate::AppState;

pub const PATH: &str = "/trouble/field-layout";

/// 入力欄 1 つ分の上書き (rust の `TroubleFieldLayoutEntry` と同じ形・同じキーの順)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldLayoutEntry {
    pub key: String,
    pub visible: bool,
    pub width: String,
    pub sort_order: i32,
    /// tenant が付け替えた表示の名前。無ければ画面の既定
    pub label: Option<String>,
}

/// rust の `TroubleFieldLayout`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldLayout {
    pub settings: Vec<FieldLayoutEntry>,
}

impl TxOutput for FieldLayout {}

pub mod sql {
    pub const GET: &str = "SELECT settings FROM alc_api.trouble_field_layouts WHERE tenant_id = $1";

    pub const UPSERT: &str =
        "INSERT INTO alc_api.trouble_field_layouts (tenant_id, settings) VALUES ($1, $2) \
         ON CONFLICT (tenant_id) DO UPDATE SET settings = EXCLUDED.settings, updated_at = NOW() \
         RETURNING settings";
}

/// 列 `settings` を配列として読む (読めなければ from_sql のエラー)
fn settings(r: &tokio_postgres::Row) -> Result<Vec<FieldLayoutEntry>, Error> {
    Ok(r.try_get::<_, PgJson<Vec<FieldLayoutEntry>>>(0)?.0)
}

pub async fn get_layout(pg: &mut PgClient, tenant_id: Uuid) -> Result<FieldLayout, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(sql::GET, &[(&tenant_id, Type::UUID)])
                .await?;
            let settings = match row {
                Some(r) => settings(&r)?,
                None => Vec::new(),
            };
            Ok(FieldLayout { settings })
        })
    })
    .await
}

pub async fn upsert(
    pg: &mut PgClient,
    tenant_id: Uuid,
    layout: FieldLayout,
) -> Result<FieldLayout, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_one(
                    sql::UPSERT,
                    &[
                        (&tenant_id, Type::UUID),
                        (&PgJson(&layout.settings), Type::JSONB),
                    ],
                )
                .await?;
            Ok(FieldLayout {
                settings: settings(&row)?,
            })
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new().route(PATH, get(get_handler).put(put_handler))
}

async fn get_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<FieldLayout>, StatusCode> {
    let mut pg = state.pg.lock().await;
    get_layout(&mut pg, tenant_id)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("field layout get", &e))
}

async fn put_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<FieldLayout>,
) -> Result<Json<FieldLayout>, StatusCode> {
    let mut pg = state.pg.lock().await;
    upsert(&mut pg, tenant_id, body)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("field layout put", &e))
}
