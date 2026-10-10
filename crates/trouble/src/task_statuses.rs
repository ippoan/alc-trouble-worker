//! タスクの状態のマスタ `/api/trouble/task-statuses` (rust の `alc-trouble` の task_statuses.rs と
//! repo/trouble_task_statuses.rs の写し)。
//!
//! - `GET` — 並びは `sort_order, name`。1 行も無ければ [`DEFAULT_SEED`] を 1 行ずつ作って返す (作れなかった行は飛ばす)
//! - `POST` — 201。名前が空白だけなら 400、制約違反 (key / name の重複) は 409。key が無い・空白だけなら名前を key にする
//! - `PUT /{id}` — 名前・色・並び順・完了扱いのうち来たものだけ変える。制約違反は 409、無ければ 404
//! - `DELETE /{id}` — 204 / 404

use alc_core_wasm::TenantId;
use alc_worker_db::{PgClient, TxOutput};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio_postgres::types::Type;
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::{is_conflict, AppState};

pub const PATH: &str = "/trouble/task-statuses";

/// テナントに 1 行も無いときに作る行 (key, 名前, 色, 並び順, 完了扱い)。migration 098 の seed と同じ
pub const DEFAULT_SEED: &[(&str, &str, &str, i32, bool)] = &[
    ("open", "未着手", "#9CA3AF", 10, false),
    ("in_progress", "進行中", "#3B82F6", 20, false),
    ("waiting", "待機", "#F59E0B", 30, false),
    ("done", "完了", "#10B981", 40, true),
];

/// 色が来なかったときの色
const DEFAULT_COLOR: &str = "#9CA3AF";

/// rust の `TroubleTaskStatus` と同じ形・同じキーの順
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TaskStatus {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub key: String,
    pub name: String,
    pub color: String,
    pub sort_order: i32,
    pub is_done: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl TxOutput for TaskStatus {}

/// rust の `CreateTroubleTaskStatus`
#[derive(Debug, Deserialize)]
pub struct CreateTaskStatus {
    pub key: Option<String>,
    pub name: String,
    pub color: Option<String>,
    pub sort_order: Option<i32>,
    pub is_done: Option<bool>,
}

/// rust の `UpdateTroubleTaskStatus`
#[derive(Debug, Deserialize)]
pub struct UpdateTaskStatus {
    pub name: Option<String>,
    pub color: Option<String>,
    pub sort_order: Option<i32>,
    pub is_done: Option<bool>,
}

pub mod sql {
    pub const LIST: &str =
        "SELECT id, tenant_id, key, name, color, sort_order, is_done, created_at, updated_at \
         FROM alc_api.trouble_task_statuses WHERE tenant_id = $1 ORDER BY sort_order, name";

    pub const INSERT: &str = "INSERT INTO alc_api.trouble_task_statuses (tenant_id, key, name, color, sort_order, is_done) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         RETURNING id, tenant_id, key, name, color, sort_order, is_done, created_at, updated_at";

    pub const UPDATE: &str = "UPDATE alc_api.trouble_task_statuses \
         SET name = COALESCE($3, name), color = COALESCE($4, color), \
         sort_order = COALESCE($5, sort_order), is_done = COALESCE($6, is_done), updated_at = now() \
         WHERE id = $1 AND tenant_id = $2 \
         RETURNING id, tenant_id, key, name, color, sort_order, is_done, created_at, updated_at";

    pub const DELETE: &str =
        "DELETE FROM alc_api.trouble_task_statuses WHERE id = $1 AND tenant_id = $2";
}

/// key が来ていて空白だけでなければその trim、無ければ名前の trim (rust の `derive_key`)
fn derive_key(key: Option<&str>, name: &str) -> String {
    match key.map(str::trim) {
        Some(k) if !k.is_empty() => k.to_owned(),
        _ => name.trim().to_owned(),
    }
}

fn status(r: &Row) -> TaskStatus {
    TaskStatus {
        id: r.get(0),
        tenant_id: r.get(1),
        key: r.get(2),
        name: r.get(3),
        color: r.get(4),
        sort_order: r.get(5),
        is_done: r.get(6),
        created_at: r.get(7),
        updated_at: r.get(8),
    }
}

pub async fn list(pg: &mut PgClient, tenant_id: Uuid) -> Result<Vec<TaskStatus>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::LIST, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(rows.iter().map(status).collect())
        })
    })
    .await
}

pub async fn create(
    pg: &mut PgClient,
    tenant_id: Uuid,
    input: CreateTaskStatus,
) -> Result<TaskStatus, Error> {
    let key = derive_key(input.key.as_deref(), &input.name);
    let name = input.name;
    let color = input.color.unwrap_or_else(|| DEFAULT_COLOR.to_owned());
    let sort_order = input.sort_order.unwrap_or(0);
    let is_done = input.is_done.unwrap_or(false);
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_one(
                    sql::INSERT,
                    &[
                        (&tenant_id, Type::UUID),
                        (&key, Type::TEXT),
                        (&name, Type::TEXT),
                        (&color, Type::TEXT),
                        (&sort_order, Type::INT4),
                        (&is_done, Type::BOOL),
                    ],
                )
                .await?;
            Ok(status(&row))
        })
    })
    .await
}

pub async fn update(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
    input: UpdateTaskStatus,
) -> Result<Option<TaskStatus>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(
                    sql::UPDATE,
                    &[
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                        (&input.name, Type::TEXT),
                        (&input.color, Type::TEXT),
                        (&input.sort_order, Type::INT4),
                        (&input.is_done, Type::BOOL),
                    ],
                )
                .await?;
            Ok(row.as_ref().map(status))
        })
    })
    .await
}

pub async fn delete(pg: &mut PgClient, tenant_id: Uuid, id: Uuid) -> Result<bool, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let n = tx
                .execute_typed(sql::DELETE, &[(&id, Type::UUID), (&tenant_id, Type::UUID)])
                .await?;
            Ok(n > 0)
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route(PATH, get(list_handler).post(create_handler))
        .route(
            &format!("{PATH}/{{id}}"),
            axum::routing::delete(delete_handler).put(update_handler),
        )
}

async fn list_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Vec<TaskStatus>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    let statuses = list(&mut pg, tenant_id)
        .await
        .map_err(|e| state.db_fail("task status list", &e))?;
    if !statuses.is_empty() {
        return Ok(Json(statuses));
    }
    let mut seeded = Vec::new();
    for (key, name, color, sort_order, is_done) in DEFAULT_SEED {
        let input = CreateTaskStatus {
            key: Some((*key).to_owned()),
            name: (*name).to_owned(),
            color: Some((*color).to_owned()),
            sort_order: Some(*sort_order),
            is_done: Some(*is_done),
        };
        match create(&mut pg, tenant_id, input).await {
            Ok(s) => seeded.push(s),
            Err(e) => {
                state.db_fail("task status auto-seed", &e);
            }
        }
    }
    Ok(Json(seeded))
}

async fn create_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<CreateTaskStatus>,
) -> Result<(StatusCode, Json<TaskStatus>), StatusCode> {
    if body.name.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut pg = state.pg.lock().await;
    let s = create(&mut pg, tenant_id, body).await.map_err(|e| {
        if is_conflict(&e) {
            return StatusCode::CONFLICT;
        }
        state.db_fail("task status create", &e)
    })?;
    Ok((StatusCode::CREATED, Json(s)))
}

async fn update_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateTaskStatus>,
) -> Result<Json<TaskStatus>, StatusCode> {
    let mut pg = state.pg.lock().await;
    update(&mut pg, tenant_id, id, body)
        .await
        .map_err(|e| {
            if is_conflict(&e) {
                return StatusCode::CONFLICT;
            }
            state.db_fail("task status update", &e)
        })?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn delete_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    let deleted = delete(&mut pg, tenant_id, id)
        .await
        .map_err(|e| state.db_fail("task status delete", &e))?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_falls_back_to_trimmed_name() {
        assert_eq!(derive_key(Some(" on_hold "), "保留"), "on_hold");
        assert_eq!(derive_key(Some("   "), " 保留 "), "保留");
        assert_eq!(derive_key(None, " 保留 "), "保留");
    }
}
