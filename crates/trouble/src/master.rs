//! 名前と並び順だけのマスタ 4 本 (rust の `alc_core::master_data` + `master_handlers` + `alc-trouble` の
//! categories.rs / offices.rs / progress_statuses.rs / task_types.rs の写し)。
//!
//! 4 本の表は同じ形 (`id / tenant_id / name / sort_order / created_at` + `UNIQUE (tenant_id, name)`) で、
//! 口も同じ: `GET` (並びは `sort_order, name`)・`POST` (201。名前が空白だけなら 400、制約違反は 409)・
//! `PUT /{id}` (sort_order だけ変える。無ければ 404)・`DELETE /{id}` (204 / 404)。
//! categories と task-types は、GET で 1 行も無ければ既定の行を作って返す (rust の auto-seed と同じ。
//! 1 行ずつ作り、作れなかった行は飛ばす)。
//!
//! **表の名前は SQL に文字列で埋め込む** (識別子は bind できない)。入口は [`MasterTable::TABLE`] の
//! 関連定数だけで、実行時の文字列を流し込む経路は無い。`TABLE` にはソースのリテラル以外を書かないこと。

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

/// rust の `alc_trouble::DEFAULT_CATEGORIES`
pub const DEFAULT_CATEGORIES: &[&str] = &[
    "苦情・トラブル",
    "貨物事故",
    "被害事故",
    "対物事故(他損)",
    "対物事故(自損)",
    "人身事故",
    "その他",
];

/// rust の `alc_trouble::DEFAULT_TASK_TYPES`
pub const DEFAULT_TASK_TYPES: &[&str] = &[
    "レッカー対応",
    "修理手配",
    "保険対応",
    "示談交渉",
    "処分決定",
    "再発防止策",
    "現場確認",
    "その他",
];

/// どの表・どの path の口かをコンパイル時に固定する
pub trait MasterTable: Send + Sync + 'static {
    /// schema `alc_api` の表の名前 (**リテラルだけ**)
    const TABLE: &'static str;
    /// `/api` の下の path
    const PATH: &'static str;
    /// GET で 1 行も無いときに作る行の名前 (並び順は 1 から)。空なら作らない
    const SEED: &'static [&'static str];
}

pub struct Categories;
impl MasterTable for Categories {
    const TABLE: &'static str = "trouble_categories";
    const PATH: &'static str = "/trouble/categories";
    const SEED: &'static [&'static str] = DEFAULT_CATEGORIES;
}

pub struct Offices;
impl MasterTable for Offices {
    const TABLE: &'static str = "trouble_offices";
    const PATH: &'static str = "/trouble/offices";
    const SEED: &'static [&'static str] = &[];
}

pub struct ProgressStatuses;
impl MasterTable for ProgressStatuses {
    const TABLE: &'static str = "trouble_progress_statuses";
    const PATH: &'static str = "/trouble/progress-statuses";
    const SEED: &'static [&'static str] = &[];
}

pub struct TaskTypes;
impl MasterTable for TaskTypes {
    const TABLE: &'static str = "trouble_task_types";
    const PATH: &'static str = "/trouble/task-types";
    const SEED: &'static [&'static str] = DEFAULT_TASK_TYPES;
}

/// 4 本の行 (rust の `TroubleCategory` / `TroubleOffice` / `TroubleProgressStatus` と同じ形・同じキーの順)
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MasterItem {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub name: String,
    pub sort_order: i32,
    pub created_at: DateTime<Utc>,
}

impl TxOutput for MasterItem {}

/// `POST` の body (rust の `CreateTroubleCategory` ほか)
#[derive(Debug, Deserialize)]
pub struct CreateMaster {
    pub name: String,
    pub sort_order: Option<i32>,
}

/// `PUT /{id}` の body (rust の `master_handlers::UpdateSortOrder`)
#[derive(Debug, Deserialize)]
pub struct UpdateSortOrder {
    pub sort_order: i32,
}

const COLUMNS: &str = "id, tenant_id, name, sort_order, created_at";

fn list_sql(table: &str) -> String {
    format!("SELECT {COLUMNS} FROM alc_api.{table} WHERE tenant_id = $1 ORDER BY sort_order, name")
}

fn insert_sql(table: &str) -> String {
    format!(
        "INSERT INTO alc_api.{table} (tenant_id, name, sort_order) VALUES ($1, $2, $3) RETURNING {COLUMNS}"
    )
}

fn delete_sql(table: &str) -> String {
    format!("DELETE FROM alc_api.{table} WHERE id = $1 AND tenant_id = $2")
}

fn update_sort_order_sql(table: &str) -> String {
    format!(
        "UPDATE alc_api.{table} SET sort_order = $3 WHERE id = $1 AND tenant_id = $2 RETURNING {COLUMNS}"
    )
}

fn item(r: &Row) -> MasterItem {
    MasterItem {
        id: r.get(0),
        tenant_id: r.get(1),
        name: r.get(2),
        sort_order: r.get(3),
        created_at: r.get(4),
    }
}

pub async fn list<M: MasterTable>(
    pg: &mut PgClient,
    tenant_id: Uuid,
) -> Result<Vec<MasterItem>, Error> {
    let sql = list_sql(M::TABLE);
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx.query_typed(&sql, &[(&tenant_id, Type::UUID)]).await?;
            Ok(rows.iter().map(item).collect())
        })
    })
    .await
}

pub async fn create<M: MasterTable>(
    pg: &mut PgClient,
    tenant_id: Uuid,
    name: String,
    sort_order: Option<i32>,
) -> Result<MasterItem, Error> {
    let sql = insert_sql(M::TABLE);
    let sort_order = sort_order.unwrap_or(0);
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_one(
                    &sql,
                    &[
                        (&tenant_id, Type::UUID),
                        (&name, Type::TEXT),
                        (&sort_order, Type::INT4),
                    ],
                )
                .await?;
            Ok(item(&row))
        })
    })
    .await
}

pub async fn delete<M: MasterTable>(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
) -> Result<bool, Error> {
    let sql = delete_sql(M::TABLE);
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let n = tx
                .execute_typed(&sql, &[(&id, Type::UUID), (&tenant_id, Type::UUID)])
                .await?;
            Ok(n > 0)
        })
    })
    .await
}

pub async fn update_sort_order<M: MasterTable>(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
    sort_order: i32,
) -> Result<Option<MasterItem>, Error> {
    let sql = update_sort_order_sql(M::TABLE);
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(
                    &sql,
                    &[
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                        (&sort_order, Type::INT4),
                    ],
                )
                .await?;
            Ok(row.as_ref().map(item))
        })
    })
    .await
}

pub fn router<M: MasterTable>() -> Router<AppState> {
    Router::new()
        .route(M::PATH, get(list_handler::<M>).post(create_handler::<M>))
        .route(
            &format!("{}/{{id}}", M::PATH),
            axum::routing::delete(delete_handler::<M>).put(update_sort_handler::<M>),
        )
}

async fn list_handler<M: MasterTable>(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Vec<MasterItem>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    let items = list::<M>(&mut pg, tenant_id)
        .await
        .map_err(|e| state.db_fail("master list", &e))?;
    if !items.is_empty() {
        return Ok(Json(items));
    }
    let mut seeded = Vec::new();
    for (i, name) in M::SEED.iter().enumerate() {
        match create::<M>(&mut pg, tenant_id, (*name).to_owned(), Some(i as i32 + 1)).await {
            Ok(it) => seeded.push(it),
            Err(e) => {
                state.db_fail("master auto-seed", &e);
            }
        }
    }
    Ok(Json(seeded))
}

async fn create_handler<M: MasterTable>(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<CreateMaster>,
) -> Result<(StatusCode, Json<MasterItem>), StatusCode> {
    if body.name.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut pg = state.pg.lock().await;
    let it = create::<M>(&mut pg, tenant_id, body.name, body.sort_order)
        .await
        .map_err(|e| {
            if is_conflict(&e) {
                return StatusCode::CONFLICT;
            }
            state.db_fail("master create", &e)
        })?;
    Ok((StatusCode::CREATED, Json(it)))
}

async fn update_sort_handler<M: MasterTable>(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateSortOrder>,
) -> Result<Json<MasterItem>, StatusCode> {
    let mut pg = state.pg.lock().await;
    update_sort_order::<M>(&mut pg, tenant_id, id, body.sort_order)
        .await
        .map_err(|e| state.db_fail("master update", &e))?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn delete_handler<M: MasterTable>(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    let deleted = delete::<M>(&mut pg, tenant_id, id)
        .await
        .map_err(|e| state.db_fail("master delete", &e))?;
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
    fn sql_embeds_only_the_table_constant() {
        assert_eq!(
            list_sql(Categories::TABLE),
            "SELECT id, tenant_id, name, sort_order, created_at FROM alc_api.trouble_categories \
             WHERE tenant_id = $1 ORDER BY sort_order, name"
        );
        assert_eq!(
            insert_sql(Offices::TABLE),
            "INSERT INTO alc_api.trouble_offices (tenant_id, name, sort_order) VALUES ($1, $2, $3) \
             RETURNING id, tenant_id, name, sort_order, created_at"
        );
        assert_eq!(
            delete_sql(ProgressStatuses::TABLE),
            "DELETE FROM alc_api.trouble_progress_statuses WHERE id = $1 AND tenant_id = $2"
        );
        assert_eq!(
            update_sort_order_sql(TaskTypes::TABLE),
            "UPDATE alc_api.trouble_task_types SET sort_order = $3 WHERE id = $1 AND tenant_id = $2 \
             RETURNING id, tenant_id, name, sort_order, created_at"
        );
    }

    #[test]
    fn seeds_match_rust() {
        assert_eq!(Categories::SEED.len(), 7);
        assert_eq!(TaskTypes::SEED.len(), 8);
        assert!(Offices::SEED.is_empty());
        assert!(ProgressStatuses::SEED.is_empty());
    }

    #[test]
    fn update_sort_order_body() {
        let body: UpdateSortOrder = serde_json::from_str(r#"{"sort_order": 3}"#).unwrap();
        assert_eq!(body.sort_order, 3);
    }
}
