//! チケットのタスク (経過記録) (rust の `alc-trouble` の tasks.rs と repo/trouble_tasks.rs の写し)。
//!
//! - `GET /trouble/tasks` — チケット横断の一覧。絞り込み 9 種・並び 5 種 (許可リストに無い `sort_by` は 400)・
//!   ページ (`per_page` は 1〜200)。`{items, total, page, per_page}`
//! - `POST /trouble/tickets/{ticket_id}/tasks` — 201。題名が空 (空白だけ) は 400。`sort_order` を省くと末尾。
//!   チケットがこの tenant に無い (無い・別の tenant) なら 404 で作らない (**rust と違う**: rust は確かめず、
//!   無いチケットは外部キーの違反で 500、別の tenant のチケットには作れてしまう)
//! - `GET /trouble/tickets/{ticket_id}/tasks` — `sort_order`・作成の順
//! - `PUT /trouble/tickets/{ticket_id}/tasks/reorder` — `task_ids` の順に 0 から振り直し、並べた一覧を返す。
//!   空・重複は 400。そのチケットに無い id が混ざれば 404 で、何も変えない
//! - `PUT`/`DELETE /trouble/tasks/{task_id}` — 来た項目だけ変える (担当・期限などは `""` / `null` で空にできる)。
//!   削除は物理削除 (204)。無いものは 404
//!
//! ファイルの口 (`/trouble/tasks/{task_id}/files`・`/trouble/task-files/...`) は R2 が要るので files と一緒に
//! 後続の PR で移す。
//!
//! 一覧の SQL は WHERE・ORDER BY を組み立てるが、文に入るのは固定の断片と `$n` だけ (利用者の入力は全部 bind。
//! 並びの列は許可リスト [`SortBy`] からだけ選ぶ)。

use std::collections::HashSet;

use alc_core_wasm::TenantId;
use alc_worker_db::{PgClient, TxOutput};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, put};
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::tickets::{empty_string_as_clear, empty_string_as_none};
use crate::AppState;

/// rust の `TroubleTask` と同じ形・同じキーの順
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Task {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub ticket_id: Uuid,
    pub task_type: String,
    pub title: String,
    pub description: String,
    pub status: String,
    pub assigned_to: Option<Uuid>,
    pub due_date: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub sort_order: i32,
    pub next_action: String,
    pub next_action_detail: String,
    pub next_action_by: Option<String>,
    pub next_action_due: Option<DateTime<Utc>>,
    pub occurred_at: Option<DateTime<Utc>>,
    pub print_page_break_before: bool,
    pub created_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl TxOutput for Task {}

/// rust の `CreateTroubleTask`
#[derive(Debug, Deserialize)]
pub struct CreateTask {
    pub task_type: String,
    pub title: String,
    #[serde(default)]
    pub description: String,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub assigned_to: Option<Uuid>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub due_date: Option<DateTime<Utc>>,
    #[serde(default)]
    pub sort_order: Option<i32>,
    #[serde(default)]
    pub next_action: Option<String>,
    #[serde(default)]
    pub next_action_detail: Option<String>,
    #[serde(default)]
    pub next_action_by: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub next_action_due: Option<DateTime<Utc>>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub occurred_at: Option<DateTime<Utc>>,
}

/// rust の `UpdateTroubleTask`。`Option<Option<_>>` の項目は、来ない → 変えない、`""` / `null` → 空にする
#[derive(Debug, Deserialize)]
pub struct UpdateTask {
    #[serde(default)]
    pub task_type: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_clear_of")]
    pub assigned_to: Option<Option<Uuid>>,
    #[serde(default, deserialize_with = "empty_string_as_clear_of")]
    pub due_date: Option<Option<DateTime<Utc>>>,
    #[serde(default, deserialize_with = "empty_string_as_clear_of")]
    pub completed_at: Option<Option<DateTime<Utc>>>,
    #[serde(default)]
    pub sort_order: Option<i32>,
    #[serde(default)]
    pub next_action: Option<String>,
    #[serde(default)]
    pub next_action_detail: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_clear")]
    pub next_action_by: Option<Option<String>>,
    #[serde(default, deserialize_with = "empty_string_as_clear_of")]
    pub next_action_due: Option<Option<DateTime<Utc>>>,
    #[serde(default, deserialize_with = "empty_string_as_clear_of")]
    pub occurred_at: Option<Option<DateTime<Utc>>>,
    #[serde(default)]
    pub print_page_break_before: Option<bool>,
}

/// rust の `ReorderTroubleTasks`
#[derive(Debug, Deserialize)]
pub struct ReorderTasks {
    pub task_ids: Vec<Uuid>,
}

/// 一覧の絞り込み (rust の `TroubleTasksFilter`)
#[derive(Debug, Default)]
pub struct TaskFilter {
    pub ticket_id: Option<Uuid>,
    pub status: Option<String>,
    pub task_type: Option<String>,
    pub assigned_to: Option<Uuid>,
    pub q: Option<String>,
    pub due_from: Option<DateTime<Utc>>,
    pub due_to: Option<DateTime<Utc>>,
    pub occurred_from: Option<DateTime<Utc>>,
    pub occurred_to: Option<DateTime<Utc>>,
}

/// rust の `ListTasksQuery` (query string。`#[serde(flatten)]` は数値・bool が読めなくなるので使わない)
#[derive(Debug, Default, Deserialize)]
pub struct ListTasksQuery {
    pub ticket_id: Option<Uuid>,
    pub status: Option<String>,
    pub task_type: Option<String>,
    pub assigned_to: Option<Uuid>,
    pub q: Option<String>,
    pub due_from: Option<DateTime<Utc>>,
    pub due_to: Option<DateTime<Utc>>,
    pub occurred_from: Option<DateTime<Utc>>,
    pub occurred_to: Option<DateTime<Utc>>,
    pub sort_by: Option<String>,
    pub sort_desc: Option<bool>,
    pub page: Option<i64>,
    pub per_page: Option<i64>,
}

/// rust の `ListTasksResponse`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ListTasksResponse {
    pub items: Vec<Task>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
}

/// 来ない → None、`""` / `null` → Some(None) (空にする)、値 → Some(Some) (rust の
/// `serde_helpers::empty_string_as_none_option_uuid` / `_datetime`)
fn empty_string_as_clear_of<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: serde::de::DeserializeOwned,
{
    match Value::deserialize(deserializer)? {
        Value::Null => Ok(Some(None)),
        Value::String(s) if s.is_empty() => Ok(Some(None)),
        v => T::deserialize(v)
            .map(|t| Some(Some(t)))
            .map_err(serde::de::Error::custom),
    }
}

/// 一覧の並びの列 (rust の `TroubleTasksSortBy`。許可リスト)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortBy {
    CreatedAt,
    OccurredAt,
    DueDate,
    NextActionDue,
    Status,
}

impl SortBy {
    /// 無し → `created_at`。許可リストに無い値は None (呼び手は 400)
    pub fn parse(s: Option<&str>) -> Option<Self> {
        match s.unwrap_or("created_at") {
            "created_at" => Some(Self::CreatedAt),
            "occurred_at" => Some(Self::OccurredAt),
            "due_date" => Some(Self::DueDate),
            "next_action_due" => Some(Self::NextActionDue),
            "status" => Some(Self::Status),
            _ => None,
        }
    }

    fn column(self) -> &'static str {
        match self {
            Self::CreatedAt => "created_at",
            Self::OccurredAt => "occurred_at",
            Self::DueDate => "due_date",
            Self::NextActionDue => "next_action_due",
            Self::Status => "status",
        }
    }
}

/// 並び (rust と同じく `{列} {向き} NULLS LAST, created_at DESC`)
pub fn order_sql(sort_by: SortBy, sort_desc: bool) -> String {
    let direction = if sort_desc { "DESC" } else { "ASC" };
    format!(
        "{} {direction} NULLS LAST, created_at DESC",
        sort_by.column()
    )
}

/// 返す列 (20 本)
macro_rules! columns {
    () => {
        "id, tenant_id, ticket_id, task_type, title, description, status, \
         assigned_to, due_date, completed_at, sort_order, \
         next_action, next_action_detail, next_action_by, next_action_due, \
         occurred_at, print_page_break_before, \
         created_by, created_at, updated_at"
    };
}

pub mod sql {
    pub const COLUMNS: &str = columns!();

    pub const INSERT: &str = concat!(
        "INSERT INTO alc_api.trouble_tasks (\
         tenant_id, ticket_id, task_type, title, description, assigned_to, due_date, sort_order, \
         created_by, next_action, next_action_detail, next_action_by, next_action_due, occurred_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, \
         COALESCE($8, (SELECT COALESCE(MAX(sort_order) + 1, 0) FROM alc_api.trouble_tasks \
         WHERE tenant_id = $1 AND ticket_id = $2)), \
         $9, COALESCE($10, ''), COALESCE($11, ''), $12, $13, $14) \
         RETURNING ",
        columns!()
    );

    /// 作成の前に、チケットがこの tenant のものか確かめる (外部キーの検査は RLS を通らないので、
    /// これが無いと別の tenant のチケットにタスクを付けられる)
    pub const TICKET_EXISTS: &str =
        "SELECT 1 FROM alc_api.trouble_tickets WHERE id = $1 AND tenant_id = $2";

    pub const LIST_BY_TICKET: &str = concat!(
        "SELECT ",
        columns!(),
        " FROM alc_api.trouble_tasks WHERE ticket_id = $1 AND tenant_id = $2 \
         ORDER BY sort_order, created_at"
    );

    pub const UPDATE: &str = concat!(
        "UPDATE alc_api.trouble_tasks SET \
         task_type = COALESCE($3, task_type), \
         title = COALESCE($4, title), \
         description = COALESCE($5, description), \
         status = COALESCE($6, status), \
         assigned_to = CASE WHEN $7 THEN $8 ELSE assigned_to END, \
         due_date = CASE WHEN $9 THEN $10 ELSE due_date END, \
         completed_at = CASE WHEN $11 THEN $12 ELSE completed_at END, \
         sort_order = COALESCE($13, sort_order), \
         next_action = COALESCE($14, next_action), \
         next_action_detail = COALESCE($15, next_action_detail), \
         next_action_by = CASE WHEN $16 THEN $17 ELSE next_action_by END, \
         next_action_due = CASE WHEN $18 THEN $19 ELSE next_action_due END, \
         occurred_at = CASE WHEN $20 THEN $21 ELSE occurred_at END, \
         print_page_break_before = COALESCE($22, print_page_break_before), \
         updated_at = now() \
         WHERE id = $1 AND tenant_id = $2 \
         RETURNING ",
        columns!()
    );

    pub const DELETE: &str = "DELETE FROM alc_api.trouble_tasks WHERE id = $1 AND tenant_id = $2";

    /// 並べ替える行を数え、終わるまで動かせないよう押さえる (そのチケットに無い id は数えない)
    pub const REORDER_LOCK: &str = "SELECT id FROM alc_api.trouble_tasks \
         WHERE id = ANY($3) AND ticket_id = $1 AND tenant_id = $2 FOR UPDATE";

    /// 渡された順 (ORDINALITY) をそのまま 0 起点の sort_order にする (rust と同じ 1 文)
    pub const REORDER: &str = "UPDATE alc_api.trouble_tasks t \
         SET sort_order = (v.ord - 1)::INT, updated_at = now() \
         FROM unnest($3::uuid[]) WITH ORDINALITY AS v(id, ord) \
         WHERE t.id = v.id AND t.ticket_id = $1 AND t.tenant_id = $2";
}

/// 一覧の WHERE と、その `$n` に入れる値 (`$1` は tenant_id)。順は rust と同じ
/// (ticket_id → status → task_type → assigned_to → q → due_from → due_to → occurred_from → occurred_to)
fn where_clause<'a>(
    tenant_id: &'a Uuid,
    f: &'a TaskFilter,
) -> (String, Vec<(&'a (dyn ToSql + Sync), Type)>) {
    let mut clauses = vec!["tenant_id = $1".to_owned()];
    let mut params: Vec<(&(dyn ToSql + Sync), Type)> = vec![(tenant_id, Type::UUID)];
    let mut add = |clause: &dyn Fn(usize) -> String, value: &'a (dyn ToSql + Sync), ty: Type| {
        params.push((value, ty));
        clauses.push(clause(params.len()));
    };
    if let Some(v) = &f.ticket_id {
        add(&|i| format!("ticket_id = ${i}"), v, Type::UUID);
    }
    if let Some(v) = &f.status {
        add(&|i| format!("status = ${i}"), v, Type::TEXT);
    }
    if let Some(v) = &f.task_type {
        add(&|i| format!("task_type = ${i}"), v, Type::TEXT);
    }
    if let Some(v) = &f.assigned_to {
        add(&|i| format!("assigned_to = ${i}"), v, Type::UUID);
    }
    if let Some(v) = &f.q {
        add(
            &|i| {
                format!("(title ILIKE '%' || ${i} || '%' OR description ILIKE '%' || ${i} || '%')")
            },
            v,
            Type::TEXT,
        );
    }
    if let Some(v) = &f.due_from {
        add(&|i| format!("due_date >= ${i}"), v, Type::TIMESTAMPTZ);
    }
    if let Some(v) = &f.due_to {
        add(&|i| format!("due_date <= ${i}"), v, Type::TIMESTAMPTZ);
    }
    if let Some(v) = &f.occurred_from {
        add(&|i| format!("occurred_at >= ${i}"), v, Type::TIMESTAMPTZ);
    }
    if let Some(v) = &f.occurred_to {
        add(&|i| format!("occurred_at <= ${i}"), v, Type::TIMESTAMPTZ);
    }
    (clauses.join(" AND "), params)
}

fn task(r: &Row) -> Task {
    Task {
        id: r.get(0),
        tenant_id: r.get(1),
        ticket_id: r.get(2),
        task_type: r.get(3),
        title: r.get(4),
        description: r.get(5),
        status: r.get(6),
        assigned_to: r.get(7),
        due_date: r.get(8),
        completed_at: r.get(9),
        sort_order: r.get(10),
        next_action: r.get(11),
        next_action_detail: r.get(12),
        next_action_by: r.get(13),
        next_action_due: r.get(14),
        occurred_at: r.get(15),
        print_page_break_before: r.get(16),
        created_by: r.get(17),
        created_at: r.get(18),
        updated_at: r.get(19),
    }
}

/// 作成。チケットがこの tenant に無ければ (無い・別の tenant) INSERT せずに None
pub async fn create(
    pg: &mut PgClient,
    tenant_id: Uuid,
    ticket_id: Uuid,
    input: CreateTask,
) -> Result<Option<Task>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let ticket = tx
                .query_typed_opt(
                    sql::TICKET_EXISTS,
                    &[(&ticket_id, Type::UUID), (&tenant_id, Type::UUID)],
                )
                .await?;
            if ticket.is_none() {
                return Ok(None);
            }
            let i = &input;
            let row = tx
                .query_typed_one(
                    sql::INSERT,
                    &[
                        (&tenant_id, Type::UUID),
                        (&ticket_id, Type::UUID),
                        (&i.task_type, Type::TEXT),
                        (&i.title, Type::TEXT),
                        (&i.description, Type::TEXT),
                        (&i.assigned_to, Type::UUID),
                        (&i.due_date, Type::TIMESTAMPTZ),
                        (&i.sort_order, Type::INT4),
                        (&None::<Uuid>, Type::UUID),
                        (&i.next_action, Type::TEXT),
                        (&i.next_action_detail, Type::TEXT),
                        (&i.next_action_by, Type::TEXT),
                        (&i.next_action_due, Type::TIMESTAMPTZ),
                        (&i.occurred_at, Type::TIMESTAMPTZ),
                    ],
                )
                .await?;
            Ok(Some(task(&row)))
        })
    })
    .await
}

pub async fn list_by_ticket(
    pg: &mut PgClient,
    tenant_id: Uuid,
    ticket_id: Uuid,
) -> Result<Vec<Task>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(
                    sql::LIST_BY_TICKET,
                    &[(&ticket_id, Type::UUID), (&tenant_id, Type::UUID)],
                )
                .await?;
            Ok(rows.iter().map(task).collect())
        })
    })
    .await
}

/// 1 ページ分と、絞り込みに合う件数 (COUNT と一覧を 1 つの `tenant_tx` で)
pub async fn list_all(
    pg: &mut PgClient,
    tenant_id: Uuid,
    filter: TaskFilter,
    sort_by: SortBy,
    sort_desc: bool,
    limit: i64,
    offset: i64,
) -> Result<(Vec<Task>, i64), Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let (where_sql, mut params) = where_clause(&tenant_id, &filter);
            let count_sql = format!("SELECT COUNT(*) FROM alc_api.trouble_tasks WHERE {where_sql}");
            let total: i64 = tx.query_typed_one(&count_sql, &params).await?.get(0);
            let n = params.len();
            let list_sql = format!(
                "SELECT {} FROM alc_api.trouble_tasks WHERE {where_sql} ORDER BY {} LIMIT ${} OFFSET ${}",
                sql::COLUMNS,
                order_sql(sort_by, sort_desc),
                n + 1,
                n + 2
            );
            params.push((&limit, Type::INT8));
            params.push((&offset, Type::INT8));
            let rows = tx.query_typed(&list_sql, &params).await?;
            Ok((rows.iter().map(task).collect(), total))
        })
    })
    .await
}

pub async fn update(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
    input: UpdateTask,
) -> Result<Option<Task>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let i = &input;
            let assigned_to = i.assigned_to.flatten();
            let due_date = i.due_date.flatten();
            let completed_at = i.completed_at.flatten();
            let next_action_by = i.next_action_by.clone().flatten();
            let next_action_due = i.next_action_due.flatten();
            let occurred_at = i.occurred_at.flatten();
            let flags = [
                i.assigned_to.is_some(),
                i.due_date.is_some(),
                i.completed_at.is_some(),
                i.next_action_by.is_some(),
                i.next_action_due.is_some(),
                i.occurred_at.is_some(),
            ];
            let row = tx
                .query_typed_opt(
                    sql::UPDATE,
                    &[
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                        (&i.task_type, Type::TEXT),
                        (&i.title, Type::TEXT),
                        (&i.description, Type::TEXT),
                        (&i.status, Type::TEXT),
                        (&flags[0], Type::BOOL),
                        (&assigned_to, Type::UUID),
                        (&flags[1], Type::BOOL),
                        (&due_date, Type::TIMESTAMPTZ),
                        (&flags[2], Type::BOOL),
                        (&completed_at, Type::TIMESTAMPTZ),
                        (&i.sort_order, Type::INT4),
                        (&i.next_action, Type::TEXT),
                        (&i.next_action_detail, Type::TEXT),
                        (&flags[3], Type::BOOL),
                        (&next_action_by, Type::TEXT),
                        (&flags[4], Type::BOOL),
                        (&next_action_due, Type::TIMESTAMPTZ),
                        (&flags[5], Type::BOOL),
                        (&occurred_at, Type::TIMESTAMPTZ),
                        (&i.print_page_break_before, Type::BOOL),
                    ],
                )
                .await?;
            Ok(row.as_ref().map(task))
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

/// `task_ids` の順に 0 から振り直し、そのチケットの一覧を返す (全部を 1 つの `tenant_tx` で)。
/// そのチケットに無い id (別のチケット・別の tenant・無い id) が 1 つでも在れば何も変えずに None。
/// rust は更新した行数が足りなければ巻き戻すが、ここでは先に `FOR UPDATE` で数えて揃わなければ更新しない
/// (押さえた行は transaction が終わるまで消せない・動かせないので、更新の行数は数えた数と同じになる)。
/// 重複の id は呼び手が先に 400 にする
pub async fn reorder(
    pg: &mut PgClient,
    tenant_id: Uuid,
    ticket_id: Uuid,
    task_ids: Vec<Uuid>,
) -> Result<Option<Vec<Task>>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let params: [(&(dyn ToSql + Sync), Type); 3] = [
                (&ticket_id, Type::UUID),
                (&tenant_id, Type::UUID),
                (&task_ids, Type::UUID_ARRAY),
            ];
            let found = tx.query_typed(sql::REORDER_LOCK, &params).await?;
            if found.len() != task_ids.len() {
                return Ok(None);
            }
            tx.execute_typed(sql::REORDER, &params).await?;
            let rows = tx.query_typed(sql::LIST_BY_TICKET, &params[..2]).await?;
            Ok(Some(rows.iter().map(task).collect()))
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/trouble/tasks", get(list_all_handler))
        .route(
            "/trouble/tickets/{id}/tasks",
            get(list_handler).post(create_handler),
        )
        .route("/trouble/tickets/{id}/tasks/reorder", put(reorder_handler))
        .route(
            "/trouble/tasks/{task_id}",
            put(update_handler).delete(delete_handler),
        )
}

async fn list_all_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(query): Query<ListTasksQuery>,
) -> Result<Json<ListTasksResponse>, StatusCode> {
    let sort_by = SortBy::parse(query.sort_by.as_deref()).ok_or(StatusCode::BAD_REQUEST)?;
    let sort_desc = query.sort_desc.unwrap_or(true);
    let per_page = query.per_page.unwrap_or(50).clamp(1, 200);
    let page = query.page.unwrap_or(1).max(1);
    let offset = (page - 1) * per_page;
    let filter = TaskFilter {
        ticket_id: query.ticket_id,
        status: query.status,
        task_type: query.task_type,
        assigned_to: query.assigned_to,
        q: query.q,
        due_from: query.due_from,
        due_to: query.due_to,
        occurred_from: query.occurred_from,
        occurred_to: query.occurred_to,
    };
    let mut pg = state.pg.lock().await;
    let (items, total) = list_all(
        &mut pg, tenant_id, filter, sort_by, sort_desc, per_page, offset,
    )
    .await
    .map_err(|e| state.db_fail("task list all", &e))?;
    Ok(Json(ListTasksResponse {
        items,
        total,
        page,
        per_page,
    }))
}

async fn create_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(ticket_id): Path<Uuid>,
    Json(body): Json<CreateTask>,
) -> Result<(StatusCode, Json<Task>), StatusCode> {
    if body.title.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut pg = state.pg.lock().await;
    let task = create(&mut pg, tenant_id, ticket_id, body)
        .await
        .map_err(|e| state.db_fail("task create", &e))?
        .ok_or(StatusCode::NOT_FOUND)?;
    // TODO(#747 T5): LINE WORKS 通知
    Ok((StatusCode::CREATED, Json(task)))
}

async fn list_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(ticket_id): Path<Uuid>,
) -> Result<Json<Vec<Task>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    list_by_ticket(&mut pg, tenant_id, ticket_id)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("task list", &e))
}

async fn reorder_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(ticket_id): Path<Uuid>,
    Json(body): Json<ReorderTasks>,
) -> Result<Json<Vec<Task>>, StatusCode> {
    let unique: HashSet<&Uuid> = body.task_ids.iter().collect();
    if body.task_ids.is_empty() || unique.len() != body.task_ids.len() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut pg = state.pg.lock().await;
    reorder(&mut pg, tenant_id, ticket_id, body.task_ids)
        .await
        .map_err(|e| state.db_fail("task reorder", &e))?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn update_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(task_id): Path<Uuid>,
    Json(body): Json<UpdateTask>,
) -> Result<Json<Task>, StatusCode> {
    let mut pg = state.pg.lock().await;
    let task = update(&mut pg, tenant_id, task_id, body)
        .await
        .map_err(|e| state.db_fail("task update", &e))?
        .ok_or(StatusCode::NOT_FOUND)?;
    // TODO(#747 T5): LINE WORKS 通知
    Ok(Json(task))
}

async fn delete_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(task_id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    let found = delete(&mut pg, tenant_id, task_id)
        .await
        .map_err(|e| state.db_fail("task delete", &e))?;
    if found {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sort_is_from_the_allow_list() {
        assert_eq!(SortBy::parse(None), Some(SortBy::CreatedAt));
        for (s, by) in [
            ("created_at", SortBy::CreatedAt),
            ("occurred_at", SortBy::OccurredAt),
            ("due_date", SortBy::DueDate),
            ("next_action_due", SortBy::NextActionDue),
            ("status", SortBy::Status),
        ] {
            assert_eq!(SortBy::parse(Some(s)), Some(by));
            assert_eq!(
                order_sql(by, false),
                format!("{s} ASC NULLS LAST, created_at DESC")
            );
        }
        assert_eq!(
            order_sql(SortBy::DueDate, true),
            "due_date DESC NULLS LAST, created_at DESC"
        );
        // 許可リストに無い値は文に入らない (呼び手は 400)
        assert_eq!(SortBy::parse(Some("id; DROP TABLE x")), None);
    }

    #[test]
    fn where_numbers_params_in_rust_order() {
        let tenant = Uuid::nil();
        let t = DateTime::<Utc>::default();
        let f = TaskFilter {
            ticket_id: Some(Uuid::nil()),
            status: Some("open".into()),
            task_type: Some("連絡".into()),
            assigned_to: Some(Uuid::nil()),
            q: Some("'; --".into()),
            due_from: Some(t),
            due_to: Some(t),
            occurred_from: Some(t),
            occurred_to: Some(t),
        };
        let (sql, params) = where_clause(&tenant, &f);
        let types: Vec<Type> = params.iter().map(|p| p.1.clone()).collect();
        assert_eq!(
            types,
            vec![
                Type::UUID,
                Type::UUID,
                Type::TEXT,
                Type::TEXT,
                Type::UUID,
                Type::TEXT,
                Type::TIMESTAMPTZ,
                Type::TIMESTAMPTZ,
                Type::TIMESTAMPTZ,
                Type::TIMESTAMPTZ
            ]
        );
        assert_eq!(
            sql,
            "tenant_id = $1 AND ticket_id = $2 AND status = $3 AND task_type = $4 AND \
             assigned_to = $5 AND (title ILIKE '%' || $6 || '%' OR description ILIKE '%' || $6 || '%') AND \
             due_date >= $7 AND due_date <= $8 AND occurred_at >= $9 AND occurred_at <= $10"
        );
        let empty = TaskFilter::default();
        let (sql, params) = where_clause(&tenant, &empty);
        assert_eq!((sql.as_str(), params.len()), ("tenant_id = $1", 1));
    }

    #[test]
    fn update_clears_with_empty_or_null() {
        let u: UpdateTask = serde_json::from_value(json!({})).unwrap();
        assert!(u.assigned_to.is_none() && u.due_date.is_none() && u.next_action_by.is_none());
        let u: UpdateTask = serde_json::from_value(json!({
            "assigned_to": "", "due_date": null, "next_action_by": "", "occurred_at": ""
        }))
        .unwrap();
        assert_eq!(
            (u.assigned_to, u.due_date, u.next_action_by, u.occurred_at),
            (Some(None), Some(None), Some(None), Some(None))
        );
        let u: UpdateTask = serde_json::from_value(json!({
            "assigned_to": Uuid::nil(), "next_action_by": "青井 健",
            "completed_at": "2026-01-02T03:04:05Z"
        }))
        .unwrap();
        assert_eq!(u.assigned_to, Some(Some(Uuid::nil())));
        assert_eq!(u.next_action_by, Some(Some("青井 健".into())));
        assert!(matches!(u.completed_at, Some(Some(_))));
        assert!(serde_json::from_value::<UpdateTask>(json!({"assigned_to": "nope"})).is_err());
        let c: CreateTask = serde_json::from_value(json!({
            "task_type": "連絡", "title": "t", "assigned_to": "", "due_date": ""
        }))
        .unwrap();
        assert!(c.assigned_to.is_none() && c.due_date.is_none() && c.description.is_empty());
    }
}
