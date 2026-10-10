//! チケットの状態の流れ (rust の `alc-trouble` の workflow.rs と repo/trouble_workflow.rs の写し)。
//!
//! - `GET`/`POST /trouble/workflow/states` — 並びは `sort_order`。POST は 201、(tenant, name) の重複は 409
//! - `DELETE /trouble/workflow/states/{id}` — 204 / 404
//! - `GET`/`POST /trouble/workflow/transitions` — POST は 201 (重複・知らない状態も含めて DB のエラーは 500。rust と同じ)
//! - `DELETE /trouble/workflow/transitions/{id}` — 204 / 404
//! - `POST /trouble/workflow/setup` — 状態が 1 つも無ければ既定の 4 つと遷移 7 本を作る。在ればそのまま返す。
//!   数える・作るを 1 つの `tenant_tx` で行う
//! - `GET /trouble/tickets/{id}/history` — 状態の変更の履歴 (古い順)
//!
//! チケットの口 ([`crate::tickets`]) が使う [`get_initial_state`]・[`is_transition_allowed`] (複数の文を 1 つの
//! `tenant_tx` で)・[`record_history_or_log`] もここに置く。

use alc_core_wasm::TenantId;
use alc_worker_db::{PgClient, TxOutput};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::Type;
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::AppState;

/// rust の `TroubleWorkflowState` と同じ形・同じキーの順
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WorkflowState {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub name: String,
    pub label: String,
    pub color: String,
    pub sort_order: i32,
    pub is_initial: bool,
    pub is_terminal: bool,
    pub created_at: DateTime<Utc>,
}

impl TxOutput for WorkflowState {}

/// rust の `CreateWorkflowState`
#[derive(Debug, Deserialize)]
pub struct CreateWorkflowState {
    pub name: String,
    pub label: String,
    pub color: Option<String>,
    pub sort_order: Option<i32>,
    pub is_initial: Option<bool>,
    pub is_terminal: Option<bool>,
}

/// rust の `TroubleWorkflowTransition`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WorkflowTransition {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub from_state_id: Uuid,
    pub to_state_id: Uuid,
    pub label: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl TxOutput for WorkflowTransition {}

/// rust の `CreateWorkflowTransition`
#[derive(Debug, Deserialize)]
pub struct CreateWorkflowTransition {
    pub from_state_id: Uuid,
    pub to_state_id: Uuid,
    pub label: Option<String>,
}

/// rust の `TroubleStatusHistory`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StatusHistory {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub ticket_id: Uuid,
    pub from_state_id: Option<Uuid>,
    pub to_state_id: Uuid,
    pub changed_by: Option<Uuid>,
    pub comment: String,
    pub created_at: DateTime<Utc>,
}

impl TxOutput for StatusHistory {}

/// setup で作る状態 (name, label, color, sort_order, is_initial, is_terminal)。rust と同じ
pub const DEFAULT_STATES: &[(&str, &str, &str, i32, bool, bool)] = &[
    ("new", "新規", "#3B82F6", 1, true, false),
    ("in_progress", "対応中", "#F59E0B", 2, false, false),
    ("resolved", "解決", "#10B981", 3, false, false),
    ("closed", "完了", "#6B7280", 4, false, true),
];

/// setup で作る遷移 (from の添字, to の添字, label)。(0,3) / (1,3) で、どの非 terminal の状態からも
/// 完了へ直接行ける (migration 111 と同じ)。rust と同じ
pub const DEFAULT_TRANSITIONS: &[(usize, usize, &str)] = &[
    (0, 1, "対応開始"),
    (1, 2, "解決"),
    (2, 3, "完了"),
    (1, 0, "差し戻し"),
    (2, 1, "再対応"),
    (0, 3, "完了"),
    (1, 3, "完了"),
];

pub mod sql {
    pub const STATE_COLUMNS: &str =
        "id, tenant_id, name, label, color, sort_order, is_initial, is_terminal, created_at";

    pub const LIST_STATES: &str =
        "SELECT id, tenant_id, name, label, color, sort_order, is_initial, is_terminal, created_at \
         FROM alc_api.trouble_workflow_states WHERE tenant_id = $1 ORDER BY sort_order";

    pub const INSERT_STATE: &str = "INSERT INTO alc_api.trouble_workflow_states \
         (tenant_id, name, label, color, sort_order, is_initial, is_terminal) \
         VALUES ($1, $2, $3, COALESCE($4, '#6B7280'), COALESCE($5, 0), COALESCE($6, FALSE), COALESCE($7, FALSE)) \
         RETURNING id, tenant_id, name, label, color, sort_order, is_initial, is_terminal, created_at";

    pub const DELETE_STATE: &str =
        "DELETE FROM alc_api.trouble_workflow_states WHERE id = $1 AND tenant_id = $2";

    pub const LIST_TRANSITIONS: &str =
        "SELECT id, tenant_id, from_state_id, to_state_id, label, created_at \
         FROM alc_api.trouble_workflow_transitions WHERE tenant_id = $1";

    pub const INSERT_TRANSITION: &str = "INSERT INTO alc_api.trouble_workflow_transitions \
         (tenant_id, from_state_id, to_state_id, label) VALUES ($1, $2, $3, $4) \
         RETURNING id, tenant_id, from_state_id, to_state_id, label, created_at";

    pub const DELETE_TRANSITION: &str =
        "DELETE FROM alc_api.trouble_workflow_transitions WHERE id = $1 AND tenant_id = $2";

    pub const IS_INITIAL: &str =
        "SELECT is_initial FROM alc_api.trouble_workflow_states WHERE id = $1 AND tenant_id = $2";

    pub const TRANSITION_EXISTS: &str =
        "SELECT EXISTS(SELECT 1 FROM alc_api.trouble_workflow_transitions \
         WHERE tenant_id = $1 AND from_state_id = $2 AND to_state_id = $3)";

    pub const INITIAL_STATE: &str =
        "SELECT id, tenant_id, name, label, color, sort_order, is_initial, is_terminal, created_at \
         FROM alc_api.trouble_workflow_states WHERE tenant_id = $1 AND is_initial = TRUE LIMIT 1";

    pub const INSERT_HISTORY: &str = "INSERT INTO alc_api.trouble_status_history \
         (tenant_id, ticket_id, from_state_id, to_state_id, changed_by, comment) \
         VALUES ($1, $2, $3, $4, $5, COALESCE($6, ''))";

    pub const LIST_HISTORY: &str =
        "SELECT id, tenant_id, ticket_id, from_state_id, to_state_id, changed_by, comment, created_at \
         FROM alc_api.trouble_status_history WHERE ticket_id = $1 AND tenant_id = $2 ORDER BY created_at";

    pub const COUNT_STATES: &str =
        "SELECT COUNT(*) FROM alc_api.trouble_workflow_states WHERE tenant_id = $1";

    pub const INSERT_DEFAULT_STATE: &str = "INSERT INTO alc_api.trouble_workflow_states \
         (tenant_id, name, label, color, sort_order, is_initial, is_terminal) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) \
         RETURNING id, tenant_id, name, label, color, sort_order, is_initial, is_terminal, created_at";

    pub const INSERT_DEFAULT_TRANSITION: &str = "INSERT INTO alc_api.trouble_workflow_transitions \
         (tenant_id, from_state_id, to_state_id, label) VALUES ($1, $2, $3, $4)";
}

fn state_row(r: &Row) -> WorkflowState {
    WorkflowState {
        id: r.get(0),
        tenant_id: r.get(1),
        name: r.get(2),
        label: r.get(3),
        color: r.get(4),
        sort_order: r.get(5),
        is_initial: r.get(6),
        is_terminal: r.get(7),
        created_at: r.get(8),
    }
}

fn transition_row(r: &Row) -> WorkflowTransition {
    WorkflowTransition {
        id: r.get(0),
        tenant_id: r.get(1),
        from_state_id: r.get(2),
        to_state_id: r.get(3),
        label: r.get(4),
        created_at: r.get(5),
    }
}

fn history_row(r: &Row) -> StatusHistory {
    StatusHistory {
        id: r.get(0),
        tenant_id: r.get(1),
        ticket_id: r.get(2),
        from_state_id: r.get(3),
        to_state_id: r.get(4),
        changed_by: r.get(5),
        comment: r.get(6),
        created_at: r.get(7),
    }
}

pub async fn list_states(pg: &mut PgClient, tenant_id: Uuid) -> Result<Vec<WorkflowState>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::LIST_STATES, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(rows.iter().map(state_row).collect())
        })
    })
    .await
}

pub async fn create_state(
    pg: &mut PgClient,
    tenant_id: Uuid,
    input: CreateWorkflowState,
) -> Result<WorkflowState, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_one(
                    sql::INSERT_STATE,
                    &[
                        (&tenant_id, Type::UUID),
                        (&input.name, Type::TEXT),
                        (&input.label, Type::TEXT),
                        (&input.color, Type::TEXT),
                        (&input.sort_order, Type::INT4),
                        (&input.is_initial, Type::BOOL),
                        (&input.is_terminal, Type::BOOL),
                    ],
                )
                .await?;
            Ok(state_row(&row))
        })
    })
    .await
}

/// `sql` の `id = $1 AND tenant_id = $2` の DELETE を流し、消えたか
async fn delete_by_id(
    pg: &mut PgClient,
    sql: &'static str,
    tenant_id: Uuid,
    id: Uuid,
) -> Result<bool, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let n = tx
                .execute_typed(sql, &[(&id, Type::UUID), (&tenant_id, Type::UUID)])
                .await?;
            Ok(n > 0)
        })
    })
    .await
}

pub async fn list_transitions(
    pg: &mut PgClient,
    tenant_id: Uuid,
) -> Result<Vec<WorkflowTransition>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::LIST_TRANSITIONS, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(rows.iter().map(transition_row).collect())
        })
    })
    .await
}

pub async fn create_transition(
    pg: &mut PgClient,
    tenant_id: Uuid,
    input: CreateWorkflowTransition,
) -> Result<WorkflowTransition, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_one(
                    sql::INSERT_TRANSITION,
                    &[
                        (&tenant_id, Type::UUID),
                        (&input.from_state_id, Type::UUID),
                        (&input.to_state_id, Type::UUID),
                        (&input.label, Type::TEXT),
                    ],
                )
                .await?;
            Ok(transition_row(&row))
        })
    })
    .await
}

/// `from` から `to` へ移れるか。`from` が無い (状態の無いチケット) なら `to` が初期の状態かどうか。
/// 2 通りの文を 1 つの `tenant_tx` で流す
pub async fn is_transition_allowed(
    pg: &mut PgClient,
    tenant_id: Uuid,
    from_state_id: Option<Uuid>,
    to_state_id: Uuid,
) -> Result<bool, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let Some(from_state_id) = from_state_id else {
                let row = tx
                    .query_typed_opt(
                        sql::IS_INITIAL,
                        &[(&to_state_id, Type::UUID), (&tenant_id, Type::UUID)],
                    )
                    .await?;
                return Ok(row.is_some_and(|r| r.get::<_, bool>(0)));
            };
            let row = tx
                .query_typed_one(
                    sql::TRANSITION_EXISTS,
                    &[
                        (&tenant_id, Type::UUID),
                        (&from_state_id, Type::UUID),
                        (&to_state_id, Type::UUID),
                    ],
                )
                .await?;
            Ok(row.get::<_, bool>(0))
        })
    })
    .await
}

pub async fn get_initial_state(
    pg: &mut PgClient,
    tenant_id: Uuid,
) -> Result<Option<WorkflowState>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(sql::INITIAL_STATE, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(row.as_ref().map(state_row))
        })
    })
    .await
}

/// 状態の変更の履歴を 1 行足す。rust と同じく失敗してもチケットの口は失敗にしない (ここでは固定の語でログに出す)
pub async fn record_history_or_log(
    state: &AppState,
    pg: &mut PgClient,
    tenant_id: Uuid,
    ticket_id: Uuid,
    from_state_id: Option<Uuid>,
    to_state_id: Uuid,
    comment: Option<String>,
) {
    let result = pg
        .tenant_tx(tenant_id, move |tx| {
            Box::pin(async move {
                tx.execute_typed(
                    sql::INSERT_HISTORY,
                    &[
                        (&tenant_id, Type::UUID),
                        (&ticket_id, Type::UUID),
                        (&from_state_id, Type::UUID),
                        (&to_state_id, Type::UUID),
                        (&None::<Uuid>, Type::UUID),
                        (&comment, Type::TEXT),
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await;
    if let Err(e) = result {
        state.db_fail("ticket history", &e);
    }
}

pub async fn list_history(
    pg: &mut PgClient,
    tenant_id: Uuid,
    ticket_id: Uuid,
) -> Result<Vec<StatusHistory>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(
                    sql::LIST_HISTORY,
                    &[(&ticket_id, Type::UUID), (&tenant_id, Type::UUID)],
                )
                .await?;
            Ok(rows.iter().map(history_row).collect())
        })
    })
    .await
}

/// 状態が 1 つも無ければ [`DEFAULT_STATES`] と [`DEFAULT_TRANSITIONS`] を作る。在ればいまの状態を返す。
/// 数える・作るを 1 つの `tenant_tx` で行う (途中で失敗すれば何も残らない)
pub async fn setup_defaults(
    pg: &mut PgClient,
    tenant_id: Uuid,
) -> Result<Vec<WorkflowState>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let existing: i64 = tx
                .query_typed_one(sql::COUNT_STATES, &[(&tenant_id, Type::UUID)])
                .await?
                .get(0);
            if existing > 0 {
                let rows = tx
                    .query_typed(sql::LIST_STATES, &[(&tenant_id, Type::UUID)])
                    .await?;
                return Ok(rows.iter().map(state_row).collect());
            }
            let mut states = Vec::new();
            for (name, label, color, order, initial, terminal) in DEFAULT_STATES {
                let row = tx
                    .query_typed_one(
                        sql::INSERT_DEFAULT_STATE,
                        &[
                            (&tenant_id, Type::UUID),
                            (name, Type::TEXT),
                            (label, Type::TEXT),
                            (color, Type::TEXT),
                            (order, Type::INT4),
                            (initial, Type::BOOL),
                            (terminal, Type::BOOL),
                        ],
                    )
                    .await?;
                states.push(state_row(&row));
            }
            for (from, to, label) in DEFAULT_TRANSITIONS {
                tx.execute_typed(
                    sql::INSERT_DEFAULT_TRANSITION,
                    &[
                        (&tenant_id, Type::UUID),
                        (&states[*from].id, Type::UUID),
                        (&states[*to].id, Type::UUID),
                        (label, Type::TEXT),
                    ],
                )
                .await?;
            }
            Ok(states)
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/trouble/workflow/states",
            get(list_states_handler).post(create_state_handler),
        )
        .route(
            "/trouble/workflow/states/{id}",
            delete(delete_state_handler),
        )
        .route(
            "/trouble/workflow/transitions",
            get(list_transitions_handler).post(create_transition_handler),
        )
        .route(
            "/trouble/workflow/transitions/{id}",
            delete(delete_transition_handler),
        )
        .route("/trouble/workflow/setup", post(setup_handler))
        .route("/trouble/tickets/{id}/history", get(history_handler))
}

async fn list_states_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Vec<WorkflowState>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    list_states(&mut pg, tenant_id)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("workflow state list", &e))
}

async fn create_state_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<CreateWorkflowState>,
) -> Result<(StatusCode, Json<WorkflowState>), StatusCode> {
    let mut pg = state.pg.lock().await;
    let ws = create_state(&mut pg, tenant_id, body).await.map_err(|e| {
        // rust は「duplicate」を含むエラー (一意制約の違反) だけを 409 にする
        if e.code() == Some(&SqlState::UNIQUE_VIOLATION) {
            return StatusCode::CONFLICT;
        }
        state.db_fail("workflow state create", &e)
    })?;
    Ok((StatusCode::CREATED, Json(ws)))
}

fn deleted(found: bool) -> Result<StatusCode, StatusCode> {
    if found {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

async fn delete_state_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    let found = delete_by_id(&mut pg, sql::DELETE_STATE, tenant_id, id)
        .await
        .map_err(|e| state.db_fail("workflow state delete", &e))?;
    deleted(found)
}

async fn list_transitions_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Vec<WorkflowTransition>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    list_transitions(&mut pg, tenant_id)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("workflow transition list", &e))
}

async fn create_transition_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<CreateWorkflowTransition>,
) -> Result<(StatusCode, Json<WorkflowTransition>), StatusCode> {
    let mut pg = state.pg.lock().await;
    let tr = create_transition(&mut pg, tenant_id, body)
        .await
        .map_err(|e| state.db_fail("workflow transition create", &e))?;
    Ok((StatusCode::CREATED, Json(tr)))
}

async fn delete_transition_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    let found = delete_by_id(&mut pg, sql::DELETE_TRANSITION, tenant_id, id)
        .await
        .map_err(|e| state.db_fail("workflow transition delete", &e))?;
    deleted(found)
}

async fn setup_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Vec<WorkflowState>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    setup_defaults(&mut pg, tenant_id)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("workflow setup", &e))
}

async fn history_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(ticket_id): Path<Uuid>,
) -> Result<Json<Vec<StatusHistory>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    list_history(&mut pg, tenant_id, ticket_id)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("ticket history list", &e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_transitions_point_at_default_states() {
        assert_eq!(DEFAULT_STATES.len(), 4);
        assert_eq!(DEFAULT_TRANSITIONS.len(), 7);
        for (from, to, _) in DEFAULT_TRANSITIONS {
            assert!(*from < DEFAULT_STATES.len() && *to < DEFAULT_STATES.len());
            assert_ne!(from, to);
        }
        // 初期は 1 つ・terminal は 1 つ
        assert_eq!(DEFAULT_STATES.iter().filter(|s| s.4).count(), 1);
        assert_eq!(DEFAULT_STATES.iter().filter(|s| s.5).count(), 1);
    }

    #[test]
    fn state_columns_match_the_statements() {
        for s in [sql::LIST_STATES, sql::INITIAL_STATE] {
            assert!(s.starts_with(&format!("SELECT {} FROM", sql::STATE_COLUMNS)));
        }
        for s in [sql::INSERT_STATE, sql::INSERT_DEFAULT_STATE] {
            assert!(s.ends_with(&format!("RETURNING {}", sql::STATE_COLUMNS)));
        }
    }
}
