//! 通知の設定 `/api/trouble/notification-prefs` (rust の `alc-trouble` の notifications.rs と
//! repo/trouble_notification_prefs.rs の写し)。
//!
//! - `POST` — (event_type, notify_channel) ごとに 1 行。在れば来た項目だけ変える。201。
//!   event_type が [`VALID_EVENT_TYPES`] に無い・notify_channel が [`VALID_CHANNELS`] に無いなら 400
//! - `GET` — 並びは `event_type, notify_channel`
//! - `DELETE /{id}` — 204 / 404

use alc_core_wasm::TenantId;
use alc_worker_db::{PgClient, TxOutput};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio_postgres::types::Type;
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::AppState;

pub const PATH: &str = "/trouble/notification-prefs";

pub const VALID_EVENT_TYPES: &[&str] = &[
    "trouble_created",
    "trouble_status_changed",
    "trouble_assigned",
    "task_assigned",
    "task_due_reminder",
];

pub const VALID_CHANNELS: &[&str] = &["lineworks"];

/// rust の `TroubleNotificationPref` と同じ形・同じキーの順
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct NotificationPref {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub event_type: String,
    pub notify_channel: String,
    pub enabled: bool,
    pub recipient_ids: Vec<Uuid>,
    pub notify_admins: bool,
    pub lineworks_user_ids: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl TxOutput for NotificationPref {}

/// rust の `UpsertNotificationPref`
#[derive(Debug, Deserialize)]
pub struct UpsertNotificationPref {
    pub event_type: String,
    pub notify_channel: String,
    pub enabled: Option<bool>,
    pub recipient_ids: Option<Vec<Uuid>>,
    pub notify_admins: Option<bool>,
    pub lineworks_user_ids: Option<Vec<String>>,
}

pub mod sql {
    pub const UPSERT: &str = "INSERT INTO alc_api.trouble_notification_prefs \
         (tenant_id, event_type, notify_channel, enabled, recipient_ids, notify_admins, lineworks_user_ids) \
         VALUES ($1, $2, $3, COALESCE($4, TRUE), COALESCE($5, '{}'), COALESCE($6, FALSE), COALESCE($7, '{}')) \
         ON CONFLICT (tenant_id, event_type, notify_channel) DO UPDATE SET \
         enabled = COALESCE($4, trouble_notification_prefs.enabled), \
         recipient_ids = COALESCE($5, trouble_notification_prefs.recipient_ids), \
         notify_admins = COALESCE($6, trouble_notification_prefs.notify_admins), \
         lineworks_user_ids = COALESCE($7, trouble_notification_prefs.lineworks_user_ids), \
         updated_at = NOW() \
         RETURNING id, tenant_id, event_type, notify_channel, enabled, recipient_ids, notify_admins, \
         lineworks_user_ids, created_at, updated_at";

    pub const LIST: &str = "SELECT id, tenant_id, event_type, notify_channel, enabled, recipient_ids, notify_admins, \
         lineworks_user_ids, created_at, updated_at \
         FROM alc_api.trouble_notification_prefs WHERE tenant_id = $1 ORDER BY event_type, notify_channel";

    pub const DELETE: &str =
        "DELETE FROM alc_api.trouble_notification_prefs WHERE id = $1 AND tenant_id = $2";

    /// rust の find_enabled (通知の前段)。使う列は宛先だけ
    pub const FIND_ENABLED: &str =
        "SELECT lineworks_user_ids FROM alc_api.trouble_notification_prefs \
         WHERE tenant_id = $1 AND event_type = $2 AND notify_channel = $3 AND enabled = TRUE";
}

fn pref(r: &Row) -> NotificationPref {
    NotificationPref {
        id: r.get(0),
        tenant_id: r.get(1),
        event_type: r.get(2),
        notify_channel: r.get(3),
        enabled: r.get(4),
        recipient_ids: r.get(5),
        notify_admins: r.get(6),
        lineworks_user_ids: r.get(7),
        created_at: r.get(8),
        updated_at: r.get(9),
    }
}

pub async fn upsert(
    pg: &mut PgClient,
    tenant_id: Uuid,
    input: UpsertNotificationPref,
) -> Result<NotificationPref, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_one(
                    sql::UPSERT,
                    &[
                        (&tenant_id, Type::UUID),
                        (&input.event_type, Type::TEXT),
                        (&input.notify_channel, Type::TEXT),
                        (&input.enabled, Type::BOOL),
                        (&input.recipient_ids, Type::UUID_ARRAY),
                        (&input.notify_admins, Type::BOOL),
                        (&input.lineworks_user_ids, Type::TEXT_ARRAY),
                    ],
                )
                .await?;
            Ok(pref(&row))
        })
    })
    .await
}

pub async fn list(pg: &mut PgClient, tenant_id: Uuid) -> Result<Vec<NotificationPref>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(sql::LIST, &[(&tenant_id, Type::UUID)])
                .await?;
            Ok(rows.iter().map(pref).collect())
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

/// `(event_type, "lineworks")` の設定が有効なら、その LINE WORKS の宛先。無い・無効・読めないなら None
/// (rust の `if let Ok(Some(pref)) = find_enabled(..)` と同じく、読めないときは通知しないだけ)
pub async fn enabled_lineworks(
    pg: &mut PgClient,
    tenant_id: Uuid,
    event_type: &'static str,
) -> Option<Vec<String>> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(
                    sql::FIND_ENABLED,
                    &[
                        (&tenant_id, Type::UUID),
                        (&event_type, Type::TEXT),
                        (&"lineworks", Type::TEXT),
                    ],
                )
                .await?;
            Ok(row.map(|r| r.get::<_, Vec<String>>(0)))
        })
    })
    .await
    .ok()
    .flatten()
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route(PATH, post(upsert_handler).get(list_handler))
        .route(
            &format!("{PATH}/{{id}}"),
            axum::routing::delete(delete_handler),
        )
}

async fn upsert_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<UpsertNotificationPref>,
) -> Result<(StatusCode, Json<NotificationPref>), StatusCode> {
    if !VALID_EVENT_TYPES.contains(&body.event_type.as_str())
        || !VALID_CHANNELS.contains(&body.notify_channel.as_str())
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut pg = state.pg.lock().await;
    let p = upsert(&mut pg, tenant_id, body)
        .await
        .map_err(|e| state.db_fail("notification pref upsert", &e))?;
    Ok((StatusCode::CREATED, Json(p)))
}

async fn list_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Vec<NotificationPref>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    list(&mut pg, tenant_id)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("notification pref list", &e))
}

async fn delete_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    let deleted = delete(&mut pg, tenant_id, id)
        .await
        .map_err(|e| state.db_fail("notification pref delete", &e))?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}
