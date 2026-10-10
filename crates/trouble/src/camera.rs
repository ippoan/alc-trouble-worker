//! カメラ停止の自動チケット (rust-alc-api の main.rs の `TroubleDownTicketSink` の写し)。
//!
//! - `POST /internal/trouble/camera-down-tickets` — rust のカメラの死活監視が、auth-worker の `/alc-internal-proxy`
//!   (shared secret・`X-Tenant-ID` 必須) 経由で呼ぶ。201 + `{"id"}`
//!
//! 作成は rust と同じく repo 直 ([`tickets::create`]): 分類は「その他」、初期の状態・履歴・LINE WORKS の通知は無し。
//! `custom_fields` に自動起票の印 (`source` = `camera_auto`・`camera_id`) を入れる。
//! 冪等は持たない (rust の `cameras.active_down_ticket_id` が担保する)。
//!
//! shared secret を持つ呼び手なら tenant を申告して叩けるので、その tenant に在るカメラ (`cameras`) の id でなければ
//! 404 にする。本文が読めなければ 400 (本文は空)。

use alc_core_wasm::TenantId;
use alc_worker_db::PgClient;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Extension, Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_postgres::types::Type;
use tokio_postgres::Error;
use uuid::Uuid;

use crate::tickets::{self, CreateTicket};
use crate::AppState;

/// rust の `CameraDownTicket` と同じ 5 項目
#[derive(Debug, Deserialize)]
pub struct CameraDownTicket {
    pub title: String,
    pub description: String,
    pub location: String,
    pub occurred_at: DateTime<Utc>,
    pub camera_id: Uuid,
}

#[derive(Debug, Serialize)]
pub struct Created {
    pub id: Uuid,
}

/// rust の adapter と同じ写像 (ほかの項目は既定値)
pub fn ticket_input(t: CameraDownTicket) -> CreateTicket {
    CreateTicket {
        category: "その他".to_owned(),
        title: Some(t.title),
        occurred_at: Some(t.occurred_at),
        occurred_date: None,
        company_name: None,
        office_name: None,
        department: None,
        person_name: None,
        person_id: None,
        person_is_external: None,
        registration_number: None,
        location: Some(t.location),
        description: Some(t.description),
        assigned_to: None,
        damage_amount: None,
        compensation_amount: None,
        road_service_cost: None,
        counterparty: None,
        counterparty_insurance: None,
        custom_fields: Some(json!({"source": "camera_auto", "camera_id": t.camera_id})),
        due_date: None,
    }
}

/// その tenant にカメラが在るか
pub async fn camera_exists(pg: &mut PgClient, tenant_id: Uuid, id: Uuid) -> Result<bool, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(
                    "SELECT 1 FROM alc_api.cameras WHERE id = $1 AND tenant_id = $2",
                    &[(&id, Type::UUID), (&tenant_id, Type::UUID)],
                )
                .await?;
            Ok(row.is_some())
        })
    })
    .await
}

pub fn router() -> Router<AppState> {
    Router::new().route(
        "/internal/trouble/camera-down-tickets",
        post(create_handler),
    )
}

async fn create_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    body: Bytes,
) -> Result<(StatusCode, Json<Created>), StatusCode> {
    let body: CameraDownTicket =
        serde_json::from_slice(&body).map_err(|_| StatusCode::BAD_REQUEST)?;
    let mut pg = state.pg.lock().await;
    let exists = camera_exists(&mut pg, tenant_id, body.camera_id)
        .await
        .map_err(|e| state.db_fail("camera ticket camera get", &e))?;
    if !exists {
        return Err(StatusCode::NOT_FOUND);
    }
    let ticket = tickets::create(&mut pg, tenant_id, ticket_input(body), None)
        .await
        .map_err(|e| state.db_fail("camera ticket create", &e))?;
    Ok((StatusCode::CREATED, Json(Created { id: ticket.id })))
}
