//! 予約通知 (rust の `alc-trouble` の schedules.rs と repo/trouble_schedules.rs の写し)。
//!
//! - `POST /trouble/schedules` — 201。予約の時刻が過去・30 日より先、message が空白だけ、宛先が空なら 400 (rust と同じ)。
//!   チケットがこの tenant に無ければ (無い・別の tenant) 404 (タスクの作成と同じ確かめ。外部キーは RLS を通らない)。
//!   作成の後に alarm を登録し ([`crate::alarm`])、成功なら `cloud_task_name` に `alarms/{id}` を保存する
//!   (応答は保存の前の行。rust と同じ)。登録の失敗はログだけで、予約は残る
//! - `GET /trouble/tickets/{ticket_id}/schedules` — 予約の時刻の順
//! - `DELETE /trouble/schedules/{id}` — 無い → 404、pending でない → 409。alarm を取り消し (失敗はログだけ)、
//!   状態を `cancelled` にする (204)
//! - `POST /internal/trouble/schedules/{id}/fire` — alarm の DO が auth-worker の alc-internal-proxy (internal-jwt を検証する)
//!   経由で呼ぶ。`X-Tenant-ID` は来ない (この口だけ tenant の layer の外)。予約は SECURITY DEFINER の
//!   `get_trouble_schedule` で引く。**この関数は pending の行だけを返す**ので、送信済み・取消済み・無い id は 404
//!   (rust と同じ。rust の「pending 以外は 200」の枝は到達しないので持ち込まない)。チケットの見出しを付けて
//!   ([`build_fire_message`]。チケットが読めなければ本文だけ) LINE WORKS へ送り ([`crate::lineworks::notify`]。await する)、
//!   送信の成否によらず `sent` にする (rust と同じ)
//!
//! alarm・LINE WORKS の await は `tenant_tx` の閉包の外。ログは固定の語だけ。

use alc_core_wasm::TenantId;
use alc_worker_db::{PgClient, TxOutput};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use chrono::{DateTime, Duration, FixedOffset, Utc};
use serde::{Deserialize, Serialize};
use tokio_postgres::types::Type;
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::tickets::{self, Ticket};
use crate::{lineworks, tasks, AppState};

pub const FIRE_PATH: &str = "/internal/trouble/schedules/{id}/fire";
/// 予約できる先の上限 (rust と同じ 30 日)
pub const MAX_DAYS_AHEAD: i64 = 30;

/// rust の `TroubleSchedule` と同じ形・同じキーの順
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Schedule {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub ticket_id: Uuid,
    pub scheduled_at: DateTime<Utc>,
    pub message: String,
    pub lineworks_user_ids: Vec<String>,
    pub cloud_task_name: Option<String>,
    pub status: String,
    pub created_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub sent_at: Option<DateTime<Utc>>,
}

impl TxOutput for Schedule {}

/// rust の `CreateTroubleSchedule`
#[derive(Debug, Deserialize)]
pub struct CreateSchedule {
    pub ticket_id: Uuid,
    pub scheduled_at: DateTime<Utc>,
    pub message: String,
    pub lineworks_user_ids: Vec<String>,
}

/// 作成の入力の検証 (rust の create_schedule の 4 つの 400 と同じ順)
pub fn validate(input: &CreateSchedule, now: DateTime<Utc>) -> Result<(), StatusCode> {
    let bad = input.scheduled_at > now + Duration::days(MAX_DAYS_AHEAD)
        || input.scheduled_at <= now
        || input.message.trim().is_empty()
        || input.lineworks_user_ids.is_empty();
    if bad {
        Err(StatusCode::BAD_REQUEST)
    } else {
        Ok(())
    }
}

macro_rules! columns {
    () => {
        "id, tenant_id, ticket_id, scheduled_at, message, lineworks_user_ids, cloud_task_name, \
         status, created_by, created_at, sent_at"
    };
}

pub mod sql {
    pub const INSERT: &str = concat!(
        "INSERT INTO alc_api.trouble_schedules \
         (tenant_id, ticket_id, scheduled_at, message, lineworks_user_ids, created_by) \
         VALUES ($1, $2, $3, $4, $5, NULL) RETURNING ",
        columns!()
    );

    pub const LIST_BY_TICKET: &str = concat!(
        "SELECT ",
        columns!(),
        " FROM alc_api.trouble_schedules WHERE tenant_id = $1 AND ticket_id = $2 ORDER BY scheduled_at"
    );

    pub const GET: &str = concat!(
        "SELECT ",
        columns!(),
        " FROM alc_api.trouble_schedules WHERE id = $1 AND tenant_id = $2"
    );

    pub const UPDATE_STATUS: &str =
        "UPDATE alc_api.trouble_schedules SET status = $3 WHERE id = $1 AND tenant_id = $2";

    pub const SET_CLOUD_TASK_NAME: &str =
        "UPDATE alc_api.trouble_schedules SET cloud_task_name = $3 WHERE id = $1 AND tenant_id = $2";

    /// SECURITY DEFINER (RLS を通らない。pending の行だけを返す。migration 088)
    pub const GET_FOR_FIRE: &str = concat!(
        "SELECT ",
        columns!(),
        " FROM alc_api.get_trouble_schedule($1)"
    );

    pub const MARK_SENT: &str =
        "UPDATE alc_api.trouble_schedules SET status = 'sent', sent_at = NOW() \
         WHERE id = $1 AND tenant_id = $2 AND status = 'pending'";
}

fn schedule(r: &Row) -> Schedule {
    Schedule {
        id: r.get(0),
        tenant_id: r.get(1),
        ticket_id: r.get(2),
        scheduled_at: r.get(3),
        message: r.get(4),
        lineworks_user_ids: r.get(5),
        cloud_task_name: r.get(6),
        status: r.get(7),
        created_by: r.get(8),
        created_at: r.get(9),
        sent_at: r.get(10),
    }
}

/// 作成。チケットがこの tenant に無ければ INSERT せずに None
pub async fn create(
    pg: &mut PgClient,
    tenant_id: Uuid,
    input: CreateSchedule,
) -> Result<Option<Schedule>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let ticket = tx
                .query_typed_opt(
                    tasks::sql::TICKET_EXISTS,
                    &[(&input.ticket_id, Type::UUID), (&tenant_id, Type::UUID)],
                )
                .await?;
            if ticket.is_none() {
                return Ok(None);
            }
            let row = tx
                .query_typed_one(
                    sql::INSERT,
                    &[
                        (&tenant_id, Type::UUID),
                        (&input.ticket_id, Type::UUID),
                        (&input.scheduled_at, Type::TIMESTAMPTZ),
                        (&input.message, Type::TEXT),
                        (&input.lineworks_user_ids, Type::TEXT_ARRAY),
                    ],
                )
                .await?;
            Ok(Some(schedule(&row)))
        })
    })
    .await
}

pub async fn list_by_ticket(
    pg: &mut PgClient,
    tenant_id: Uuid,
    ticket_id: Uuid,
) -> Result<Vec<Schedule>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(
                    sql::LIST_BY_TICKET,
                    &[(&tenant_id, Type::UUID), (&ticket_id, Type::UUID)],
                )
                .await?;
            Ok(rows.iter().map(schedule).collect())
        })
    })
    .await
}

pub async fn get_one(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
) -> Result<Option<Schedule>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(sql::GET, &[(&id, Type::UUID), (&tenant_id, Type::UUID)])
                .await?;
            Ok(row.as_ref().map(schedule))
        })
    })
    .await
}

/// `sql` (`$1` = id・`$2` = tenant・`$3` = 値 (無ければ渡さない)) を流し、行が変わったか
async fn update(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
    stmt: &'static str,
    value: Option<String>,
) -> Result<bool, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let n = match &value {
                Some(v) => {
                    tx.execute_typed(
                        stmt,
                        &[(&id, Type::UUID), (&tenant_id, Type::UUID), (v, Type::TEXT)],
                    )
                    .await?
                }
                None => {
                    tx.execute_typed(stmt, &[(&id, Type::UUID), (&tenant_id, Type::UUID)])
                        .await?
                }
            };
            Ok(n > 0)
        })
    })
    .await
}

pub async fn set_cloud_task_name(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
    task_name: String,
) -> Result<bool, Error> {
    update(pg, tenant_id, id, sql::SET_CLOUD_TASK_NAME, Some(task_name)).await
}

/// fire 用 (RLS を通らない関数。tenant を知らないので nil の tenant の transaction の中で呼ぶ)
pub async fn get_for_fire(pg: &mut PgClient, id: Uuid) -> Result<Option<Schedule>, Error> {
    pg.tenant_tx(Uuid::nil(), move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(sql::GET_FOR_FIRE, &[(&id, Type::UUID)])
                .await?;
            Ok(row.as_ref().map(schedule))
        })
    })
    .await
}

/// 発火のメッセージの先頭に付けるチケットの見出し (rust の build_ticket_heading と同じ。空の項目は省き、日時は JST 固定)
pub fn build_ticket_heading(ticket: &Ticket) -> String {
    let jst = FixedOffset::east_opt(9 * 3600).expect("+09:00 is valid");

    let mut lines: Vec<String> = Vec::new();

    let when = ticket
        .occurred_at
        .map(|at| at.with_timezone(&jst).format("%Y-%m-%d %H:%M").to_string())
        .or_else(|| {
            ticket
                .occurred_date
                .map(|d| d.format("%Y-%m-%d").to_string())
        })
        .unwrap_or_default();
    let mut line1: Vec<&str> = Vec::new();
    let person = ticket.person_name.trim();
    if !person.is_empty() {
        line1.push(person);
    }
    if !when.is_empty() {
        line1.push(&when);
    }
    if !line1.is_empty() {
        lines.push(line1.join("  "));
    }

    let org = match (ticket.company_name.trim(), ticket.office_name.trim()) {
        ("", "") => String::new(),
        (c, "") => c.to_string(),
        ("", o) => o.to_string(),
        (c, o) => format!("{c}/{o}"),
    };
    let mut line2: Vec<&str> = Vec::new();
    if !org.is_empty() {
        line2.push(&org);
    }
    let location = ticket.location.trim();
    if !location.is_empty() {
        line2.push(location);
    }
    if !line2.is_empty() {
        lines.push(line2.join(" | "));
    }

    lines.join("\n")
}

/// 発火のメッセージ (rust の build_fire_message と同じ)。チケットが無ければ本文だけ。チケットの URL は入れない
/// (LINE WORKS のアプリ内ブラウザでは Google のログインが通らないため。rust と同じ)
pub fn build_fire_message(ticket: Option<&Ticket>, message: &str) -> String {
    let Some(ticket) = ticket else {
        return message.to_string();
    };

    let mut out = String::new();
    let heading = build_ticket_heading(ticket);
    if !heading.is_empty() {
        out.push_str(&heading);
        out.push_str("\n\n");
    }
    out.push_str(message);
    out
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/trouble/schedules", post(create_handler))
        .route("/trouble/tickets/{ticket_id}/schedules", get(list_handler))
        .route("/trouble/schedules/{id}", delete(cancel_handler))
}

/// tenant の layer の外に置く口 (呼び手は auth-worker の alc-internal-proxy だけ)
pub fn internal_router() -> Router<AppState> {
    Router::new().route(FIRE_PATH, post(fire_handler))
}

async fn create_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<CreateSchedule>,
) -> Result<(StatusCode, Json<Schedule>), StatusCode> {
    validate(&body, Utc::now())?;
    let mut pg = state.pg.lock().await;
    let schedule = create(&mut pg, tenant_id, body)
        .await
        .map_err(|e| state.db_fail("schedule create", &e))?
        .ok_or(StatusCode::NOT_FOUND)?;
    if let Some(alarm) = &state.alarm {
        match alarm.register(schedule.id, schedule.scheduled_at).await {
            // 保存の失敗は rust と同じく黙って捨てる (予約は残り、取消のときに alarm を消さないだけ)
            Ok(task_name) => {
                let _ = set_cloud_task_name(&mut pg, tenant_id, schedule.id, task_name).await;
            }
            Err(e) => (state.log)(&format!("trouble: schedule alarm register failed: {e}")),
        }
    }
    Ok((StatusCode::CREATED, Json(schedule)))
}

async fn list_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(ticket_id): Path<Uuid>,
) -> Result<Json<Vec<Schedule>>, StatusCode> {
    let mut pg = state.pg.lock().await;
    list_by_ticket(&mut pg, tenant_id, ticket_id)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("schedule list", &e))
}

async fn cancel_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    let schedule = get_one(&mut pg, tenant_id, id)
        .await
        .map_err(|e| state.db_fail("schedule get", &e))?
        .ok_or(StatusCode::NOT_FOUND)?;
    if schedule.status != "pending" {
        return Err(StatusCode::CONFLICT);
    }
    if let (Some(alarm), Some(task_name)) = (&state.alarm, &schedule.cloud_task_name) {
        if let Err(e) = alarm.cancel(task_name).await {
            (state.log)(&format!("trouble: schedule alarm cancel failed: {e}"));
        }
    }
    let cancelled = update(
        &mut pg,
        tenant_id,
        id,
        sql::UPDATE_STATUS,
        Some("cancelled".to_owned()),
    )
    .await
    .map_err(|e| state.db_fail("schedule cancel", &e))?;
    cancelled
        .then_some(StatusCode::NO_CONTENT)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn fire_handler(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    let schedule = get_for_fire(&mut pg, id)
        .await
        .map_err(|e| state.db_fail("schedule fire get", &e))?
        .ok_or(StatusCode::NOT_FOUND)?;
    // 見出しのチケット (行の tenant で引く)。読めなくても通知は落とさず本文だけ送る (rust と同じ)
    let ticket = tickets::get_one(&mut pg, schedule.tenant_id, schedule.ticket_id)
        .await
        .unwrap_or_else(|e| {
            state.db_fail("schedule fire ticket", &e);
            None
        });
    let message = build_fire_message(ticket.as_ref(), &schedule.message);
    lineworks::notify(
        &state,
        schedule.tenant_id,
        "trouble_schedule",
        &message,
        &schedule.lineworks_user_ids,
    )
    .await;
    // 送信の成否によらず送信済みにする (rust と同じく、失敗しても応答は 200)
    if let Err(e) = update(&mut pg, schedule.tenant_id, id, sql::MARK_SENT, None).await {
        state.db_fail("schedule mark sent", &e);
    }
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, TimeZone};

    fn ticket_fixture() -> Ticket {
        Ticket {
            id: Uuid::nil(),
            tenant_id: Uuid::nil(),
            ticket_no: 1,
            category: "事故".to_string(),
            title: "テスト".to_string(),
            // JST 2026-05-19 07:00 = UTC 2026-05-18 22:00
            occurred_at: Some(Utc.with_ymd_and_hms(2026, 5, 18, 22, 0, 0).unwrap()),
            occurred_date: None,
            company_name: "運送会社".to_string(),
            office_name: "本社".to_string(),
            department: String::new(),
            person_name: "山田 太郎".to_string(),
            person_id: None,
            person_is_external: false,
            registration_number: String::new(),
            location: "本社整備工場前".to_string(),
            description: String::new(),
            status_id: None,
            assigned_to: None,
            progress_notes: String::new(),
            allowance: String::new(),
            damage_amount: None,
            compensation_amount: None,
            confirmation_notice: String::new(),
            disciplinary_content: String::new(),
            disciplinary_action: String::new(),
            disciplinary_committee: String::new(),
            road_service_cost: None,
            counterparty: String::new(),
            counterparty_insurance: String::new(),
            counterparty_vehicle: String::new(),
            custom_fields: serde_json::json!({}),
            due_date: None,
            overdue_notified_at: None,
            created_by: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            deleted_at: None,
        }
    }

    #[test]
    fn heading_full_fields_with_jst_datetime() {
        assert_eq!(
            build_ticket_heading(&ticket_fixture()),
            "山田 太郎  2026-05-19 07:00\n運送会社/本社 | 本社整備工場前"
        );
    }

    #[test]
    fn heading_falls_back_to_occurred_date_when_no_datetime() {
        let mut t = ticket_fixture();
        t.occurred_at = None;
        t.occurred_date = Some(NaiveDate::from_ymd_opt(2026, 5, 19).unwrap());
        assert!(build_ticket_heading(&t).starts_with("山田 太郎  2026-05-19\n"));
    }

    #[test]
    fn heading_omits_empty_fields_per_line() {
        let mut t = ticket_fixture();
        t.person_name = String::new();
        t.occurred_at = None;
        assert_eq!(build_ticket_heading(&t), "運送会社/本社 | 本社整備工場前");

        let mut t2 = ticket_fixture();
        t2.company_name = String::new();
        t2.office_name = String::new();
        t2.location = String::new();
        assert_eq!(build_ticket_heading(&t2), "山田 太郎  2026-05-19 07:00");

        let mut t3 = ticket_fixture();
        t3.office_name = String::new();
        assert_eq!(
            build_ticket_heading(&t3),
            "山田 太郎  2026-05-19 07:00\n運送会社 | 本社整備工場前"
        );

        let mut t4 = ticket_fixture();
        t4.location = String::new();
        assert_eq!(
            build_ticket_heading(&t4),
            "山田 太郎  2026-05-19 07:00\n運送会社/本社"
        );

        let mut t5 = ticket_fixture();
        t5.company_name = String::new();
        assert_eq!(
            build_ticket_heading(&t5),
            "山田 太郎  2026-05-19 07:00\n本社 | 本社整備工場前"
        );
    }

    #[test]
    fn heading_empty_when_all_fields_empty() {
        let mut t = ticket_fixture();
        t.person_name = String::new();
        t.occurred_at = None;
        t.company_name = String::new();
        t.office_name = String::new();
        t.location = String::new();
        assert_eq!(build_ticket_heading(&t), "");
        // 見出しが空でも本文は素のまま (先頭に空行を作らない)
        assert_eq!(build_fire_message(Some(&t), "本文です"), "本文です");
    }

    #[test]
    fn fire_message_prepends_heading_without_url() {
        let msg = build_fire_message(Some(&ticket_fixture()), "本文です");
        assert_eq!(
            msg,
            "山田 太郎  2026-05-19 07:00\n運送会社/本社 | 本社整備工場前\n\n本文です"
        );
        assert!(!msg.contains("http") && !msg.contains("/tickets/"));
        assert_eq!(build_fire_message(None, "本文です"), "本文です");
    }

    #[test]
    fn validate_matches_rust() {
        let now = Utc.with_ymd_and_hms(2026, 10, 10, 0, 0, 0).unwrap();
        let input = |at: DateTime<Utc>, message: &str, to: &[&str]| CreateSchedule {
            ticket_id: Uuid::nil(),
            scheduled_at: at,
            message: message.to_owned(),
            lineworks_user_ids: to.iter().map(|s| s.to_string()).collect(),
        };
        let ok = now + Duration::hours(1);
        assert_eq!(validate(&input(ok, "m", &["u"]), now), Ok(()));
        // 30 日ちょうどは可、それより先は不可
        assert_eq!(
            validate(&input(now + Duration::days(30), "m", &["u"]), now),
            Ok(())
        );
        let bad = StatusCode::BAD_REQUEST;
        assert_eq!(
            validate(
                &input(now + Duration::days(30) + Duration::seconds(1), "m", &["u"]),
                now
            ),
            Err(bad)
        );
        // 今ちょうどと過去は不可
        assert_eq!(validate(&input(now, "m", &["u"]), now), Err(bad));
        assert_eq!(validate(&input(ok, " \n", &["u"]), now), Err(bad));
        assert_eq!(validate(&input(ok, "m", &[]), now), Err(bad));
    }
}
