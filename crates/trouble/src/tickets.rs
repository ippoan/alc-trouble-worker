//! トラブルのチケット (rust の `alc-trouble` の tickets.rs と repo/trouble_tickets.rs の写し)。
//!
//! - `POST /trouble/tickets` — 201。分類は tenant の分類のマスタ (無ければ既定の 7 つ) のどれかでないと 400。
//!   状態は初期の状態 (在れば) で、履歴を 1 行足す。`ticket_no` は DB の既定値で振る (INSERT で指定しない)。
//!   webhook `trouble_created` を積む
//! - `GET /trouble/tickets` — 絞り込み 8 種・並び 2 種・ページ。`{tickets, total, page, per_page}`
//! - `GET /trouble/tickets/csv` — 同じ絞り込みで 10000 件まで、BOM 付きの CSV
//! - `GET`/`PUT`/`DELETE /trouble/tickets/{id}` — 削除は論理削除 (204)。消したもの・無いものは 404
//! - `POST /trouble/tickets/{id}/transition` — 遷移の表に無い移り方は 422。履歴を 1 行足し、
//!   webhook `trouble_status_changed` を積む
//!
//! 一覧の SQL は WHERE・ORDER BY を組み立てるが、文に入るのは固定の断片と `$n` だけ (利用者の入力は全部 bind。
//! 並びの列と向きは許可リスト [`order_sql`] からだけ選ぶ)。

use alc_core_wasm::TenantId;
use alc_worker_db::{PgClient, TxOutput};
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::master::{self, DEFAULT_CATEGORIES};
use crate::webhook::Event;
use crate::{workflow, AppState};

/// rust の `TroubleTicket` と同じ形・同じキーの順
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Ticket {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub ticket_no: i32,
    pub category: String,
    pub title: String,
    pub occurred_at: Option<DateTime<Utc>>,
    pub occurred_date: Option<NaiveDate>,
    pub company_name: String,
    pub office_name: String,
    pub department: String,
    pub person_name: String,
    pub person_id: Option<Uuid>,
    pub person_is_external: bool,
    pub registration_number: String,
    pub location: String,
    pub description: String,
    pub status_id: Option<Uuid>,
    pub assigned_to: Option<Uuid>,
    pub progress_notes: String,
    pub allowance: String,
    pub damage_amount: Option<String>,
    pub compensation_amount: Option<String>,
    pub confirmation_notice: String,
    pub disciplinary_content: String,
    pub disciplinary_action: String,
    pub disciplinary_committee: String,
    pub road_service_cost: Option<String>,
    pub counterparty: String,
    pub counterparty_insurance: String,
    pub counterparty_vehicle: String,
    pub custom_fields: Value,
    pub due_date: Option<DateTime<Utc>>,
    pub overdue_notified_at: Option<DateTime<Utc>>,
    pub created_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

impl TxOutput for Ticket {}

/// rust の `CreateTroubleTicket`
#[derive(Debug, Deserialize)]
pub struct CreateTicket {
    pub category: String,
    pub title: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub occurred_at: Option<DateTime<Utc>>,
    pub occurred_date: Option<NaiveDate>,
    pub company_name: Option<String>,
    pub office_name: Option<String>,
    pub department: Option<String>,
    pub person_name: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub person_id: Option<Uuid>,
    #[serde(default)]
    pub person_is_external: Option<bool>,
    pub registration_number: Option<String>,
    pub location: Option<String>,
    pub description: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub assigned_to: Option<Uuid>,
    pub damage_amount: Option<f64>,
    pub compensation_amount: Option<f64>,
    pub road_service_cost: Option<f64>,
    pub counterparty: Option<String>,
    pub counterparty_insurance: Option<String>,
    pub custom_fields: Option<Value>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub due_date: Option<DateTime<Utc>>,
}

/// rust の `UpdateTroubleTicket`。来た項目だけ変える (登録番号だけは `""` / `null` で空にできる)
#[derive(Debug, Deserialize)]
pub struct UpdateTicket {
    pub category: Option<String>,
    pub title: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub occurred_at: Option<DateTime<Utc>>,
    pub occurred_date: Option<NaiveDate>,
    pub company_name: Option<String>,
    pub office_name: Option<String>,
    pub department: Option<String>,
    pub person_name: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub person_id: Option<Uuid>,
    #[serde(default)]
    pub person_is_external: Option<bool>,
    #[serde(default, deserialize_with = "empty_string_as_clear")]
    pub registration_number: Option<Option<String>>,
    pub location: Option<String>,
    pub description: Option<String>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub assigned_to: Option<Uuid>,
    pub progress_notes: Option<String>,
    pub allowance: Option<String>,
    pub damage_amount: Option<f64>,
    pub compensation_amount: Option<f64>,
    pub confirmation_notice: Option<String>,
    pub disciplinary_content: Option<String>,
    pub disciplinary_action: Option<String>,
    pub disciplinary_committee: Option<String>,
    pub road_service_cost: Option<f64>,
    pub counterparty: Option<String>,
    pub counterparty_insurance: Option<String>,
    pub counterparty_vehicle: Option<String>,
    pub custom_fields: Option<Value>,
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub due_date: Option<DateTime<Utc>>,
}

/// rust の `TroubleTicketFilter` (query string)
#[derive(Debug, Default, Deserialize)]
pub struct TicketFilter {
    pub category: Option<String>,
    pub status_id: Option<Uuid>,
    pub person_name: Option<String>,
    pub company_name: Option<String>,
    pub office_name: Option<String>,
    pub date_from: Option<NaiveDate>,
    pub date_to: Option<NaiveDate>,
    pub q: Option<String>,
    pub page: Option<i64>,
    pub per_page: Option<i64>,
    /// `occurred` | `ticket_no` (それ以外・無しは `ticket_no` の降順)
    pub sort_by: Option<String>,
    pub sort_desc: Option<bool>,
}

/// rust の `TroubleTicketsResponse`
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TicketsResponse {
    pub tickets: Vec<Ticket>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
}

impl TxOutput for TicketsResponse {}

/// rust の `TransitionRequest`
#[derive(Debug, Deserialize)]
pub struct TransitionRequest {
    pub to_state_id: Uuid,
    pub comment: Option<String>,
}

/// `""` と `null` を None にする (rust の `serde_helpers::empty_string_as_none_datetime` / `_uuid`)。
/// 画面の日付・UUID の入力欄は空のとき `""` を送ってくる
fn empty_string_as_none<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: serde::de::DeserializeOwned,
{
    match Option::<Value>::deserialize(deserializer)? {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) if s.is_empty() => Ok(None),
        Some(v) => T::deserialize(v)
            .map(Some)
            .map_err(serde::de::Error::custom),
    }
}

/// 来ない → None、`""` / `null` → Some(None) (空にする)、文字列 → Some(Some) (rust の
/// `serde_helpers::empty_string_as_none_option_string`)
fn empty_string_as_clear<'de, D>(deserializer: D) -> Result<Option<Option<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    match Value::deserialize(deserializer)? {
        Value::Null => Ok(Some(None)),
        Value::String(s) if s.is_empty() => Ok(Some(None)),
        Value::String(s) => Ok(Some(Some(s))),
        _ => Err(serde::de::Error::custom("expected string or null")),
    }
}

/// 返す列 (37 本。数値の 3 本は rust と同じく text にする)
macro_rules! columns {
    () => {
        "id, tenant_id, ticket_no, category, title, \
         occurred_at, occurred_date, \
         company_name, office_name, department, \
         person_name, person_id, person_is_external, \
         registration_number, \
         location, description, \
         status_id, assigned_to, \
         progress_notes, allowance, \
         damage_amount::text, compensation_amount::text, \
         confirmation_notice, disciplinary_content, disciplinary_action, disciplinary_committee, \
         road_service_cost::text, \
         counterparty, counterparty_insurance, counterparty_vehicle, \
         custom_fields, due_date, overdue_notified_at, \
         created_by, created_at, updated_at, deleted_at"
    };
}

pub mod sql {
    pub const COLUMNS: &str = columns!();

    pub const INSERT: &str = concat!(
        "INSERT INTO alc_api.trouble_tickets (\
         tenant_id, category, title, \
         occurred_at, occurred_date, \
         company_name, office_name, department, \
         person_name, person_id, person_is_external, \
         registration_number, \
         location, description, \
         status_id, assigned_to, \
         damage_amount, compensation_amount, road_service_cost, \
         counterparty, counterparty_insurance, \
         custom_fields, due_date, created_by) \
         VALUES (\
         $1, $2, COALESCE($3, ''), \
         $4, $5, \
         COALESCE($6, ''), COALESCE($7, ''), COALESCE($8, ''), \
         COALESCE($9, ''), $10, COALESCE($11, false), \
         COALESCE($12, ''), \
         COALESCE($13, ''), COALESCE($14, ''), \
         $15, $16, \
         $17, $18, $19, \
         COALESCE($20, ''), COALESCE($21, ''), \
         COALESCE($22, '{}'::jsonb), $23, $24) \
         RETURNING ",
        columns!()
    );

    pub const GET: &str = concat!(
        "SELECT ",
        columns!(),
        " FROM alc_api.trouble_tickets WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL"
    );

    pub const UPDATE: &str = concat!(
        "UPDATE alc_api.trouble_tickets SET \
         category = COALESCE($3, category), \
         title = COALESCE($4, title), \
         occurred_at = COALESCE($5, occurred_at), \
         occurred_date = COALESCE($6, occurred_date), \
         company_name = COALESCE($7, company_name), \
         office_name = COALESCE($8, office_name), \
         department = COALESCE($9, department), \
         person_name = COALESCE($10, person_name), \
         person_id = COALESCE($11, person_id), \
         person_is_external = COALESCE($12, person_is_external), \
         location = COALESCE($13, location), \
         description = COALESCE($14, description), \
         assigned_to = COALESCE($15, assigned_to), \
         progress_notes = COALESCE($16, progress_notes), \
         allowance = COALESCE($17, allowance), \
         damage_amount = COALESCE($18, damage_amount), \
         compensation_amount = COALESCE($19, compensation_amount), \
         confirmation_notice = COALESCE($20, confirmation_notice), \
         disciplinary_content = COALESCE($21, disciplinary_content), \
         disciplinary_action = COALESCE($22, disciplinary_action), \
         disciplinary_committee = COALESCE($23, disciplinary_committee), \
         road_service_cost = COALESCE($24, road_service_cost), \
         counterparty = COALESCE($25, counterparty), \
         counterparty_insurance = COALESCE($26, counterparty_insurance), \
         counterparty_vehicle = COALESCE($27, counterparty_vehicle), \
         custom_fields = COALESCE($28, custom_fields), \
         due_date = COALESCE($29, due_date), \
         registration_number = CASE WHEN $30 THEN $31 ELSE registration_number END, \
         updated_at = NOW() \
         WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL \
         RETURNING ",
        columns!()
    );

    pub const SOFT_DELETE: &str =
        "UPDATE alc_api.trouble_tickets SET deleted_at = NOW(), updated_at = NOW() \
         WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL";

    pub const UPDATE_STATUS: &str = concat!(
        "UPDATE alc_api.trouble_tickets SET status_id = $3, updated_at = NOW() \
         WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL \
         RETURNING ",
        columns!()
    );
}

/// 登録番号の比較で全角の数字・ハイフン・長音を半角にそろえる (rust の q の検索と同じ `translate`)
const NORMALIZE_FROM: &str = "０１２３４５６７８９－ー";
const NORMALIZE_TO: &str = "0123456789--";

/// 一覧の WHERE と、その `$n` に入れる値 (`$1` は tenant_id)。順は rust と同じ
/// (category → status_id → person_name → company_name → office_name → date_from → date_to → q)
fn where_clause<'a>(
    tenant_id: &'a Uuid,
    f: &'a TicketFilter,
) -> (String, Vec<(&'a (dyn ToSql + Sync), Type)>) {
    let mut clauses = vec!["tenant_id = $1".to_owned(), "deleted_at IS NULL".to_owned()];
    let mut params: Vec<(&(dyn ToSql + Sync), Type)> = vec![(tenant_id, Type::UUID)];
    let mut add = |clause: &dyn Fn(usize) -> String, value: &'a (dyn ToSql + Sync), ty: Type| {
        params.push((value, ty));
        clauses.push(clause(params.len()));
    };
    if let Some(v) = &f.category {
        add(&|i| format!("category = ${i}"), v, Type::TEXT);
    }
    if let Some(v) = &f.status_id {
        add(&|i| format!("status_id = ${i}"), v, Type::UUID);
    }
    if let Some(v) = &f.person_name {
        add(
            &|i| format!("person_name ILIKE '%' || ${i} || '%'"),
            v,
            Type::TEXT,
        );
    }
    if let Some(v) = &f.company_name {
        add(
            &|i| format!("company_name ILIKE '%' || ${i} || '%'"),
            v,
            Type::TEXT,
        );
    }
    if let Some(v) = &f.office_name {
        add(
            &|i| format!("office_name ILIKE '%' || ${i} || '%'"),
            v,
            Type::TEXT,
        );
    }
    if let Some(v) = &f.date_from {
        add(&|i| format!("occurred_date >= ${i}"), v, Type::DATE);
    }
    if let Some(v) = &f.date_to {
        add(&|i| format!("occurred_date <= ${i}"), v, Type::DATE);
    }
    if let Some(v) = &f.q {
        add(
            &|i| {
                format!(
                    "(description ILIKE '%' || ${i} || '%' \
                     OR location ILIKE '%' || ${i} || '%' \
                     OR person_name ILIKE '%' || ${i} || '%' \
                     OR translate(registration_number, '{NORMALIZE_FROM}', '{NORMALIZE_TO}') \
                     ILIKE '%' || translate(${i}, '{NORMALIZE_FROM}', '{NORMALIZE_TO}') || '%')"
                )
            },
            v,
            Type::TEXT,
        );
    }
    (clauses.join(" AND "), params)
}

/// 並び (許可リスト。利用者の文字列は文に入らない)。`occurred` は日付だけの行も並ぶよう occurred_date を
/// 第 1 キーにする (Refs ippoan/nuxt-trouble#225)。それ以外・無しは `ticket_no DESC`
pub fn order_sql(sort_by: Option<&str>, sort_desc: Option<bool>) -> String {
    let direction = if sort_desc.unwrap_or(true) {
        "DESC"
    } else {
        "ASC"
    };
    match sort_by {
        Some("occurred") => format!(
            "occurred_date {direction} NULLS LAST, occurred_at {direction} NULLS LAST, ticket_no DESC"
        ),
        Some("ticket_no") => format!("ticket_no {direction}"),
        _ => "ticket_no DESC".to_owned(),
    }
}

fn ticket(r: &Row) -> Ticket {
    Ticket {
        id: r.get(0),
        tenant_id: r.get(1),
        ticket_no: r.get(2),
        category: r.get(3),
        title: r.get(4),
        occurred_at: r.get(5),
        occurred_date: r.get(6),
        company_name: r.get(7),
        office_name: r.get(8),
        department: r.get(9),
        person_name: r.get(10),
        person_id: r.get(11),
        person_is_external: r.get(12),
        registration_number: r.get(13),
        location: r.get(14),
        description: r.get(15),
        status_id: r.get(16),
        assigned_to: r.get(17),
        progress_notes: r.get(18),
        allowance: r.get(19),
        damage_amount: r.get(20),
        compensation_amount: r.get(21),
        confirmation_notice: r.get(22),
        disciplinary_content: r.get(23),
        disciplinary_action: r.get(24),
        disciplinary_committee: r.get(25),
        road_service_cost: r.get(26),
        counterparty: r.get(27),
        counterparty_insurance: r.get(28),
        counterparty_vehicle: r.get(29),
        custom_fields: r.get(30),
        due_date: r.get(31),
        overdue_notified_at: r.get(32),
        created_by: r.get(33),
        created_at: r.get(34),
        updated_at: r.get(35),
        deleted_at: r.get(36),
    }
}

pub async fn create(
    pg: &mut PgClient,
    tenant_id: Uuid,
    input: CreateTicket,
    initial_status_id: Option<Uuid>,
) -> Result<Ticket, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let i = &input;
            let row = tx
                .query_typed_one(
                    sql::INSERT,
                    &[
                        (&tenant_id, Type::UUID),
                        (&i.category, Type::TEXT),
                        (&i.title, Type::TEXT),
                        (&i.occurred_at, Type::TIMESTAMPTZ),
                        (&i.occurred_date, Type::DATE),
                        (&i.company_name, Type::TEXT),
                        (&i.office_name, Type::TEXT),
                        (&i.department, Type::TEXT),
                        (&i.person_name, Type::TEXT),
                        (&i.person_id, Type::UUID),
                        (&i.person_is_external, Type::BOOL),
                        (&i.registration_number, Type::TEXT),
                        (&i.location, Type::TEXT),
                        (&i.description, Type::TEXT),
                        (&initial_status_id, Type::UUID),
                        (&i.assigned_to, Type::UUID),
                        (&i.damage_amount, Type::FLOAT8),
                        (&i.compensation_amount, Type::FLOAT8),
                        (&i.road_service_cost, Type::FLOAT8),
                        (&i.counterparty, Type::TEXT),
                        (&i.counterparty_insurance, Type::TEXT),
                        (&i.custom_fields, Type::JSONB),
                        (&i.due_date, Type::TIMESTAMPTZ),
                        (&None::<Uuid>, Type::UUID),
                    ],
                )
                .await?;
            Ok(ticket(&row))
        })
    })
    .await
}

/// 1 ページ分と、絞り込みに合う件数 (COUNT と一覧を 1 つの `tenant_tx` で)
pub async fn list(
    pg: &mut PgClient,
    tenant_id: Uuid,
    filter: TicketFilter,
) -> Result<TicketsResponse, Error> {
    let per_page = filter.per_page.unwrap_or(50).min(10000);
    let page = filter.page.unwrap_or(1).max(1);
    let offset = (page - 1) * per_page;
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let (where_sql, mut params) = where_clause(&tenant_id, &filter);
            let count_sql = format!("SELECT COUNT(*) FROM alc_api.trouble_tickets WHERE {where_sql}");
            let total: i64 = tx.query_typed_one(&count_sql, &params).await?.get(0);
            let n = params.len();
            let list_sql = format!(
                "SELECT {} FROM alc_api.trouble_tickets WHERE {where_sql} ORDER BY {} LIMIT ${} OFFSET ${}",
                sql::COLUMNS,
                order_sql(filter.sort_by.as_deref(), filter.sort_desc),
                n + 1,
                n + 2
            );
            params.push((&per_page, Type::INT8));
            params.push((&offset, Type::INT8));
            let rows = tx.query_typed(&list_sql, &params).await?;
            Ok(TicketsResponse {
                tickets: rows.iter().map(ticket).collect(),
                total,
                page,
                per_page,
            })
        })
    })
    .await
}

pub async fn get_one(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
) -> Result<Option<Ticket>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(sql::GET, &[(&id, Type::UUID), (&tenant_id, Type::UUID)])
                .await?;
            Ok(row.as_ref().map(ticket))
        })
    })
    .await
}

pub async fn update(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
    input: UpdateTicket,
) -> Result<Option<Ticket>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let i = &input;
            let set_registration = i.registration_number.is_some();
            let registration = i.registration_number.clone().flatten().unwrap_or_default();
            let row = tx
                .query_typed_opt(
                    sql::UPDATE,
                    &[
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                        (&i.category, Type::TEXT),
                        (&i.title, Type::TEXT),
                        (&i.occurred_at, Type::TIMESTAMPTZ),
                        (&i.occurred_date, Type::DATE),
                        (&i.company_name, Type::TEXT),
                        (&i.office_name, Type::TEXT),
                        (&i.department, Type::TEXT),
                        (&i.person_name, Type::TEXT),
                        (&i.person_id, Type::UUID),
                        (&i.person_is_external, Type::BOOL),
                        (&i.location, Type::TEXT),
                        (&i.description, Type::TEXT),
                        (&i.assigned_to, Type::UUID),
                        (&i.progress_notes, Type::TEXT),
                        (&i.allowance, Type::TEXT),
                        (&i.damage_amount, Type::FLOAT8),
                        (&i.compensation_amount, Type::FLOAT8),
                        (&i.confirmation_notice, Type::TEXT),
                        (&i.disciplinary_content, Type::TEXT),
                        (&i.disciplinary_action, Type::TEXT),
                        (&i.disciplinary_committee, Type::TEXT),
                        (&i.road_service_cost, Type::FLOAT8),
                        (&i.counterparty, Type::TEXT),
                        (&i.counterparty_insurance, Type::TEXT),
                        (&i.counterparty_vehicle, Type::TEXT),
                        (&i.custom_fields, Type::JSONB),
                        (&i.due_date, Type::TIMESTAMPTZ),
                        (&set_registration, Type::BOOL),
                        (&registration, Type::TEXT),
                    ],
                )
                .await?;
            Ok(row.as_ref().map(ticket))
        })
    })
    .await
}

pub async fn soft_delete(pg: &mut PgClient, tenant_id: Uuid, id: Uuid) -> Result<bool, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let n = tx
                .execute_typed(
                    sql::SOFT_DELETE,
                    &[(&id, Type::UUID), (&tenant_id, Type::UUID)],
                )
                .await?;
            Ok(n > 0)
        })
    })
    .await
}

pub async fn update_status(
    pg: &mut PgClient,
    tenant_id: Uuid,
    id: Uuid,
    status_id: Uuid,
) -> Result<Option<Ticket>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(
                    sql::UPDATE_STATUS,
                    &[
                        (&id, Type::UUID),
                        (&tenant_id, Type::UUID),
                        (&status_id, Type::UUID),
                    ],
                )
                .await?;
            Ok(row.as_ref().map(ticket))
        })
    })
    .await
}

/// CSV の見出し (rust の export_csv と同じ 20 列)
pub const CSV_HEADER: [&str; 20] = [
    "No",
    "発生日",
    "所属会社名",
    "営業所名",
    "運行課",
    "当事者名",
    "登録番号",
    "事故等分類",
    "発生場所",
    "内容",
    "進捗状況",
    "手当等",
    "損害額",
    "賠償額",
    "確認書_決定通知書",
    "処分検討内容",
    "処分内容",
    "ロードサービス費用",
    "相手",
    "相手保険会社",
];

/// BOM (EF BB BF) + CSV (rust の export_csv と同じ csv crate の既定の書き方)
pub fn csv_bytes(tickets: &[Ticket]) -> Vec<u8> {
    let mut wtr = csv::Writer::from_writer(vec![0xEF, 0xBB, 0xBF]);
    // 書き先は Vec なので失敗しない
    wtr.write_record(CSV_HEADER).expect("csv header");
    for t in tickets {
        wtr.write_record([
            t.ticket_no.to_string(),
            t.occurred_date
                .map_or(String::new(), |d| d.format("%Y-%m-%d").to_string()),
            t.company_name.clone(),
            t.office_name.clone(),
            t.department.clone(),
            t.person_name.clone(),
            t.registration_number.clone(),
            t.category.clone(),
            t.location.clone(),
            t.description.clone(),
            t.progress_notes.clone(),
            t.allowance.clone(),
            t.damage_amount.clone().unwrap_or_default(),
            t.compensation_amount.clone().unwrap_or_default(),
            t.confirmation_notice.clone(),
            t.disciplinary_content.clone(),
            t.disciplinary_action.clone(),
            t.road_service_cost.clone().unwrap_or_default(),
            t.counterparty.clone(),
            t.counterparty_insurance.clone(),
        ])
        .expect("csv row");
    }
    wtr.into_inner().expect("csv flush")
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/trouble/tickets", post(create_handler).get(list_handler))
        .route("/trouble/tickets/csv", get(csv_handler))
        .route(
            "/trouble/tickets/{id}",
            get(get_handler).put(update_handler).delete(delete_handler),
        )
        .route("/trouble/tickets/{id}/transition", post(transition_handler))
}

async fn create_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<CreateTicket>,
) -> Result<(StatusCode, Json<Ticket>), StatusCode> {
    let mut pg = state.pg.lock().await;
    // rust と同じく、分類のマスタが読めなければ空 (= 既定の分類) として扱う
    let categories = master::list::<master::Categories>(&mut pg, tenant_id)
        .await
        .unwrap_or_default();
    let valid = if categories.is_empty() {
        DEFAULT_CATEGORIES.contains(&body.category.as_str())
    } else {
        categories.iter().any(|c| c.name == body.category)
    };
    if !valid {
        return Err(StatusCode::BAD_REQUEST);
    }
    let initial = workflow::get_initial_state(&mut pg, tenant_id)
        .await
        .map_err(|e| state.db_fail("ticket initial state", &e))?;
    let ticket = create(&mut pg, tenant_id, body, initial.map(|s| s.id))
        .await
        .map_err(|e| state.db_fail("ticket create", &e))?;
    if let Some(status_id) = ticket.status_id {
        workflow::record_history_or_log(
            &state, &mut pg, tenant_id, ticket.id, None, status_id, None,
        )
        .await;
    }
    state.webhooks.push(Event {
        tenant_id,
        event_type: "trouble_created",
        payload: json!({
            "event": "trouble_created",
            "timestamp": (state.now)(),
            "tenant_id": tenant_id,
            "data": {
                "ticket_id": ticket.id,
                "ticket_no": ticket.ticket_no,
                "category": ticket.category,
                "person_name": ticket.person_name,
                "description": ticket.description,
            }
        }),
    });
    // TODO(#747 T5): LINE WORKS 通知
    Ok((StatusCode::CREATED, Json(ticket)))
}

async fn list_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(filter): Query<TicketFilter>,
) -> Result<Json<TicketsResponse>, StatusCode> {
    let mut pg = state.pg.lock().await;
    list(&mut pg, tenant_id, filter)
        .await
        .map(Json)
        .map_err(|e| state.db_fail("ticket list", &e))
}

async fn get_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<Json<Ticket>, StatusCode> {
    let mut pg = state.pg.lock().await;
    get_one(&mut pg, tenant_id, id)
        .await
        .map_err(|e| state.db_fail("ticket get", &e))?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn update_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateTicket>,
) -> Result<Json<Ticket>, StatusCode> {
    let mut pg = state.pg.lock().await;
    let ticket = update(&mut pg, tenant_id, id, body)
        .await
        .map_err(|e| state.db_fail("ticket update", &e))?
        .ok_or(StatusCode::NOT_FOUND)?;
    // TODO(#747 T5): LINE WORKS 通知
    Ok(Json(ticket))
}

async fn delete_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let mut pg = state.pg.lock().await;
    let found = soft_delete(&mut pg, tenant_id, id)
        .await
        .map_err(|e| state.db_fail("ticket delete", &e))?;
    if found {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

async fn transition_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(id): Path<Uuid>,
    Json(body): Json<TransitionRequest>,
) -> Result<Json<Ticket>, StatusCode> {
    let mut pg = state.pg.lock().await;
    let current = get_one(&mut pg, tenant_id, id)
        .await
        .map_err(|e| state.db_fail("ticket get", &e))?
        .ok_or(StatusCode::NOT_FOUND)?;
    let allowed =
        workflow::is_transition_allowed(&mut pg, tenant_id, current.status_id, body.to_state_id)
            .await
            .map_err(|e| state.db_fail("ticket transition check", &e))?;
    if !allowed {
        return Err(StatusCode::UNPROCESSABLE_ENTITY);
    }
    let updated = update_status(&mut pg, tenant_id, id, body.to_state_id)
        .await
        .map_err(|e| state.db_fail("ticket status update", &e))?
        .ok_or(StatusCode::NOT_FOUND)?;
    workflow::record_history_or_log(
        &state,
        &mut pg,
        tenant_id,
        id,
        current.status_id,
        body.to_state_id,
        body.comment,
    )
    .await;
    state.webhooks.push(Event {
        tenant_id,
        event_type: "trouble_status_changed",
        payload: json!({
            "event": "trouble_status_changed",
            "timestamp": (state.now)(),
            "tenant_id": tenant_id,
            "data": {
                "ticket_id": id,
                "ticket_no": updated.ticket_no,
                "from_status_id": current.status_id,
                "to_status_id": body.to_state_id,
            }
        }),
    });
    // TODO(#747 T5): LINE WORKS 通知
    Ok(Json(updated))
}

async fn csv_handler(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(filter): Query<TicketFilter>,
) -> Result<impl IntoResponse, StatusCode> {
    // 全件 (10000 件まで) を 1 ページで取る
    let filter = TicketFilter {
        per_page: Some(10000),
        page: Some(1),
        ..filter
    };
    let mut pg = state.pg.lock().await;
    let response = list(&mut pg, tenant_id, filter)
        .await
        .map_err(|e| state.db_fail("ticket csv", &e))?;
    Ok((
        [
            (header::CONTENT_TYPE, "text/csv; charset=utf-8"),
            (
                header::CONTENT_DISPOSITION,
                "attachment; filename=\"trouble_tickets.csv\"",
            ),
        ],
        csv_bytes(&response.tickets),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_is_from_the_allow_list() {
        assert_eq!(order_sql(None, None), "ticket_no DESC");
        assert_eq!(order_sql(Some("ticket_no"), Some(false)), "ticket_no ASC");
        assert_eq!(order_sql(Some("ticket_no"), None), "ticket_no DESC");
        assert_eq!(
            order_sql(Some("occurred"), Some(false)),
            "occurred_date ASC NULLS LAST, occurred_at ASC NULLS LAST, ticket_no DESC"
        );
        // 許可リストに無い値は文に入らない
        assert_eq!(
            order_sql(Some("id; DROP TABLE x"), Some(false)),
            "ticket_no DESC"
        );
    }

    #[test]
    fn where_numbers_params_in_rust_order() {
        let tenant = Uuid::nil();
        let f = TicketFilter {
            category: Some("c".into()),
            status_id: Some(Uuid::nil()),
            person_name: Some("p".into()),
            company_name: Some("co".into()),
            office_name: Some("o".into()),
            date_from: NaiveDate::from_ymd_opt(2026, 1, 1),
            date_to: NaiveDate::from_ymd_opt(2026, 12, 31),
            q: Some("'; --".into()),
            ..Default::default()
        };
        let (sql, params) = where_clause(&tenant, &f);
        assert_eq!(params.len(), 9);
        let types: Vec<Type> = params.iter().map(|p| p.1.clone()).collect();
        assert_eq!(
            types,
            vec![
                Type::UUID,
                Type::TEXT,
                Type::UUID,
                Type::TEXT,
                Type::TEXT,
                Type::TEXT,
                Type::DATE,
                Type::DATE,
                Type::TEXT
            ]
        );
        assert!(sql.starts_with(
            "tenant_id = $1 AND deleted_at IS NULL AND category = $2 AND status_id = $3 AND \
             person_name ILIKE '%' || $4 || '%' AND company_name ILIKE '%' || $5 || '%' AND \
             office_name ILIKE '%' || $6 || '%' AND occurred_date >= $7 AND occurred_date <= $8 AND \
             (description ILIKE '%' || $9 || '%'"
        ));
        // 値は文に入らない
        assert!(!sql.contains("';"));
        let empty = TicketFilter::default();
        let (sql, params) = where_clause(&tenant, &empty);
        assert_eq!(
            (sql.as_str(), params.len()),
            ("tenant_id = $1 AND deleted_at IS NULL", 1)
        );
    }

    #[test]
    fn empty_strings_become_none() {
        let c: CreateTicket = serde_json::from_value(json!({
            "category": "x", "occurred_at": "", "person_id": null, "assigned_to": "", "due_date": ""
        }))
        .unwrap();
        assert!(c.occurred_at.is_none() && c.person_id.is_none() && c.assigned_to.is_none());
        assert!(c.due_date.is_none());
        let bad =
            serde_json::from_value::<CreateTicket>(json!({"category": "x", "person_id": "nope"}));
        assert!(bad.is_err());
        let u: UpdateTicket = serde_json::from_value(json!({"registration_number": ""})).unwrap();
        assert_eq!(u.registration_number, Some(None));
        let u: UpdateTicket = serde_json::from_value(json!({"registration_number": null})).unwrap();
        assert_eq!(u.registration_number, Some(None));
        let u: UpdateTicket =
            serde_json::from_value(json!({"registration_number": "品川"})).unwrap();
        assert_eq!(u.registration_number, Some(Some("品川".into())));
        let u: UpdateTicket = serde_json::from_value(json!({})).unwrap();
        assert_eq!(u.registration_number, None);
        assert!(serde_json::from_value::<UpdateTicket>(json!({"registration_number": 1})).is_err());
    }
}
