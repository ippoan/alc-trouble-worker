//! トラブル管理の口 (Refs ippoan/rust-alc-api#747)。rust-alc-api の `crates/alc-trouble` を移す先で、
//! path・method・JSON の形・status は rust と同じ (nuxt-trouble がそのまま使う)。
//!
//! - [`master`] — 名前と並び順だけのマスタ 4 本 (categories / offices / progress-statuses / task-types)。
//!   rust の `alc_core::master_data` と `master_handlers` の写し
//! - [`task_statuses`] — タスクの状態のマスタ (key・色・完了扱い)
//! - [`field_layouts`] — チケットの入力欄の表示設定
//! - [`notifications`] — 通知の設定
//! - [`tickets`] — チケット (CRUD・一覧・CSV・状態の遷移)
//! - [`workflow`] — チケットの状態と遷移の定義・既定の作成・履歴
//! - [`tasks`] — チケットのタスク (経過記録。CRUD・横断の一覧・並べ替え)
//! - [`files`] — チケット・タスクの添付 (R2 に置く。置き場は [`files::FileStore`] で差し替える)
//! - [`schedules`] — 予約通知 (作成・一覧・取消と、alarm の DO が呼ぶ発火の口)。alarm は [`alarm`]
//! - [`lineworks`] — LINE WORKS の通知 (チケット・タスク・予約) とメンバーの一覧。ippoan/alc-lineworks-worker を呼ぶ
//!
//! 別の Worker (alc-lineworks-worker・alarm の DO) は Service Binding で呼ぶ ([`service::Service`]。テストはモック)。
//!
//! 発火の口 (`/api/internal/trouble/schedules/{id}/fire`) を除く全部の口が `X-Tenant-ID` を要る (auth-worker が付ける。無い・UUID でない → 401。
//! `alc_core_wasm::require_tenant_header`)。rust と同じく role は見ない。
//! DB は `PgClient::tenant_tx` + `query_typed` 系だけ (名前付き prepared statement は使わない)。
//! ログ・エラー応答は固定の語だけ (エラーの文・識別子を載せない。DB のエラーは `kind` = SQLSTATE か固定の label)。

pub mod alarm;
pub mod field_layouts;
pub mod files;
pub mod lineworks;
pub mod master;
pub mod notifications;
pub mod schedules;
pub mod service;
pub mod task_statuses;
pub mod tasks;
pub mod tickets;
pub mod workflow;

use std::sync::Arc;

use alc_core_wasm::require_tenant_header;
use alc_worker_db::{kind, PgClient};
use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::{middleware, Router};
use futures_util::lock::Mutex;

pub type LogSink = Arc<dyn Fn(&str) + Send + Sync>;

#[derive(Clone)]
pub struct AppState {
    pub pg: Arc<Mutex<PgClient>>,
    pub log: LogSink,
    /// 添付の置き場 (R2 の binding `TROUBLE_R2`)。無ければ添付の読み書きは 503 (rust の storage 無しと同じ)
    pub files: Option<files::SharedStore>,
    /// alc-lineworks-worker (Service Binding `ALC_LINEWORKS`)。無ければ通知は送らず、メンバーの口は 502
    pub lineworks: Option<service::SharedService>,
    /// 予約通知の alarm (Service Binding `SCHEDULE_ALARM` と secret `INTERNAL_SHARED_SECRET`)。
    /// どちらかが無ければ None で、alarm の登録・取消をしない (rust の `SCHEDULE_ALARM_URL` 未設定と同じ)
    pub alarm: Option<alarm::Alarm>,
}

impl AppState {
    /// DB の失敗を固定の語でログに出し、500 にする
    fn db_fail(&self, stage: &str, e: &tokio_postgres::Error) -> StatusCode {
        (self.log)(&format!("trouble: {stage} failed: db ({})", kind(e)));
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

/// 一意制約などの制約違反か (rust の `master_handlers::is_conflict` と同じ: 制約の名前が付いた DB のエラー)。
/// 呼び手は 409 にし、ログに出さない (利用者の入力が原因なので)
pub fn is_conflict(e: &tokio_postgres::Error) -> bool {
    e.as_db_error().is_some_and(|db| db.constraint().is_some())
}

/// alarm を使う口 (予約の作成・取消) へのリクエストか。worker はこのときだけ Secrets Store から
/// `INTERNAL_SHARED_SECRET` を引く (ほかの口で Secrets Store を待たない)
pub fn is_alarm_request(method: &str, path: &str) -> bool {
    match method {
        "POST" => path == "/api/trouble/schedules",
        "DELETE" => path.starts_with("/api/trouble/schedules/"),
        _ => false,
    }
}

/// 本文の上限 (rust の main.rs の `DefaultBodyLimit` と同じ 20MB。添付の multipart と JSON に効く)
pub const BODY_LIMIT: usize = 20 * 1024 * 1024;

/// 全部の口。rust と同じく `/api` の下 (auth-worker は `/api/trouble/...` のまま Service Binding に渡す)
pub fn router() -> Router<AppState> {
    let trouble = Router::new()
        .merge(master::router::<master::Categories>())
        .merge(master::router::<master::Offices>())
        .merge(master::router::<master::ProgressStatuses>())
        .merge(master::router::<master::TaskTypes>())
        .merge(task_statuses::router())
        .merge(field_layouts::router())
        .merge(notifications::router())
        .merge(tickets::router())
        .merge(workflow::router())
        .merge(tasks::router())
        .merge(files::router())
        .merge(schedules::router())
        .merge(lineworks::router())
        .route_layer(middleware::from_fn(require_tenant_header))
        // 発火の口は tenant の layer の外 (X-Tenant-ID は来ない。tenant は予約の行から取る)
        .merge(schedules::internal_router());
    Router::new()
        .nest("/api", trouble)
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
}

#[cfg(test)]
mod tests {
    use super::is_alarm_request;

    #[test]
    fn alarm_request_is_schedule_create_and_cancel_only() {
        assert!(is_alarm_request("POST", "/api/trouble/schedules"));
        assert!(is_alarm_request("DELETE", "/api/trouble/schedules/x"));
        assert!(!is_alarm_request("GET", "/api/trouble/schedules"));
        assert!(!is_alarm_request("POST", "/api/trouble/schedules/x"));
        assert!(!is_alarm_request("DELETE", "/api/trouble/tickets/x"));
        assert!(!is_alarm_request(
            "POST",
            "/api/internal/trouble/schedules/x/fire"
        ));
    }
}
