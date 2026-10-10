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
//!
//! 全部の口が `X-Tenant-ID` を要る (auth-worker が付ける。無い・UUID でない → 401。
//! `alc_core_wasm::require_tenant_header`)。rust と同じく role は見ない。
//! DB は `PgClient::tenant_tx` + `query_typed` 系だけ (名前付き prepared statement は使わない)。
//! ログ・エラー応答は固定の語だけ (エラーの文・識別子を載せない。DB のエラーは `kind` = SQLSTATE か固定の label)。

pub mod field_layouts;
pub mod master;
pub mod notifications;
pub mod task_statuses;
pub mod tickets;
pub mod workflow;

use std::sync::Arc;

use alc_core_wasm::require_tenant_header;
use alc_worker_db::{kind, PgClient};
use axum::http::StatusCode;
use axum::{middleware, Router};
use futures_util::lock::Mutex;

pub type LogSink = Arc<dyn Fn(&str) + Send + Sync>;

#[derive(Clone)]
pub struct AppState {
    pub pg: Arc<Mutex<PgClient>>,
    pub log: LogSink,
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
        .route_layer(middleware::from_fn(require_tenant_header));
    Router::new().nest("/api", trouble)
}
