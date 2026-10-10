//! トラブル管理の Worker `alc-trouble` (Refs ippoan/rust-alc-api#747)。crates/trouble の口 (`router`、axum) を
//! workers-rs (`http` / `axum` feature) に載せる。型は ippoan/alc-lineworks-worker と同じ。
//! DB への経路 (staging = Workers VPC の先の PgBouncer、本番 = Hyperdrive) は [`db::connect`] の 1 か所で出し分ける。
//! 口が積んだ webhook (`AppState::webhooks`) は、応答を返した後で `ctx.wait_until` に載せて送る
//! (打ち直しの待ちで応答を待たせない。外への POST は [`http::WorkerHttp`])。
//!
//! **この Worker は呼び手を認証しない。** 本番・staging とも workers.dev を開けず (`workers_dev` / `preview_urls` = false、
//! `routes` を持たない)、入口は auth-worker からの Service Binding だけ (wrangler.toml と scripts/check-exposure.sh が保証する)。

// worker 本体は wasm32 専用。native の `cargo build` / `clippy` / `test` (workspace の root で打つもの) では空の crate にして、
// crates/trouble だけを検査させる
#![cfg(target_arch = "wasm32")]

mod db;
mod http;
mod tcp;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{HeaderValue, Response, StatusCode};
use chrono::{DateTime, Utc};
use futures_util::lock::Mutex;
use tower_service::Service;
use trouble::webhook::{self, Outbox};
use trouble::{router, AppState, LogSink};
use worker::{
    console_error, event, Context, Date, Env, HttpRequest, Result, WorkerVersionMetadata,
};

fn log_sink() -> LogSink {
    Arc::new(|message| console_error!("{message}"))
}

/// `Date.now()` (ms) を UTC の時刻に (範囲の外は起こらないが、起きたら 1970-01-01)
fn now() -> DateTime<Utc> {
    i64::try_from(Date::now().as_millis())
        .ok()
        .and_then(DateTime::from_timestamp_millis)
        .unwrap_or_default()
}

fn error_response(status: StatusCode, code: &str) -> Response<Body> {
    let mut resp = Response::new(Body::from(format!(r#"{{"error":"{code}"}}"#)));
    *resp.status_mut() = status;
    resp
}

#[event(fetch)]
async fn fetch(req: HttpRequest, env: Env, ctx: Context) -> Result<Response<Body>> {
    // 全リクエストで routing の前に繋ぐ (ippoan/alc-lineworks-worker と同じ)
    let pg = match db::connect(&env).await {
        Ok(c) => c,
        Err(db::ConnectError::NotConfigured) => {
            console_error!("trouble: {}", db::ConnectError::NotConfigured);
            return Ok(error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "database_not_configured",
            ));
        }
        Err(e) => {
            console_error!("trouble: {e}");
            return Ok(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
            ));
        }
    };
    let outbox = Outbox::default();
    let state = AppState {
        pg: Arc::new(Mutex::new(pg)),
        log: log_sink(),
        now: Arc::new(now),
        webhooks: outbox.clone(),
    };
    let mut resp = match router().with_state(state.clone()).call(req).await {
        Ok(r) => r,
        Err(e) => match e {},
    };
    // 口が積んだ webhook は応答の後で送る (DB の接続は同じものを使う。口はもう終わっているので取り合わない)
    let events = outbox.take();
    if !events.is_empty() {
        ctx.wait_until(async move {
            webhook::fire_all(&state, &http::WorkerHttp, events).await;
        });
    }
    // どの版が応えたかを応答ヘッダーで分かるようにする。binding が取れなければ何も付けない
    if let Ok(meta) = env.get_binding::<WorkerVersionMetadata>("CF_VERSION_METADATA") {
        if let Ok(v) = HeaderValue::from_str(&meta.id()) {
            resp.headers_mut().insert("x-worker-version", v);
        }
        let tag = meta.tag();
        if !tag.is_empty() {
            if let Ok(v) = HeaderValue::from_str(&tag) {
                resp.headers_mut().insert("x-worker-tag", v);
            }
        }
    }
    Ok(resp)
}
