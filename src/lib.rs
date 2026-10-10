//! トラブル管理の Worker `alc-trouble` (Refs ippoan/rust-alc-api#747)。crates/trouble の口 (`router`、axum) を
//! workers-rs (`http` / `axum` feature) に載せる。型は ippoan/alc-lineworks-worker と同じ。
//! DB への経路 (staging = Workers VPC の先の PgBouncer、本番 = Hyperdrive) は [`db::connect`] の 1 か所で出し分ける。
//! LINE WORKS は ippoan/alc-lineworks-worker (`ALC_LINEWORKS`)、予約通知の alarm は ippoan/nuxt-notify の schedule-alarm
//! (`SCHEDULE_ALARM`。認証は Secrets Store の `INTERNAL_SHARED_SECRET`) を Service Binding で呼ぶ ([`service`])。
//!
//! **この Worker は呼び手を認証しない。** 本番・staging とも workers.dev を開けず (`workers_dev` / `preview_urls` = false、
//! `routes` を持たない)、入口は auth-worker からの Service Binding だけ (wrangler.toml と scripts/check-exposure.sh が保証する)。

// worker 本体は wasm32 専用。native の `cargo build` / `clippy` / `test` (workspace の root で打つもの) では空の crate にして、
// crates/trouble だけを検査させる
#![cfg(target_arch = "wasm32")]

mod db;
mod r2;
mod service;
mod tcp;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{HeaderValue, Response, StatusCode};
use futures_util::lock::Mutex;
use tower_service::Service;
use trouble::alarm::Alarm;
use trouble::service::SharedService;
use trouble::{is_alarm_request, router, AppState, LogSink};
use worker::{console_error, event, Context, Env, HttpRequest, Result, WorkerVersionMetadata};

use crate::r2::R2Store;
use crate::service::ServiceBinding;

/// 添付の R2 の binding (wrangler.toml の `[[r2_buckets]]`。本番と staging で別の bucket)。
/// 無ければ添付の口だけが 503 (ほかの口は動く。rust の storage 無しと同じ)
const TROUBLE_R2_BINDING: &str = "TROUBLE_R2";

/// alc-lineworks-worker の Service Binding。無ければ通知を送らず、メンバーの口は 502
const ALC_LINEWORKS_BINDING: &str = "ALC_LINEWORKS";
/// 予約通知の alarm の DO の Worker の Service Binding
const SCHEDULE_ALARM_BINDING: &str = "SCHEDULE_ALARM";
/// alarm の DO の認証 (Secrets Store。DO の Worker・auth-worker などと同じ secret)
const INTERNAL_SHARED_SECRET_BINDING: &str = "INTERNAL_SHARED_SECRET";

fn service(env: &Env, binding: &str) -> Option<SharedService> {
    env.service(binding)
        .ok()
        .map(|f| Arc::new(ServiceBinding::new(f)) as SharedService)
}

/// alarm (binding と secret の両方が在るときだけ。無ければ登録・取消をしない)。secret は値をログに出さない
async fn alarm(env: &Env) -> Option<Alarm> {
    let service = service(env, SCHEDULE_ALARM_BINDING)?;
    let store = env.secret_store(INTERNAL_SHARED_SECRET_BINDING).ok()?;
    match store.get().await {
        Ok(Some(v)) if !v.is_empty() => Some(Alarm {
            service,
            secret: Arc::from(v),
        }),
        _ => {
            console_error!("trouble: no {INTERNAL_SHARED_SECRET_BINDING} value");
            None
        }
    }
}

fn log_sink() -> LogSink {
    Arc::new(|message| console_error!("{message}"))
}

fn error_response(status: StatusCode, code: &str) -> Response<Body> {
    let mut resp = Response::new(Body::from(format!(r#"{{"error":"{code}"}}"#)));
    *resp.status_mut() = status;
    resp
}

#[event(fetch)]
async fn fetch(req: HttpRequest, env: Env, _ctx: Context) -> Result<Response<Body>> {
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
    // Secrets Store は alarm を使う口 (予約の作成・取消) でだけ引く
    let alarm = if is_alarm_request(req.method().as_str(), req.uri().path()) {
        alarm(&env).await
    } else {
        None
    };
    let state = AppState {
        pg: Arc::new(Mutex::new(pg)),
        log: log_sink(),
        files: env
            .bucket(TROUBLE_R2_BINDING)
            .ok()
            .map(|b| Arc::new(R2Store::new(b)) as trouble::files::SharedStore),
        lineworks: service(&env, ALC_LINEWORKS_BINDING),
        alarm,
    };
    let mut resp = match router().with_state(state).call(req).await {
        Ok(r) => r,
        Err(e) => match e {},
    };
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
