//! crates/trouble の webhook の [`Http`] の Workers での実装 (ippoan/alc-lineworks-worker の `src/http.rs` の型)。
//! POST は `redirect: "manual"` で、3xx をそのまま返す (rust の `redirect::Policy::none()` と同じ。302 で内部へ
//! 回されるのを防ぐ)。[`TIMEOUT_SECS`] 秒で打ち切る (rust の reqwest の timeout と同じ)。待ちは `worker::Delay`。
//!
//! 値と future は JS の値で `Send` でないので、`worker::send` の `SendFuture` で包む (Workers は単一 thread)。
//! エラーは中身を持たない [`HttpError`] に落とす (URL・ヘッダー・ランタイムの生のエラー文をログに出さない)。
//!
//! このファイルは wasm 専用で、CI のテストと coverage の gate の外 (確かめるのは staging / 本番の実物)。

use std::time::Duration;

use futures_util::future::{select, BoxFuture, Either};
use trouble::webhook::{Http, HttpError, WebhookRequest, WebhookResponse};
use wasm_bindgen::JsValue;
use worker::send::SendFuture;
use worker::{Delay, Fetch, Headers, Method, Request, RequestInit, RequestRedirect};

/// 1 回の POST の上限 (秒)
const TIMEOUT_SECS: u64 = 10;

pub struct WorkerHttp;

impl Http for WorkerHttp {
    fn post(&self, req: WebhookRequest) -> BoxFuture<'_, Result<WebhookResponse, HttpError>> {
        Box::pin(SendFuture::new(async move {
            let send = Box::pin(send(req));
            let timeout = Delay::from(Duration::from_secs(TIMEOUT_SECS));
            match select(send, timeout).await {
                Either::Left((result, _)) => result,
                Either::Right(_) => Err(HttpError),
            }
        }))
    }

    fn sleep(&self, secs: u64) -> BoxFuture<'_, ()> {
        Box::pin(SendFuture::new(Delay::from(Duration::from_secs(secs))))
    }
}

async fn send(req: WebhookRequest) -> Result<WebhookResponse, HttpError> {
    let headers = Headers::new();
    for (name, value) in &req.headers {
        headers.set(name, value).map_err(|_| HttpError)?;
    }
    let mut init = RequestInit::new();
    init.with_method(Method::Post)
        .with_headers(headers)
        .with_redirect(RequestRedirect::Manual)
        .with_body(Some(JsValue::from_str(&req.body)));
    let request = Request::new_with_init(&req.url, &init).map_err(|_| HttpError)?;
    let mut resp = Fetch::Request(request)
        .send()
        .await
        .map_err(|_| HttpError)?;
    let status = resp.status_code();
    let body = resp.text().await.map_err(|_| HttpError)?;
    Ok(WebhookResponse { status, body })
}
