//! [`trouble::service::Service`] の Workers での実装: Service Binding の Fetcher (`ALC_LINEWORKS`・`SCHEDULE_ALARM`)。
//! 型は ippoan/alc-notify-worker の `src/lineworks.rs` の `LineworksBinding` と同じ (Fetcher と future は JS の値で
//! `Send` でないので、`worker::send` の `SendWrapper`・`SendFuture` で包む。Workers は単一 thread)。
//!
//! エラーは段の label だけに落とす (ランタイムの生のエラー文・本文はログに出さない)。
//! このファイルは wasm 専用で、CI のテストと coverage の gate の外 (確かめるのは staging / 本番の実物)。

use futures_util::future::BoxFuture;
use trouble::service::{Method, Service, ServiceRequest, ServiceResponse, Unreachable};
use wasm_bindgen::JsValue;
use worker::send::{SendFuture, SendWrapper};
use worker::{Fetcher, Headers, Request, RequestInit};

/// URL の host は名目の値 (Service Binding は host で経路が決まらない)
const NOMINAL_ORIGIN: &str = "https://service-binding";
/// 応答の本文の上限 (組織のメンバーの全件でも十分に収まる大きさ)
const BODY_LIMIT: usize = 16 * 1024 * 1024;

pub struct ServiceBinding(SendWrapper<Fetcher>);

impl ServiceBinding {
    pub fn new(fetcher: Fetcher) -> Self {
        Self(SendWrapper::new(fetcher))
    }
}

impl Service for ServiceBinding {
    fn call(&self, req: ServiceRequest) -> BoxFuture<'_, Result<ServiceResponse, Unreachable>> {
        Box::pin(SendFuture::new(async move {
            let headers = Headers::new();
            for (name, value) in &req.headers {
                headers
                    .set(name, value)
                    .map_err(|_| Unreachable("headers"))?;
            }
            if req.body.is_some() {
                headers
                    .set("Content-Type", "application/json")
                    .map_err(|_| Unreachable("headers"))?;
            }
            let method = match req.method {
                Method::Get => worker::Method::Get,
                Method::Post => worker::Method::Post,
                Method::Put => worker::Method::Put,
                Method::Delete => worker::Method::Delete,
            };
            let mut init = RequestInit::new();
            init.with_method(method)
                .with_headers(headers)
                .with_body(req.body.as_deref().map(JsValue::from_str));
            let request = Request::new_with_init(&format!("{NOMINAL_ORIGIN}{}", req.path), &init)
                .map_err(|_| Unreachable("request"))?;
            let resp = self
                .0
                .fetch_request(request)
                .await
                .map_err(|_| Unreachable("fetch"))?;
            let status = resp.status().as_u16();
            let body = axum::body::to_bytes(axum::body::Body::new(resp.into_body()), BODY_LIMIT)
                .await
                .map_err(|_| Unreachable("body"))?;
            Ok(ServiceResponse {
                status,
                body: body.to_vec(),
            })
        }))
    }
}
