//! テナントの webhook への配信 (rust の `alc_core::webhook` の `fire_event_impl` → `deliver_webhook` と
//! `alc-misc` の repo/webhook.rs (`find_config`・`record_delivery`) の写し)。
//!
//! - 口 (handler) は送らずに [`Outbox`] へ積むだけ。worker 本体が応答を返した後で `ctx.wait_until` に
//!   [`fire_all`] を載せる (rust は handler の中で await していて、応答が待たされていた)
//! - 1 件ごとに: 設定 (`webhook_configs`、tenant × event_type で 1 行、有効なものだけ) を読む → 無ければ何もしない →
//!   送り先を [`validate_webhook_url`] で検査 (rust は書き込み時だけだった。配信時にも見る) → POST を
//!   [`MAX_ATTEMPTS`] 回まで、失敗したら [`RETRY_DELAYS_SECS`] 秒待って打ち直す → 1 回ごとに配信ログ
//!   (`webhook_deliveries`) を 1 行。待ちの数は rust と同じ `[1, 5, 25]` (3 回目の後は待たないので 25 は使われない)
//! - DB の読み書きは 1 回ずつ短い `tenant_tx` に分ける (送信と待ちを transaction の中に挟まない)
//! - 外への HTTP と待ちは [`Http`] の trait 越し (worker は `worker::Fetch` と `worker::Delay`、テストは偽物)
//!
//! ログに出すのは固定の語と DB の kind だけ。送り先の URL・secret・応答の本文はログに出さない
//! (配信ログの表に入れる中身は rust と同じ: 応答の status と本文。届かなかったときは [`SEND_FAILED`])。

use std::sync::{Arc, Mutex as StdMutex};

use alc_worker_db::{PgClient, TxOutput};
use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;
use tokio_postgres::types::Type;
use tokio_postgres::Error;
use uuid::Uuid;

use crate::AppState;

pub const MAX_ATTEMPTS: i32 = 3;
/// 失敗した回の後の待ち (秒)。rust の `deliver_webhook` の `delays` と同じ
pub const RETRY_DELAYS_SECS: [u64; 3] = [1, 5, 25];
/// 届かなかった (DNS・接続・時間切れ) 回の配信ログの本文。rust は reqwest のエラーの文 (送り先の URL を含む) を
/// 入れていたが、ここでは固定の語にする
pub const SEND_FAILED: &str = "error sending request";

/// 送る 1 件 (handler が積む)
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub tenant_id: Uuid,
    pub event_type: &'static str,
    pub payload: Value,
}

/// 応答の後で送る webhook の置き場。1 リクエストに 1 つ (worker が作って `AppState` に入れ、応答の後で [`Outbox::take`] する)
#[derive(Clone, Default)]
pub struct Outbox(Arc<StdMutex<Vec<Event>>>);

impl Outbox {
    pub fn push(&self, event: Event) {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).push(event);
    }

    /// 積まれたものを全部取り出す (取り出した後は空)
    pub fn take(&self) -> Vec<Event> {
        std::mem::take(&mut *self.0.lock().unwrap_or_else(|p| p.into_inner()))
    }
}

/// 外への POST 1 本
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookRequest {
    pub url: String,
    pub headers: Vec<(&'static str, String)>,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookResponse {
    pub status: u16,
    pub body: String,
}

/// 届かなかった (DNS・接続・時間切れ・本文の読み出し)。中身は持たない
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpError;

pub trait Http: Send + Sync {
    /// POST 1 本。**リダイレクトは追わない** (3xx はそのまま返す。rust の `redirect::Policy::none()`)。
    /// 10 秒で打ち切る (rust の reqwest の timeout と同じ)
    fn post(&self, req: WebhookRequest) -> BoxFuture<'_, Result<WebhookResponse, HttpError>>;
    /// 打ち直しの前の待ち
    fn sleep(&self, secs: u64) -> BoxFuture<'_, ()>;
}

/// 配信の設定 (rust の `WebhookConfig` のうち使う列だけ)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub url: String,
    pub secret: Option<String>,
}

impl TxOutput for Config {}

pub mod sql {
    /// (tenant_id, event_type) は UNIQUE なので 0 か 1 行
    pub const FIND_CONFIG: &str = "SELECT id, tenant_id, url, secret FROM alc_api.webhook_configs \
         WHERE tenant_id = $1 AND event_type = $2 AND enabled = TRUE";

    pub const INSERT_DELIVERY: &str = "INSERT INTO alc_api.webhook_deliveries (\
         tenant_id, config_id, event_type, payload, \
         status_code, response_body, attempt, delivered_at, success) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)";
}

pub async fn find_config(
    pg: &mut PgClient,
    tenant_id: Uuid,
    event_type: &'static str,
) -> Result<Option<Config>, Error> {
    pg.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_opt(
                    sql::FIND_CONFIG,
                    &[(&tenant_id, Type::UUID), (&event_type, Type::TEXT)],
                )
                .await?;
            Ok(row.map(|r| Config {
                id: r.get(0),
                tenant_id: r.get(1),
                url: r.get(2),
                secret: r.get(3),
            }))
        })
    })
    .await
}

/// 配信ログの 1 行 (rust の `record_delivery` の引数)
#[derive(Debug, Clone)]
pub struct Delivery {
    pub tenant_id: Uuid,
    pub config_id: Uuid,
    pub event_type: &'static str,
    pub payload: Value,
    pub status_code: Option<i32>,
    pub response_body: Option<String>,
    pub attempt: i32,
    pub delivered_at: Option<DateTime<Utc>>,
    pub success: bool,
}

pub async fn record_delivery(pg: &mut PgClient, d: Delivery) -> Result<(), Error> {
    pg.tenant_tx(d.tenant_id, move |tx| {
        Box::pin(async move {
            tx.execute_typed(
                sql::INSERT_DELIVERY,
                &[
                    (&d.tenant_id, Type::UUID),
                    (&d.config_id, Type::UUID),
                    (&d.event_type, Type::TEXT),
                    (&d.payload, Type::JSONB),
                    (&d.status_code, Type::INT4),
                    (&d.response_body, Type::TEXT),
                    (&d.attempt, Type::INT4),
                    (&d.delivered_at, Type::TIMESTAMPTZ),
                    (&d.success, Type::BOOL),
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
}

/// `sha256=<HMAC-SHA256(secret, body) の小文字の 16 進>` (rust の `X-Webhook-Signature` の値)
pub fn signature(secret: &str, body: &str) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC は鍵の長さを問わない");
    mac.update(body.as_bytes());
    let hex: String = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("sha256={hex}")
}

/// rust の `ReqwestWebhookClient::deliver` が組む要求 (本文は payload の JSON、ヘッダーは
/// `Content-Type`・`X-Webhook-Event`・secret が在れば `X-Webhook-Signature`)
pub fn build_request(
    url: &str,
    event_type: &str,
    payload: &Value,
    secret: Option<&str>,
) -> WebhookRequest {
    let body = payload.to_string();
    let mut headers = vec![
        ("Content-Type", "application/json".to_owned()),
        ("X-Webhook-Event", event_type.to_owned()),
    ];
    if let Some(secret) = secret {
        headers.push(("X-Webhook-Signature", signature(secret, &body)));
    }
    WebhookRequest {
        url: url.to_owned(),
        headers,
        body,
    }
}

/// 積まれた webhook を順に送る (worker が応答の後で `ctx.wait_until` に載せる)
pub async fn fire_all(state: &AppState, http: &dyn Http, events: Vec<Event>) {
    for event in events {
        fire_event(state, http, event).await;
    }
}

/// rust の `fire_event_impl`。設定が無ければ何もしない
pub async fn fire_event(state: &AppState, http: &dyn Http, event: Event) {
    let found = {
        let mut pg = state.pg.lock().await;
        find_config(&mut pg, event.tenant_id, event.event_type).await
    };
    let config = match found {
        Ok(Some(c)) => c,
        Ok(None) => return,
        Err(e) => {
            state.db_fail("webhook config", &e);
            return;
        }
    };
    if !validate_webhook_url(&config.url) {
        (state.log)("trouble: webhook skipped: url rejected");
        return;
    }
    deliver(state, http, &config, event.event_type, &event.payload).await;
}

/// rust の `deliver_webhook`。2xx で止める。届かなかった回・2xx 以外の回は配信ログに失敗として残し、待って打ち直す
async fn deliver(
    state: &AppState,
    http: &dyn Http,
    config: &Config,
    event_type: &'static str,
    payload: &Value,
) {
    for attempt in 1..=MAX_ATTEMPTS {
        let req = build_request(&config.url, event_type, payload, config.secret.as_deref());
        let (status_code, response_body, success) = match http.post(req).await {
            Ok(r) => (
                Some(i32::from(r.status)),
                Some(r.body),
                (200..300).contains(&r.status),
            ),
            Err(HttpError) => (None, Some(SEND_FAILED.to_owned()), false),
        };
        let delivery = Delivery {
            tenant_id: config.tenant_id,
            config_id: config.id,
            event_type,
            payload: payload.clone(),
            status_code,
            response_body,
            attempt,
            delivered_at: success.then(|| (state.now)()),
            success,
        };
        let recorded = {
            let mut pg = state.pg.lock().await;
            record_delivery(&mut pg, delivery).await
        };
        if let Err(e) = recorded {
            state.db_fail("webhook delivery log", &e);
        }
        if success {
            return;
        }
        if attempt < MAX_ATTEMPTS {
            http.sleep(RETRY_DELAYS_SECS[attempt as usize - 1]).await;
        }
    }
}

/// 送り先の URL が SSRF 的に危なくないか (rust の `validate_webhook_url` の写し。Refs ippoan/rust-alc-api#390)。
///
/// - `https` だけ
/// - userinfo 付きは不可
/// - host が IP のリテラルなら loopback / private / link-local / unspecified / broadcast / documentation / CGNAT は不可
/// - host 名が localhost / `*.localhost` / `*.internal` / `*.local` / メタデータの FQDN は不可
///
/// 名前の解決後の IP は見ない (DNS rebinding は rust と同じく対象外)
pub fn validate_webhook_url(raw: &str) -> bool {
    let url = match url::Url::parse(raw) {
        Ok(u) => u,
        Err(_) => return false,
    };
    let host = match url.host_str() {
        Some(h) => h.trim_end_matches('.').to_ascii_lowercase(),
        None => return false,
    };
    if url.scheme() != "https" {
        return false;
    }
    if !url.username().is_empty() || url.password().is_some() {
        return false;
    }
    let ip_candidate = host.trim_start_matches('[').trim_end_matches(']');
    if let Ok(ip) = ip_candidate.parse::<std::net::IpAddr>() {
        return is_global_ip(ip);
    }
    !(host == "localhost"
        || host.ends_with(".localhost")
        || host.ends_with(".internal")
        || host.ends_with(".local")
        || host == "metadata.google.internal")
}

/// 外から届く IP か (loopback / private / link-local / unspecified などでない)
fn is_global_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => {
            !(v4.is_private()
                || v4.is_loopback()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_documentation()
                // CGNAT 100.64.0.0/10
                || (v4.octets()[0] == 100 && (v4.octets()[1] & 0xc0) == 0x40))
        }
        std::net::IpAddr::V6(v6) => {
            !(v6.is_loopback()
                || v6.is_unspecified()
                // unique local fc00::/7
                || (v6.segments()[0] & 0xfe00) == 0xfc00
                // link-local fe80::/10
                || (v6.segments()[0] & 0xffc0) == 0xfe80)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn webhook_url_allows_public_https() {
        assert!(validate_webhook_url("https://hooks.example.com/path"));
        assert!(validate_webhook_url("https://example.co.jp/"));
        assert!(validate_webhook_url("https://example.com./"));
        assert!(validate_webhook_url("https://8.8.8.8/cb"));
        assert!(validate_webhook_url("https://[2001:4860:4860::8888]/"));
    }

    #[test]
    fn webhook_url_rejects_non_https_and_userinfo() {
        assert!(!validate_webhook_url("http://example.com/"));
        assert!(!validate_webhook_url("ftp://example.com/"));
        assert!(!validate_webhook_url("https://user@example.com/"));
        assert!(!validate_webhook_url("https://u:p@example.com/"));
        assert!(!validate_webhook_url("not-a-url"));
        assert!(!validate_webhook_url("data:text/html,evil"));
        assert!(!validate_webhook_url("mailto:admin@example.com"));
    }

    #[test]
    fn webhook_url_rejects_internal_hosts_and_ranges() {
        for url in [
            "https://169.254.169.254/latest/meta-data/",
            "https://metadata.google.internal/",
            "https://127.0.0.1/",
            "https://10.0.0.5/",
            "https://192.168.1.1/",
            "https://172.16.0.1/",
            "https://0.0.0.0/",
            "https://100.64.0.1/",
            "https://255.255.255.255/",
            "https://192.0.2.1/",
            "https://[::1]/",
            "https://[::]/",
            "https://[fc00::1]/",
            "https://[fe80::1]/",
            "https://localhost/",
            "https://foo.localhost/",
            "https://api.internal/",
            "https://db.local/",
        ] {
            assert!(!validate_webhook_url(url), "{url}");
        }
    }

    /// RFC 4231 の test case 2 (鍵 "Jefe")
    #[test]
    fn signature_is_hmac_sha256_hex() {
        assert_eq!(
            signature("Jefe", "what do ya want for nothing?"),
            "sha256=5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn request_headers_and_body() {
        let payload = serde_json::json!({"a": 1});
        let req = build_request("https://example.com/h", "trouble_created", &payload, None);
        assert_eq!(req.body, r#"{"a":1}"#);
        assert_eq!(
            req.headers,
            vec![
                ("Content-Type", "application/json".to_owned()),
                ("X-Webhook-Event", "trouble_created".to_owned()),
            ]
        );
        let signed = build_request("https://example.com/h", "e", &payload, Some("k"));
        assert_eq!(
            signed.headers[2],
            ("X-Webhook-Signature", signature("k", r#"{"a":1}"#))
        );
    }

    #[test]
    fn outbox_take_empties() {
        let outbox = Outbox::default();
        let event = Event {
            tenant_id: Uuid::nil(),
            event_type: "trouble_created",
            payload: Value::Null,
        };
        outbox.clone().push(event.clone());
        assert_eq!(outbox.take(), vec![event]);
        assert!(outbox.take().is_empty());
    }
}
