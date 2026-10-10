//! LINE WORKS の通知とメンバー (rust の `alc-trouble` の notifier.rs・lineworks_members.rs の置き換え)。
//!
//! LINE WORKS の API はこの Worker では叩かず、ippoan/alc-lineworks-worker を Service Binding `ALC_LINEWORKS` で呼ぶ
//! ([`crate::service`])。tenant は `X-Tenant-ID` で渡し、`bot_config_id` は渡さない (= tenant の有効な既定の bot。
//! rust の `resolve_lineworks_config` と同じ選び方を lineworks-worker が持つ)。Bot の秘密値はこの Worker に来ない。
//!
//! - [`notify`] — 宛先の LINE WORKS の user_id ごとに `POST /api/internal/lineworks/messages`
//!   (`{"to": {"user_id"}, "content": {"type": "text", "text"}}`)。失敗はログ (固定の語) だけで、呼び手の応答は変えない
//!   (rust と同じ)。binding が無ければ送らない (rust の notifier 無しと同じ)
//! - `GET /trouble/lineworks/members` — lineworks-worker の `GET /api/internal/lineworks/users` の中継。
//!   応答は rust の `LineworksMember` と同じ `[{user_id, user_name, email}]`。
//!   **rust と違う**: lineworks-worker が断ったときは、その status と語 (`error`、`missing_scope` の `scope` など) を写す
//!   ([`relay_error`]。rust は bot が無い → 503・一覧の失敗 → 500 で本文なし)。届かない・binding 無し・本文が読めない → 502
//!
//! ログは段の名前と、届かない理由の label か status だけ (宛先の user_id・本文・tenant を出さない)。

use alc_core_wasm::TenantId;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::service::{Method, ServiceRequest, Unreachable};
use crate::AppState;

pub const MEMBERS_PATH: &str = "/trouble/lineworks/members";

/// lineworks-worker の口 (path は lineworks-worker の `crates/lineworks/src/routes.rs` と同じ)
pub const LW_MESSAGES_PATH: &str = "/api/internal/lineworks/messages";
pub const LW_USERS_PATH: &str = "/api/internal/lineworks/users";

pub type ApiError = (StatusCode, Json<Value>);

pub fn router() -> Router<AppState> {
    Router::new().route(MEMBERS_PATH, get(members))
}

fn request(method: Method, path: &str, tenant_id: Uuid, body: Option<String>) -> ServiceRequest {
    let mut headers = vec![
        ("X-Tenant-ID", tenant_id.to_string()),
        ("Accept", "application/json".to_owned()),
    ];
    if body.is_some() {
        headers.push(("Content-Type", "application/json".to_owned()));
    }
    ServiceRequest {
        method,
        path: path.to_owned(),
        headers,
        body,
    }
}

/// 1 人宛のテキストの body (`bot_config_id` は付けない = tenant の既定の bot)
pub fn message_body(user_id: &str, text: &str) -> String {
    json!({
        "to": {"user_id": user_id},
        "content": {"type": "text", "text": text},
    })
    .to_string()
}

/// `user_ids` の各人へ `message` を送る。失敗は `event_type` (固定の語) を付けてログに出すだけ
pub async fn notify(
    state: &AppState,
    tenant_id: Uuid,
    event_type: &str,
    message: &str,
    user_ids: &[String],
) {
    let Some(lw) = &state.lineworks else {
        return;
    };
    for user_id in user_ids {
        let req = request(
            Method::Post,
            LW_MESSAGES_PATH,
            tenant_id,
            Some(message_body(user_id, message)),
        );
        let why = match lw.call(req).await {
            Ok(r) if (200..300).contains(&r.status) => continue,
            Ok(r) => format!("status {}", r.status),
            Err(Unreachable(l)) => l.to_owned(),
        };
        (state.log)(&format!(
            "trouble: {event_type} notify failed: lineworks ({why})"
        ));
    }
}

/// rust の `LineworksMember` と同じ形
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Member {
    pub user_id: String,
    pub user_name: Option<String>,
    pub email: Option<String>,
}

/// 上流の 502 (文を載せない)
fn upstream_error() -> ApiError {
    (
        StatusCode::BAD_GATEWAY,
        Json(json!({"error": "upstream_error"})),
    )
}

/// 応答に写してよい語 (小文字・数字・`_`・`.` だけの短い文字列)。自由文は通さない
fn word(v: &Value, key: &str) -> Option<String> {
    let s = v.get(key)?.as_str()?;
    let ok = !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'.');
    ok.then(|| s.to_owned())
}

/// lineworks-worker が断った応答を写す (ippoan/alc-notify-worker の `relay_error` と同じ): status (4xx / 5xx でなければ 502) と
/// `error` (語でなければ `upstream_error`)。`missing_scope` は `scope` と固定の `message` を付け、
/// ほかは `message` が語のときだけ写す
pub fn relay_error(status: u16, body: &[u8]) -> ApiError {
    let status = StatusCode::from_u16(status)
        .ok()
        .filter(|s| s.is_client_error() || s.is_server_error())
        .unwrap_or(StatusCode::BAD_GATEWAY);
    let v: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let error = word(&v, "error").unwrap_or_else(|| "upstream_error".to_owned());
    let mut out = json!({"error": error});
    if error == "missing_scope" {
        if let Some(scope) = word(&v, "scope") {
            out["message"] = json!(format!(
                "LINE WORKS Developer Console で {scope} scope を追加してください"
            ));
            out["scope"] = json!(scope);
        }
    } else if let Some(message) = word(&v, "message") {
        out["message"] = json!(message);
    }
    (status, Json(out))
}

async fn members(
    State(state): State<AppState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Vec<Member>>, ApiError> {
    let fail = |why: String| {
        (state.log)(&format!(
            "trouble: lineworks members failed: lineworks ({why})"
        ));
    };
    let Some(lw) = &state.lineworks else {
        fail("not_configured".to_owned());
        return Err(upstream_error());
    };
    let resp = lw
        .call(request(Method::Get, LW_USERS_PATH, tenant_id, None))
        .await
        .map_err(|Unreachable(l)| {
            fail(l.to_owned());
            upstream_error()
        })?;
    if !(200..300).contains(&resp.status) {
        fail(format!("status {}", resp.status));
        return Err(relay_error(resp.status, &resp.body));
    }
    serde_json::from_slice(&resp.body).map(Json).map_err(|_| {
        fail("body".to_owned());
        upstream_error()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_body_has_no_bot_config_id() {
        let v: Value = serde_json::from_str(&message_body("u1", "本文")).unwrap();
        assert_eq!(
            v,
            json!({"to": {"user_id": "u1"}, "content": {"type": "text", "text": "本文"}})
        );
    }

    #[test]
    fn relay_error_copies_words_only() {
        let (s, Json(v)) = relay_error(
            403,
            br#"{"error":"missing_scope","scope":"directory.read","message":"free text"}"#,
        );
        assert_eq!(s, StatusCode::FORBIDDEN);
        assert_eq!(
            v,
            json!({"error": "missing_scope", "scope": "directory.read",
                   "message": "LINE WORKS Developer Console で directory.read scope を追加してください"})
        );
        // scope が語でない missing_scope は error だけ
        let (_, Json(v)) = relay_error(403, br#"{"error":"missing_scope","scope":"A B"}"#);
        assert_eq!(v, json!({"error": "missing_scope"}));
        // 語の message は写し、自由文は捨てる
        let (s, Json(v)) = relay_error(404, br#"{"error":"bot_config_not_found","message":"x_y"}"#);
        assert_eq!(s, StatusCode::NOT_FOUND);
        assert_eq!(
            v,
            json!({"error": "bot_config_not_found", "message": "x_y"})
        );
        let (_, Json(v)) = relay_error(500, br#"{"error":"Internal Error!","message":"a b"}"#);
        assert_eq!(v, json!({"error": "upstream_error"}));
        // 2xx / 3xx の status・読めない本文は 502 upstream_error
        let (s, Json(v)) = relay_error(302, b"<html>");
        assert_eq!(s, StatusCode::BAD_GATEWAY);
        assert_eq!(v, json!({"error": "upstream_error"}));
        let long = format!(r#"{{"error":"{}"}}"#, "a".repeat(65));
        assert_eq!(
            relay_error(400, long.as_bytes()).1 .0,
            json!({"error": "upstream_error"})
        );
        assert_eq!(
            relay_error(400, br#"{"error":""}"#).1 .0,
            json!({"error": "upstream_error"})
        );
    }
}
