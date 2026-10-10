//! 予約通知の alarm の登録・取消 (rust の `alc-trouble` の worker_alarm.rs の写し)。
//!
//! 宛先は ippoan/nuxt-notify の `workers/schedule-alarm` (DO)。rust は `SCHEDULE_ALARM_URL` へ HTTPS で叩いていたが、
//! ここでは Service Binding `SCHEDULE_ALARM` ([`crate::service`]) で呼ぶ (path・body・ヘッダは rust と同じ):
//!
//! - 登録: `PUT /alarms/{id}` body `{"fire_at": "<RFC3339>"}`。成功なら `alarms/{id}` を返す
//!   (呼び手が `cloud_task_name` に保存し、取消の宛先に使う。rust と同じ)
//! - 取消: `DELETE /{cloud_task_name}`。404 は既に消えている (DO の DELETE は冪等) ので成功扱い
//!
//! 認証は `X-Internal-Shared-Secret` (Secrets Store の `INTERNAL_SHARED_SECRET`。DO の Worker と同じ secret)。
//! binding か secret が無ければ [`crate::AppState::alarm`] が None で、登録も取消もしない
//! (rust の「`SCHEDULE_ALARM_URL` 未設定なら登録しない」と同じ)。
//! 発火は DO → auth-worker の alc-internal-proxy → この Worker の
//! `POST /api/internal/trouble/schedules/{id}/fire` ([`crate::schedules`])。

use std::sync::Arc;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::service::{Method, ServiceRequest, SharedService, Unreachable};

pub const SECRET_HEADER: &str = "X-Internal-Shared-Secret";

/// alarm の DO の Worker と、その認証の secret
#[derive(Clone)]
pub struct Alarm {
    pub service: SharedService,
    pub secret: Arc<str>,
}

/// alarm の失敗 (ログに出す語: 届かない段の label か status)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlarmError {
    Unreachable(&'static str),
    Status(u16),
}

impl std::fmt::Display for AlarmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unreachable(l) => write!(f, "alarm ({l})"),
            Self::Status(s) => write!(f, "alarm (status {s})"),
        }
    }
}

/// 登録の口の body (rust と同じ `to_rfc3339`)
pub fn register_body(fire_at: DateTime<Utc>) -> String {
    serde_json::json!({ "fire_at": fire_at.to_rfc3339() }).to_string()
}

impl Alarm {
    async fn call(
        &self,
        method: Method,
        path: String,
        body: Option<String>,
    ) -> Result<u16, AlarmError> {
        let req = ServiceRequest {
            method,
            path,
            headers: vec![(SECRET_HEADER, self.secret.to_string())],
            body,
        };
        self.service
            .call(req)
            .await
            .map(|r| r.status)
            .map_err(|Unreachable(l)| AlarmError::Unreachable(l))
    }

    /// 登録。成功なら `cloud_task_name` に保存する `alarms/{id}`
    pub async fn register(
        &self,
        schedule_id: Uuid,
        fire_at: DateTime<Utc>,
    ) -> Result<String, AlarmError> {
        let task_name = format!("alarms/{schedule_id}");
        let status = self
            .call(
                Method::Put,
                format!("/{task_name}"),
                Some(register_body(fire_at)),
            )
            .await?;
        if (200..300).contains(&status) {
            Ok(task_name)
        } else {
            Err(AlarmError::Status(status))
        }
    }

    /// 取消。`task_name` は [`Self::register`] が返した `alarms/{id}`。404 も成功
    pub async fn cancel(&self, task_name: &str) -> Result<(), AlarmError> {
        let status = self
            .call(Method::Delete, format!("/{task_name}"), None)
            .await?;
        if (200..300).contains(&status) || status == 404 {
            Ok(())
        } else {
            Err(AlarmError::Status(status))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_body_is_rfc3339() {
        let at = DateTime::parse_from_rfc3339("2026-07-10T04:00:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            register_body(at),
            r#"{"fire_at":"2026-07-10T04:00:00+00:00"}"#
        );
    }

    #[test]
    fn error_words() {
        assert_eq!(
            AlarmError::Unreachable("fetch").to_string(),
            "alarm (fetch)"
        );
        assert_eq!(AlarmError::Status(401).to_string(), "alarm (status 401)");
    }
}
