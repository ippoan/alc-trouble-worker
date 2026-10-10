//! 別の Worker への Service Binding の 1 リクエスト (Refs ippoan/rust-alc-api#747)。
//!
//! この Worker が呼ぶのは 2 つ: LINE WORKS の送信・メンバーの ippoan/alc-lineworks-worker (`ALC_LINEWORKS`、[`crate::lineworks`]) と、
//! 予約通知の alarm の DO の Worker (ippoan/nuxt-notify の `workers/schedule-alarm`、`SCHEDULE_ALARM`、[`crate::alarm`])。
//! 口の側は [`Service`] を呼ぶだけで、Worker の実装は直下の `src/service.rs` (Fetcher)、テストはモックに差し替える
//! (型は ippoan/alc-notify-worker の `LineworksApi` と同じ)。
//!
//! **await を `tenant_tx` の閉包の中に入れない** (DB の transaction を他の Worker の応答待ちで延ばさない)。

use std::sync::Arc;

use futures_util::future::BoxFuture;

/// HTTP の method (呼ぶ口が使うものだけ)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
    Put,
    Delete,
}

/// 1 リクエスト。`path` は path と query (host は binding が決めるので持たない)。
/// `body` が在れば JSON (`Content-Type: application/json`) で送る
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceRequest {
    pub method: Method,
    pub path: String,
    pub headers: Vec<(&'static str, String)>,
    pub body: Option<String>,
}

/// 応答の status と本文のバイト列
#[derive(Debug, Clone)]
pub struct ServiceResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

/// 相手に届かなかった (fetch の失敗・本文が読めない)。中身はログに出す段の label
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unreachable(pub &'static str);

/// Service Binding (axum の state と handler が要るので `Send + Sync` で `Send` な future を返す)
pub trait Service: Send + Sync {
    fn call(&self, req: ServiceRequest) -> BoxFuture<'_, Result<ServiceResponse, Unreachable>>;
}

pub type SharedService = Arc<dyn Service>;
