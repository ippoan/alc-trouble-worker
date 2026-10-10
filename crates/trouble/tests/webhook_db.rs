//! webhook の配信を、実 DB (`alc_api_rt` で接続) と偽物の送り先 (support::FakeHttp) で通す。
//! 期待値は rust-alc-api の crates/alc-core/src/webhook.rs の単体テスト (再試行・配信ログ・設定なし・署名・SSRF)。
//!
//! `webhook_configs.event_type` は migration 016 / 022 の CHECK で点呼系の 8 種に限られ、トラブルの
//! `trouble_created` / `trouble_status_changed` の設定は作れない (rust でも同じで、トラブルの webhook は設定が
//! 無く何もしない)。配信の経路はここでは CHECK の中の `alcohol_detected` の設定で確かめる。

mod support;

use serde_json::{json, Value};
use support::*;
use trouble::webhook::{self, Event, HttpError, SEND_FAILED};
use uuid::Uuid;

const EVENT: &str = "alcohol_detected";

/// 設定を 1 行作る (superuser で)
async fn config(tenant: Uuid, url: &str, secret: Option<&str>, enabled: bool) -> Uuid {
    admin()
        .await
        .query_one(
            "INSERT INTO alc_api.webhook_configs (tenant_id, event_type, url, secret, enabled) \
             VALUES ($1, $2, $3, $4, $5) RETURNING id",
            &[&tenant, &EVENT, &url, &secret, &enabled],
        )
        .await
        .unwrap()
        .get(0)
}

/// 配信ログ (attempt, status_code, response_body, success, delivered_at が在るか)
async fn deliveries(config_id: Uuid) -> Vec<(i32, Option<i32>, Option<String>, bool, bool)> {
    admin()
        .await
        .query(
            "SELECT attempt, status_code, response_body, success, delivered_at IS NOT NULL \
             FROM alc_api.webhook_deliveries WHERE config_id = $1 ORDER BY attempt",
            &[&config_id],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4)))
        .collect()
}

fn event(tenant: Uuid) -> Event {
    Event {
        tenant_id: tenant,
        event_type: EVENT,
        payload: json!({"event": EVENT, "data": {"n": 1, "s": "あ"}}),
    }
}

/// 1 回目で 2xx: 要求 (URL・ヘッダー・署名・本文) と配信ログ 1 行 (payload・delivered_at も rust と同じ)
#[tokio::test]
async fn delivers_with_signature_and_logs() {
    let app = app().await;
    let t = tenant().await;
    let c = config(t, "https://hooks.example.com/x", Some("s3cret"), true).await;
    let http = FakeHttp::with(vec![status(204, "")]);
    webhook::fire_all(&app.state, &http, vec![event(t)]).await;

    let sent = http.sent();
    assert_eq!(sent.len(), 1);
    let body = json!({"event": EVENT, "data": {"n": 1, "s": "あ"}}).to_string();
    assert_eq!(sent[0].url, "https://hooks.example.com/x");
    assert_eq!(sent[0].body, body);
    assert_eq!(
        sent[0].headers,
        vec![
            ("Content-Type", "application/json".to_owned()),
            ("X-Webhook-Event", EVENT.to_owned()),
            ("X-Webhook-Signature", webhook::signature("s3cret", &body)),
        ]
    );
    assert!(http.sleeps().is_empty());
    assert_eq!(
        deliveries(c).await,
        vec![(1, Some(204), Some(String::new()), true, true)]
    );
    let row = admin()
        .await
        .query_one(
            "SELECT tenant_id, event_type, payload, delivered_at FROM alc_api.webhook_deliveries WHERE config_id = $1",
            &[&c],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Uuid>(0), t);
    assert_eq!(row.get::<_, String>(1), EVENT);
    assert_eq!(row.get::<_, Value>(2), event(t).payload);
    assert_eq!(row.get::<_, chrono::DateTime<chrono::Utc>>(3), now());
    assert!(app.logs().is_empty());

    // secret の無い設定は署名を付けない
    let t2 = tenant().await;
    config(t2, "https://hooks.example.com/y", None, true).await;
    let http = FakeHttp::default();
    webhook::fire_event(&app.state, &http, event(t2)).await;
    assert_eq!(http.sent()[0].headers.len(), 2);
}

/// 2xx 以外・届かないは 3 回まで、間に 1 秒・5 秒待つ。1 回ごとに配信ログ (失敗は delivered_at が無い)
#[tokio::test]
async fn retries_three_times() {
    let app = app().await;
    let t = tenant().await;
    let c = config(t, "https://hooks.example.com/x", None, true).await;
    // 302 も失敗 (リダイレクトは追わない)
    let http = FakeHttp::with(vec![status(500, "e1"), Err(HttpError), status(302, "")]);
    webhook::fire_event(&app.state, &http, event(t)).await;
    assert_eq!(http.sent().len(), 3);
    assert_eq!(http.sleeps(), vec![1, 5]);
    assert_eq!(
        deliveries(c).await,
        vec![
            (1, Some(500), Some("e1".to_owned()), false, false),
            (2, None, Some(SEND_FAILED.to_owned()), false, false),
            (3, Some(302), Some(String::new()), false, false),
        ]
    );

    // 失敗 → 成功で止まる
    let t2 = tenant().await;
    let c2 = config(t2, "https://hooks.example.com/x", None, true).await;
    let http = FakeHttp::with(vec![status(503, "busy"), status(200, "ok")]);
    webhook::fire_event(&app.state, &http, event(t2)).await;
    assert_eq!(http.sleeps(), vec![1]);
    assert_eq!(
        deliveries(c2).await,
        vec![
            (1, Some(503), Some("busy".to_owned()), false, false),
            (2, Some(200), Some("ok".to_owned()), true, true),
        ]
    );
    // 応答の本文・URL はログに出さない (何も出さない)
    assert!(app.logs().is_empty());
}

/// 設定が無い・無効・別 tenant のもの → 何もしない。チケットの口が積む trouble_created もここ (設定を作れない)
#[tokio::test]
async fn without_config_does_nothing() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;
    config(other, "https://hooks.example.com/x", None, true).await;
    let disabled = tenant().await;
    let c = config(disabled, "https://hooks.example.com/x", None, false).await;

    let got = call(
        &app,
        "POST",
        "/api/trouble/tickets",
        Some(t),
        Some(json!({"category": "その他"})),
    )
    .await;
    assert_eq!(got.status, 201);
    let mut events = app.state.webhooks.take();
    assert_eq!(events.len(), 1);
    events.extend([event(t), event(disabled)]);
    let http = FakeHttp::default();
    webhook::fire_all(&app.state, &http, events).await;
    assert!(http.sent().is_empty());
    assert!(deliveries(c).await.is_empty());
    assert!(app.logs().is_empty());
}

/// 配信時にも送り先を検査する (rust は書き込み時だけ): 内部・http の URL には送らず、ログは固定の語
#[tokio::test]
async fn rejects_internal_url() {
    let app = app().await;
    for url in [
        "https://169.254.169.254/latest/meta-data/",
        "http://hooks.example.com/x",
        "https://localhost/",
    ] {
        let t = tenant().await;
        let c = config(t, url, Some("s"), true).await;
        let http = FakeHttp::default();
        webhook::fire_event(&app.state, &http, event(t)).await;
        assert!(http.sent().is_empty(), "{url}");
        assert!(deliveries(c).await.is_empty(), "{url}");
    }
    assert_eq!(
        app.logs(),
        vec!["trouble: webhook skipped: url rejected"; 3]
    );
}

/// DB の失敗: 設定が読めない → 送らない。配信ログが書けない → 送るのは続け、ログは固定の語と kind だけ
#[tokio::test]
async fn db_failures_are_logged() {
    let t = tenant().await;
    let c = config(t, "https://hooks.example.com/x", Some("s"), true).await;

    let app = build(broken_client().await);
    let http = FakeHttp::default();
    webhook::fire_event(&app.state, &http, event(t)).await;
    assert!(http.sent().is_empty());
    assert_eq!(app.logs().len(), 1);
    assert!(app.logs()[0].starts_with("trouble: webhook config failed: db ("));

    let app = build(client_as(READ_ONLY_ROLE).await);
    let http = FakeHttp::with(vec![status(500, ""), status(200, "")]);
    webhook::fire_event(&app.state, &http, event(t)).await;
    assert_eq!(http.sent().len(), 2);
    assert_eq!(
        app.logs(),
        vec!["trouble: webhook delivery log failed: db (42501)"; 2]
    );
    assert!(deliveries(c).await.is_empty());
    for line in app.logs() {
        assert!(!line.contains(&t.to_string()) && !line.contains(&c.to_string()));
    }
}
