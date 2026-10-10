//! 予約通知の口 (作成・一覧・取消・発火) と alarm の登録・取消を、実 DB (`alc_api_rt` で接続) で通す。
//! alarm の DO と alc-lineworks-worker はモック (tests/support の `MockService`)。期待値は rust-alc-api の
//! schedules.rs・worker_alarm.rs (のテスト) に合わせる。
//! 接続先は `TROUBLE_TEST_ADMIN_DATABASE_URL` (無ければ失敗する。tests/support)。

mod support;

use std::sync::atomic::Ordering;

use chrono::{DateTime, Duration, TimeZone, Utc};
use serde_json::{json, Value};
use support::*;
use trouble::service::Method;
use uuid::Uuid;

const SCHEDULE_KEYS: &[&str] = &[
    "cloud_task_name",
    "created_at",
    "created_by",
    "id",
    "lineworks_user_ids",
    "message",
    "scheduled_at",
    "sent_at",
    "status",
    "tenant_id",
    "ticket_id",
];

fn id_of(v: &Value) -> String {
    v["id"].as_str().unwrap().to_owned()
}

/// ログに識別子・宛先が出ていないこと (固定の語と kind だけ)
fn assert_logs_clean(app: &App, words: &[String]) {
    for line in app.logs() {
        assert!(line.starts_with("trouble: "), "固定の頭で始まらないログ");
        for w in words {
            assert!(!line.contains(w.as_str()), "ログに識別子が出た");
        }
    }
}

async fn ticket(app: &App, t: Uuid, body: Value) -> String {
    let got = call(app, "POST", "/api/trouble/tickets", Some(t), Some(body)).await;
    assert_eq!(got.status, 201);
    id_of(&got.json())
}

fn later(hours: i64) -> String {
    (Utc::now() + Duration::hours(hours)).to_rfc3339()
}

async fn create(app: &App, t: Uuid, ticket: &str, at: &str, to: &[&str]) -> Value {
    let got = call(
        app,
        "POST",
        "/api/trouble/schedules",
        Some(t),
        Some(json!({"ticket_id": ticket, "scheduled_at": at, "message": "点検の確認", "lineworks_user_ids": to})),
    )
    .await;
    assert_eq!(got.status, 201, "{}", String::from_utf8_lossy(&got.body));
    got.json()
}

async fn list(app: &App, t: Uuid, ticket: &str) -> Vec<Value> {
    let got = call(
        app,
        "GET",
        &format!("/api/trouble/tickets/{ticket}/schedules"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 200);
    got.json().as_array().unwrap().clone()
}

async fn status_of(id: &str) -> (String, Option<DateTime<Utc>>) {
    let row = admin()
        .await
        .query_one(
            "SELECT status, sent_at FROM alc_api.trouble_schedules WHERE id = $1",
            &[&Uuid::parse_str(id).unwrap()],
        )
        .await
        .unwrap();
    (row.get(0), row.get(1))
}

async fn fire(app: &App, id: &str) -> u16 {
    call(
        app,
        "POST",
        &format!("/api/internal/trouble/schedules/{id}/fire"),
        None,
        None,
    )
    .await
    .status
}

/// superuser で予約を 1 行入れる (別のロールで口を通すため)
async fn insert_schedule(t: Uuid, ticket: &str, task_name: Option<&str>) -> String {
    let to = vec!["u-1".to_owned()];
    let id: Uuid = admin()
        .await
        .query_one(
            "INSERT INTO alc_api.trouble_schedules \
             (tenant_id, ticket_id, scheduled_at, message, lineworks_user_ids, cloud_task_name) \
             VALUES ($1, $2, now() + interval '1 hour', '本文', $3, $4) RETURNING id",
            &[&t, &Uuid::parse_str(ticket).unwrap(), &to, &task_name],
        )
        .await
        .unwrap()
        .get(0);
    id.to_string()
}

#[tokio::test]
async fn tenant_header_is_required_except_fire() {
    let app = app().await;
    let id = Uuid::new_v4();
    for (method, uri) in [
        ("POST", "/api/trouble/schedules".to_owned()),
        ("GET", format!("/api/trouble/tickets/{id}/schedules")),
        ("DELETE", format!("/api/trouble/schedules/{id}")),
        ("GET", "/api/trouble/lineworks/members".to_owned()),
    ] {
        assert_eq!(call(&app, method, &uri, None, None).await.status, 401);
    }
    // 発火の口は X-Tenant-ID を要らない (無い id は 404)
    assert_eq!(fire(&app, &id.to_string()).await, 404);
}

#[tokio::test]
async fn create_validates_and_registers_alarm() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;
    let tk = ticket(&app, t, json!({"category": "貨物事故"})).await;
    let other_tk = ticket(&app, other, json!({"category": "貨物事故"})).await;

    // 400 の 4 種 (rust と同じ)
    let bad = |at: String, message: &str, to: Value| json!({"ticket_id": tk, "scheduled_at": at, "message": message, "lineworks_user_ids": to});
    for body in [
        bad(later(-1), "m", json!(["u"])),
        bad(later(31 * 24), "m", json!(["u"])),
        bad(later(1), "  ", json!(["u"])),
        bad(later(1), "m", json!([])),
    ] {
        let got = call(&app, "POST", "/api/trouble/schedules", Some(t), Some(body)).await;
        assert_eq!(got.status, 400);
        assert!(got.body.is_empty());
    }
    // 別の tenant のチケット・無いチケットは 404 (行を作らない)
    for ticket_id in [other_tk.clone(), Uuid::new_v4().to_string()] {
        let got = call(
            &app,
            "POST",
            "/api/trouble/schedules",
            Some(t),
            Some(json!({"ticket_id": ticket_id, "scheduled_at": later(1), "message": "m", "lineworks_user_ids": ["u"]})),
        )
        .await;
        assert_eq!(got.status, 404);
    }
    assert!(list(&app, other, &other_tk).await.is_empty());
    assert!(app.alarm.calls().is_empty());

    let at2 = later(48);
    let s2 = create(&app, t, &tk, &at2, &["u-1", "u-2"]).await;
    let s1 = create(&app, t, &tk, &later(2), &["u-1"]).await;
    let mut keys: Vec<&str> = s2.as_object().unwrap().keys().map(String::as_str).collect();
    keys.sort();
    assert_eq!(keys, SCHEDULE_KEYS);
    assert_eq!(s2["status"], "pending");
    assert_eq!(s2["message"], "点検の確認");
    assert_eq!(s2["lineworks_user_ids"], json!(["u-1", "u-2"]));
    assert_eq!(s2["tenant_id"], json!(t.to_string()));
    assert_eq!(s2["created_by"], Value::Null);
    // 応答は保存の前の行 (rust と同じ)
    assert_eq!(s2["cloud_task_name"], Value::Null);

    // alarm: PUT /alarms/{id}・secret のヘッダ・{"fire_at": RFC3339}
    let calls = app.alarm.calls();
    assert_eq!(calls.len(), 2);
    let put = &calls[0];
    assert_eq!(put.method, Method::Put);
    assert_eq!(put.path, format!("/alarms/{}", id_of(&s2)));
    assert_eq!(
        put.headers,
        vec![("X-Internal-Shared-Secret", ALARM_SECRET.to_owned())]
    );
    let fire_at: DateTime<Utc> = s2["scheduled_at"].as_str().unwrap().parse().unwrap();
    assert_eq!(
        put.body.as_deref(),
        Some(
            json!({"fire_at": fire_at.to_rfc3339()})
                .to_string()
                .as_str()
        )
    );

    // 一覧は予約の時刻の順で、cloud_task_name に alarms/{id} が入っている
    let rows = list(&app, t, &tk).await;
    assert_eq!(
        rows.iter().map(id_of).collect::<Vec<_>>(),
        [id_of(&s1), id_of(&s2)]
    );
    assert_eq!(
        rows[1]["cloud_task_name"],
        json!(format!("alarms/{}", id_of(&s2)))
    );
    // 別の tenant からは見えない
    assert!(list(&app, other, &tk).await.is_empty());
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn alarm_failure_keeps_schedule() {
    let app = app().await;
    let t = tenant().await;
    let tk = ticket(&app, t, json!({"category": "貨物事故"})).await;

    app.alarm.respond(401, b"unauthorized");
    let s = create(&app, t, &tk, &later(1), &["u-1"]).await;
    app.alarm.unreachable.store(true, Ordering::SeqCst);
    create(&app, t, &tk, &later(2), &["u-1"]).await;
    assert_eq!(
        app.logs(),
        [
            "trouble: schedule alarm register failed: alarm (status 401)",
            "trouble: schedule alarm register failed: alarm (fetch)",
        ]
    );
    let rows = list(&app, t, &tk).await;
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| r["cloud_task_name"].is_null()));
    assert_logs_clean(&app, &[t.to_string(), id_of(&s), tk]);

    // binding か secret が無い worker は登録しない
    let app = build_bindings(
        rt_client().await,
        Bindings {
            alarm: false,
            ..ALL
        },
    );
    let tk = ticket(&app, t, json!({"category": "貨物事故"})).await;
    let s = create(&app, t, &tk, &later(1), &["u-1"]).await;
    assert!(app.alarm.calls().is_empty());
    assert!(list(&app, t, &tk).await[0]["cloud_task_name"].is_null());
    // 取消も alarm を呼ばない
    let got = call(
        &app,
        "DELETE",
        &format!("/api/trouble/schedules/{}", id_of(&s)),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 204);
    assert!(app.alarm.calls().is_empty());
}

#[tokio::test]
async fn cancel_schedule() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;
    let tk = ticket(&app, t, json!({"category": "貨物事故"})).await;
    let cancel = |id: String, tenant: Uuid| {
        let app = &app;
        async move {
            call(
                app,
                "DELETE",
                &format!("/api/trouble/schedules/{id}"),
                Some(tenant),
                None,
            )
            .await
        }
    };

    assert_eq!(cancel(Uuid::new_v4().to_string(), t).await.status, 404);
    let s = create(&app, t, &tk, &later(1), &["u-1"]).await;
    let id = id_of(&s);
    // 別の tenant からは 404 (取り消さない)
    assert_eq!(cancel(id.clone(), other).await.status, 404);
    assert_eq!(status_of(&id).await.0, "pending");

    let got = cancel(id.clone(), t).await;
    assert_eq!(got.status, 204);
    assert!(got.body.is_empty());
    let del = app.alarm.calls().pop().unwrap();
    assert_eq!(del.method, Method::Delete);
    assert_eq!(del.path, format!("/alarms/{id}"));
    assert_eq!(
        del.headers,
        vec![("X-Internal-Shared-Secret", ALARM_SECRET.to_owned())]
    );
    assert_eq!(del.body, None);
    assert_eq!(status_of(&id).await.0, "cancelled");
    // pending でないものは 409
    assert_eq!(cancel(id.clone(), t).await.status, 409);

    // DO の 404 (既に消えている) は成功扱い
    let s = create(&app, t, &tk, &later(1), &["u-1"]).await;
    app.alarm.respond(404, b"");
    assert_eq!(cancel(id_of(&s), t).await.status, 204);
    assert!(app.logs().is_empty());
    // ほかの失敗はログだけで、取消は進む
    app.alarm.respond(200, b"");
    let s = create(&app, t, &tk, &later(1), &["u-1"]).await;
    let s2 = create(&app, t, &tk, &later(1), &["u-1"]).await;
    app.alarm.respond(500, b"boom");
    assert_eq!(cancel(id_of(&s), t).await.status, 204);
    app.alarm.unreachable.store(true, Ordering::SeqCst);
    assert_eq!(cancel(id_of(&s2), t).await.status, 204);
    assert_eq!(
        app.logs(),
        [
            "trouble: schedule alarm cancel failed: alarm (status 500)",
            "trouble: schedule alarm cancel failed: alarm (fetch)",
        ]
    );
    assert_eq!(status_of(&id_of(&s)).await.0, "cancelled");
    assert_eq!(status_of(&id_of(&s2)).await.0, "cancelled");

    // cloud_task_name が無い行は alarm を呼ばない
    app.alarm.unreachable.store(false, Ordering::SeqCst);
    let before = app.alarm.calls().len();
    let id = insert_schedule(t, &tk, None).await;
    assert_eq!(cancel(id, t).await.status, 204);
    assert_eq!(app.alarm.calls().len(), before);
}

#[tokio::test]
async fn fire_sends_with_heading_and_marks_sent() {
    let app = app().await;
    let t = tenant().await;
    let tk = ticket(
        &app,
        t,
        json!({"category": "貨物事故", "person_name": "山田 太郎", "company_name": "運送会社",
               "office_name": "本社", "location": "本社整備工場前",
               "occurred_at": Utc.with_ymd_and_hms(2026, 5, 18, 22, 0, 0).unwrap().to_rfc3339()}),
    )
    .await;
    let s = create(&app, t, &tk, &later(1), &["u-1", "u-2"]).await;
    let id = id_of(&s);

    // alc_api_rt (worker の接続ロール) で get_trouble_schedule を呼べる
    assert_eq!(fire(&app, &id).await, 200);
    let text = "山田 太郎  2026-05-19 07:00\n運送会社/本社 | 本社整備工場前\n\n点検の確認";
    assert_eq!(
        sent(&app, t),
        [
            ("u-1".to_owned(), text.to_owned()),
            ("u-2".to_owned(), text.to_owned())
        ]
    );
    let (status, sent_at) = status_of(&id).await;
    assert_eq!(status, "sent");
    assert!(sent_at.is_some());

    // 送信済み・取消済み・無い id は 404 で、送らない (関数が pending だけを返す。rust と同じ)
    assert_eq!(fire(&app, &id).await, 404);
    let c = create(&app, t, &tk, &later(1), &["u-1"]).await;
    let got = call(
        &app,
        "DELETE",
        &format!("/api/trouble/schedules/{}", id_of(&c)),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 204);
    assert_eq!(fire(&app, &id_of(&c)).await, 404);
    assert_eq!(fire(&app, &Uuid::new_v4().to_string()).await, 404);
    assert_eq!(app.lineworks.calls().len(), 2);

    // チケットが消えていたら見出しなしで本文だけ
    let s = create(&app, t, &tk, &later(1), &["u-3"]).await;
    let got = call(
        &app,
        "DELETE",
        &format!("/api/trouble/tickets/{tk}"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 204);
    assert_eq!(fire(&app, &id_of(&s)).await, 200);
    assert_eq!(
        sent(&app, t).pop().unwrap(),
        ("u-3".to_owned(), "点検の確認".to_owned())
    );
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn fire_send_failure_still_marks_sent() {
    let app = app().await;
    let t = tenant().await;
    let tk = ticket(&app, t, json!({"category": "貨物事故"})).await;

    app.lineworks
        .respond(404, br#"{"error":"bot_config_not_found"}"#);
    let a = create(&app, t, &tk, &later(1), &["u-secret-1"]).await;
    assert_eq!(fire(&app, &id_of(&a)).await, 200);
    app.lineworks.unreachable.store(true, Ordering::SeqCst);
    let b = create(&app, t, &tk, &later(1), &["u-secret-2"]).await;
    assert_eq!(fire(&app, &id_of(&b)).await, 200);
    assert_eq!(
        app.logs(),
        [
            "trouble: trouble_schedule notify failed: lineworks (status 404)",
            "trouble: trouble_schedule notify failed: lineworks (fetch)",
        ]
    );
    assert_eq!(status_of(&id_of(&a)).await.0, "sent");
    assert_eq!(status_of(&id_of(&b)).await.0, "sent");
    assert_logs_clean(
        &app,
        &[t.to_string(), "u-secret".to_owned(), "点検".to_owned()],
    );

    // lineworks の binding が無い worker は送らずに送信済みにする (rust の notifier 無しと同じ)
    let app = build_bindings(
        rt_client().await,
        Bindings {
            lineworks: false,
            ..ALL
        },
    );
    let c = create(&app, t, &tk, &later(1), &["u-1"]).await;
    assert_eq!(fire(&app, &id_of(&c)).await, 200);
    assert!(app.lineworks.calls().is_empty());
    assert_eq!(status_of(&id_of(&c)).await.0, "sent");
}

/// 関数だけを呼べて、表を読めない・書けないロール (チケットの見出しと送信済みの印が失敗する経路)
const FIRE_ONLY_ROLE: &str = "trouble_test_fire_only";

async fn fire_only_client() -> alc_worker_db::PgClient {
    prepare().await;
    admin()
        .await
        .batch_execute(&format!(
            "DO $$ BEGIN
               CREATE ROLE {FIRE_ONLY_ROLE} LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD 'trouble-test-throwaway';
             EXCEPTION WHEN duplicate_object THEN NULL; END $$;
             GRANT USAGE ON SCHEMA alc_api TO {FIRE_ONLY_ROLE};
             GRANT EXECUTE ON FUNCTION alc_api.get_trouble_schedule(uuid) TO {FIRE_ONLY_ROLE};"
        ))
        .await
        .unwrap();
    client_as(FIRE_ONLY_ROLE).await
}

#[tokio::test]
async fn db_failures() {
    let t = tenant().await;
    let setup = app().await;
    let tk = ticket(&setup, t, json!({"category": "貨物事故"})).await;

    // 接続が切れている → 500 (本文なし)・固定の語
    let app = build(broken_client().await);
    let body = json!({"ticket_id": tk, "scheduled_at": later(1), "message": "m", "lineworks_user_ids": ["u"]});
    let id = Uuid::new_v4();
    for (method, uri, body) in [
        ("POST", "/api/trouble/schedules".to_owned(), Some(body)),
        ("GET", format!("/api/trouble/tickets/{tk}/schedules"), None),
        ("DELETE", format!("/api/trouble/schedules/{id}"), None),
    ] {
        let got = call(&app, method, &uri, Some(t), body).await;
        assert_eq!(got.status, 500);
        assert!(got.body.is_empty());
    }
    assert_eq!(fire(&app, &id.to_string()).await, 500);
    let logs = app.logs();
    assert_eq!(logs.len(), 4);
    for (line, stage) in logs.iter().zip([
        "schedule create",
        "schedule list",
        "schedule get",
        "schedule fire get",
    ]) {
        assert!(
            line.starts_with(&format!("trouble: {stage} failed: db (")),
            "{line}"
        );
    }

    // 読めるが書けない → 取消の UPDATE が 500 (alarm の取消は済んでいる)
    let sid = insert_schedule(t, &tk, Some("alarms/x")).await;
    let app = build(client_as(READ_ONLY_ROLE).await);
    let got = call(
        &app,
        "DELETE",
        &format!("/api/trouble/schedules/{sid}"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 500);
    assert_eq!(app.alarm.calls()[0].path, "/alarms/x");
    assert_eq!(app.logs(), ["trouble: schedule cancel failed: db (42501)"]);
    assert_eq!(status_of(&sid).await.0, "pending");

    // 関数だけ呼べる → 見出しは省き (ログ)、本文だけ送り、送信済みの印の失敗もログだけで 200
    let app = build(fire_only_client().await);
    assert_eq!(fire(&app, &sid).await, 200);
    assert_eq!(sent(&app, t), [("u-1".to_owned(), "本文".to_owned())]);
    assert_eq!(
        app.logs(),
        [
            "trouble: schedule fire ticket failed: db (42501)",
            "trouble: schedule mark sent failed: db (42501)",
        ]
    );
    assert_eq!(status_of(&sid).await.0, "pending");
    assert_logs_clean(&app, &[t.to_string(), sid, tk]);
}
