//! チケット・タスクの LINE WORKS 通知と、メンバーの口 (alc-lineworks-worker の中継) を、実 DB (`alc_api_rt` で接続) で通す。
//! alc-lineworks-worker はモック (tests/support の `MockService`)。文面と通知の条件は rust-alc-api の tickets.rs・tasks.rs と同じ。
//! 接続先は `TROUBLE_TEST_ADMIN_DATABASE_URL` (無ければ失敗する。tests/support)。

mod support;

use std::sync::atomic::Ordering;

use serde_json::{json, Value};
use support::*;
use trouble::service::Method;
use uuid::Uuid;

fn id_of(v: &Value) -> String {
    v["id"].as_str().unwrap().to_owned()
}

fn pairs(v: &[(&str, &str)]) -> Vec<(String, String)> {
    v.iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect()
}

async fn ticket(app: &App, t: Uuid, body: Value) -> Value {
    let got = call(app, "POST", "/api/trouble/tickets", Some(t), Some(body)).await;
    assert_eq!(got.status, 201);
    got.json()
}

async fn employee(t: Uuid, name: &str) -> Uuid {
    let id = Uuid::new_v4();
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.employees (id, tenant_id, nfc_id, name) VALUES ($1, $2, $3, $4)",
            &[&id, &t, &id.to_string(), &name],
        )
        .await
        .unwrap();
    id
}

#[tokio::test]
async fn ticket_notifications_follow_prefs() {
    let app = app().await;
    let t = tenant().await;
    pref(t, "trouble_created", true, &["u-a", "u-b"]).await;
    pref(t, "trouble_assigned", true, &["u-c"]).await;
    pref(t, "trouble_status_changed", false, &["u-d"]).await;

    let tk = ticket(
        &app,
        t,
        json!({"category": "貨物事故", "person_name": "山田", "description": "荷崩れ"}),
    )
    .await;
    let no = tk["ticket_no"].as_i64().unwrap();
    let id = id_of(&tk);
    // 文面は rust と同じ (値の並びも rust のまま)
    let created = format!("トラブル登録: #{no} 貨物事故\nカテゴリ: 山田\n担当者: 荷崩れ");
    assert_eq!(
        sent(&app, t),
        pairs(&[("u-a", &created), ("u-b", &created)])
    );

    // 担当者が来ない更新は送らない。来たら trouble_assigned
    let put = |body: Value| {
        let (app, id) = (&app, id.clone());
        async move {
            let got = call(
                app,
                "PUT",
                &format!("/api/trouble/tickets/{id}"),
                Some(t),
                Some(body),
            )
            .await;
            assert_eq!(got.status, 200);
        }
    };
    put(json!({"title": "x"})).await;
    assert_eq!(app.lineworks.calls().len(), 2);
    put(json!({"assigned_to": Uuid::new_v4()})).await;
    assert_eq!(
        sent(&app, t)[2..],
        pairs(&[("u-c", &format!("担当者アサイン: #{no} 貨物事故"))])
    );

    // 状態の遷移: 設定が無効なら送らない、有効にすれば送る
    let got = call(&app, "POST", "/api/trouble/workflow/setup", Some(t), None).await;
    let states: std::collections::HashMap<String, String> = got
        .json()
        .as_array()
        .unwrap()
        .iter()
        .map(|s| (s["name"].as_str().unwrap().to_owned(), id_of(s)))
        .collect();
    let tk2 = ticket(&app, t, json!({"category": "貨物事故"})).await;
    let no2 = tk2["ticket_no"].as_i64().unwrap();
    let transition = |to: &str| {
        let (app, uri, to) = (
            &app,
            format!("/api/trouble/tickets/{}/transition", id_of(&tk2)),
            states[to].clone(),
        );
        async move {
            let got = call(app, "POST", &uri, Some(t), Some(json!({"to_state_id": to}))).await;
            assert_eq!(got.status, 200);
        }
    };
    let before = app.lineworks.calls().len();
    transition("in_progress").await;
    assert_eq!(app.lineworks.calls().len(), before);
    admin()
        .await
        .execute(
            "UPDATE alc_api.trouble_notification_prefs SET enabled = TRUE WHERE tenant_id = $1",
            &[&t],
        )
        .await
        .unwrap();
    transition("closed").await;
    assert_eq!(
        sent(&app, t).pop().unwrap(),
        ("u-d".to_owned(), format!("ステータス変更: #{no2} 貨物事故"))
    );
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn no_prefs_no_send() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;
    // 別の tenant の設定は効かない
    pref(other, "trouble_created", true, &["u-x"]).await;
    pref(other, "trouble_assigned", true, &["u-x"]).await;
    pref(other, "task_assigned", true, &["u-x"]).await;

    let tk = ticket(&app, t, json!({"category": "貨物事故"})).await;
    let id = id_of(&tk);
    let got = call(
        &app,
        "PUT",
        &format!("/api/trouble/tickets/{id}"),
        Some(t),
        Some(json!({"assigned_to": Uuid::new_v4()})),
    )
    .await;
    assert_eq!(got.status, 200);
    let got = call(
        &app,
        "POST",
        &format!("/api/trouble/tickets/{id}/tasks"),
        Some(t),
        Some(json!({"task_type": "連絡", "title": "連絡", "assigned_to": Uuid::new_v4()})),
    )
    .await;
    assert_eq!(got.status, 201);
    assert!(app.lineworks.calls().is_empty());
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn send_failure_keeps_success() {
    let app = app().await;
    let t = tenant().await;
    pref(t, "trouble_created", true, &["u-secret"]).await;

    app.lineworks.respond(502, br#"{"error":"upstream_error"}"#);
    ticket(
        &app,
        t,
        json!({"category": "貨物事故", "person_name": "秘密の名前"}),
    )
    .await;
    app.lineworks.unreachable.store(true, Ordering::SeqCst);
    ticket(&app, t, json!({"category": "貨物事故"})).await;
    assert_eq!(
        app.logs(),
        [
            "trouble: trouble_created notify failed: lineworks (status 502)",
            "trouble: trouble_created notify failed: lineworks (fetch)",
        ]
    );
    for line in app.logs() {
        assert!(
            !line.contains("u-secret") && !line.contains("秘密") && !line.contains(&t.to_string())
        );
    }

    // binding が無い worker は送らない (rust の notifier 無しと同じ)
    let app = build_bindings(
        rt_client().await,
        Bindings {
            lineworks: false,
            ..ALL
        },
    );
    ticket(&app, t, json!({"category": "貨物事故"})).await;
    assert!(app.lineworks.calls().is_empty());
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn task_assigned_notifications() {
    let app = app().await;
    let t = tenant().await;
    pref(t, "task_assigned", true, &["u-a"]).await;
    let emp = employee(t, "佐藤 花子").await;
    let tk = ticket(&app, t, json!({"category": "貨物事故"})).await;
    let no = tk["ticket_no"].as_i64().unwrap();
    let tid = id_of(&tk);

    let create = |body: Value| {
        let (app, uri) = (&app, format!("/api/trouble/tickets/{tid}/tasks"));
        async move {
            let got = call(app, "POST", &uri, Some(t), Some(body)).await;
            assert_eq!(got.status, 201);
            got.json()
        }
    };
    let update = |id: String, body: Value| {
        let app = &app;
        async move {
            let got = call(
                app,
                "PUT",
                &format!("/api/trouble/tasks/{id}"),
                Some(t),
                Some(body),
            )
            .await;
            assert_eq!(got.status, 200);
        }
    };

    create(json!({"task_type": "連絡", "title": "連絡", "assigned_to": emp})).await;
    assert_eq!(
        sent(&app, t),
        pairs(&[("u-a", &format!("タスクアサイン: #{no} 連絡 → 佐藤 花子"))])
    );
    // 担当者なしの作成・担当者を外す更新・担当者に触れない更新は送らない
    let task = create(json!({"task_type": "確認", "title": "確認"})).await;
    let task_id = id_of(&task);
    update(task_id.clone(), json!({"assigned_to": null})).await;
    update(task_id.clone(), json!({"title": "確認2"})).await;
    assert_eq!(app.lineworks.calls().len(), 1);
    // 担当者に人が来た更新は送る。従業員に居なければ「不明」
    update(task_id.clone(), json!({"assigned_to": Uuid::new_v4()})).await;
    assert_eq!(
        sent(&app, t).pop().unwrap(),
        (
            "u-a".to_owned(),
            format!("タスクアサイン: #{no} 確認2 → 不明")
        )
    );
    // チケットが消えていたら番号は 0 (rust と同じ)
    let got = call(
        &app,
        "DELETE",
        &format!("/api/trouble/tickets/{tid}"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 204);
    update(task_id, json!({"assigned_to": emp})).await;
    assert_eq!(
        sent(&app, t).pop().unwrap(),
        (
            "u-a".to_owned(),
            "タスクアサイン: #0 確認2 → 佐藤 花子".to_owned()
        )
    );
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn members_relay() {
    let app = app().await;
    let t = tenant().await;
    let get = || call(&app, "GET", "/api/trouble/lineworks/members", Some(t), None);

    app.lineworks.respond(
        200,
        json!([
            {"user_id": "u-1", "user_name": "山田 太郎", "email": "a@example.com", "extra": 1},
            {"user_id": "u-2", "user_name": null, "email": null}
        ])
        .to_string()
        .as_bytes(),
    );
    let got = get().await;
    assert_eq!(got.status, 200);
    assert_eq!(
        got.json(),
        json!([
            {"user_id": "u-1", "user_name": "山田 太郎", "email": "a@example.com"},
            {"user_id": "u-2", "user_name": null, "email": null}
        ])
    );
    let req = app.lineworks.calls().pop().unwrap();
    assert_eq!(req.method, Method::Get);
    assert_eq!(req.path, "/api/internal/lineworks/users");
    assert!(req.headers.contains(&("X-Tenant-ID", t.to_string())));
    assert_eq!(req.body, None);

    // 403 missing_scope は status と語を写す (自由文は載せない)
    app.lineworks.respond(
        403,
        br#"{"error":"missing_scope","scope":"directory.read","message":"free text here"}"#,
    );
    let got = get().await;
    assert_eq!(got.status, 403);
    assert_eq!(
        got.json(),
        json!({"error": "missing_scope", "scope": "directory.read",
               "message": "LINE WORKS Developer Console で directory.read scope を追加してください"})
    );
    app.lineworks
        .respond(404, br#"{"error":"bot_config_not_found"}"#);
    let got = get().await;
    assert_eq!(got.status, 404);
    assert_eq!(got.json(), json!({"error": "bot_config_not_found"}));
    // 本文が読めない・届かない → 502
    app.lineworks.respond(200, b"not json");
    assert_eq!(get().await.status, 502);
    app.lineworks.unreachable.store(true, Ordering::SeqCst);
    let got = get().await;
    assert_eq!(got.status, 502);
    assert_eq!(got.json(), json!({"error": "upstream_error"}));
    assert_eq!(
        app.logs(),
        [
            "trouble: lineworks members failed: lineworks (status 403)",
            "trouble: lineworks members failed: lineworks (status 404)",
            "trouble: lineworks members failed: lineworks (body)",
            "trouble: lineworks members failed: lineworks (fetch)",
        ]
    );

    // binding が無い → 502
    let app = build_bindings(
        rt_client().await,
        Bindings {
            lineworks: false,
            ..ALL
        },
    );
    let got = call(&app, "GET", "/api/trouble/lineworks/members", Some(t), None).await;
    assert_eq!(got.status, 502);
    assert_eq!(
        app.logs(),
        ["trouble: lineworks members failed: lineworks (not_configured)"]
    );
}
