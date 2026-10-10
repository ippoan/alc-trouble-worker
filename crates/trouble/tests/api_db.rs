//! 全部の口を、実 DB (`alc_api_rt` で接続) で通す。期待値は rust-alc-api の tests/trouble_test.rs・
//! tests/trouble_task_statuses_test.rs (応答の形) に合わせる。
//! 接続先は `TROUBLE_TEST_ADMIN_DATABASE_URL` (無ければ失敗する。tests/support)。

mod support;

use serde_json::{json, Value};
use support::*;
use uuid::Uuid;

/// 4 本のマスタの (path, 既定で作る行の数)
const MASTERS: &[(&str, usize)] = &[
    ("/api/trouble/categories", 7),
    ("/api/trouble/offices", 0),
    ("/api/trouble/progress-statuses", 0),
    ("/api/trouble/task-types", 8),
];

fn keys(v: &Value) -> Vec<String> {
    let mut k: Vec<String> = v.as_object().unwrap().keys().cloned().collect();
    k.sort();
    k
}

fn names(v: &Value) -> Vec<String> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|r| r["name"].as_str().unwrap().to_owned())
        .collect()
}

/// ログに識別子が出ていないこと (固定の語と kind だけ)
fn assert_logs_clean(app: &App, ids: &[Uuid]) {
    for line in app.logs() {
        assert!(line.starts_with("trouble: "), "固定の頭で始まらないログ");
        for id in ids {
            assert!(!line.contains(&id.to_string()), "ログに識別子が出た");
        }
    }
}

/// tenant のヘッダーが無い・UUID でない → 401 (どの口も)。知らない path は 404
#[tokio::test]
async fn tenant_header_is_required() {
    let app = app().await;
    let id = Uuid::new_v4();
    let paths = [
        ("GET", "/api/trouble/categories".to_owned()),
        ("DELETE", format!("/api/trouble/offices/{id}")),
        ("GET", "/api/trouble/progress-statuses".to_owned()),
        ("PUT", format!("/api/trouble/task-types/{id}")),
        ("GET", "/api/trouble/task-statuses".to_owned()),
        ("GET", "/api/trouble/field-layout".to_owned()),
        ("GET", "/api/trouble/notification-prefs".to_owned()),
        ("DELETE", format!("/api/trouble/notification-prefs/{id}")),
    ];
    for (method, path) in &paths {
        let got = call(&app, method, path, None, None).await;
        assert_eq!(got.status, 401, "{method} {path}");
        assert!(got.body.is_empty());
    }
    let req = axum::http::Request::builder()
        .uri("/api/trouble/categories")
        .header("X-Tenant-ID", "not-a-uuid")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = tower::ServiceExt::oneshot(app.router.clone(), req)
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 401);
    // rust と同じく /api の下だけ (頭の無い path は無い)
    let t = tenant().await;
    let got = call(&app, "GET", "/trouble/categories", Some(t), None).await;
    assert_eq!(got.status, 404);
    assert!(app.logs().is_empty());
}

/// 4 本のマスタ: 一覧 (auto-seed) → 作成 → 並び替え → 削除。409・400・404 と、別 tenant から見えないこと
#[tokio::test]
async fn masters_crud() {
    let app = app().await;
    for (path, seed) in MASTERS {
        let t = tenant().await;
        let other = tenant().await;

        // 1 行も無い → categories / task-types は既定の行を並び順 1.. で作る。offices / progress-statuses は空
        let got = call(&app, "GET", path, Some(t), None).await;
        assert_eq!(got.status, 200, "{path}");
        let list = got.json();
        assert_eq!(list.as_array().unwrap().len(), *seed, "{path}");
        for (i, row) in list.as_array().unwrap().iter().enumerate() {
            assert_eq!(row["sort_order"], json!(i + 1));
            assert_eq!(row["tenant_id"], json!(t));
        }
        // 2 回目は作らない (同じもの)
        let again = call(&app, "GET", path, Some(t), None).await;
        assert_eq!(again.json(), list, "{path}");

        // 作成: 201・行の形は rust の TroubleCategory と同じキー。sort_order が無ければ 0
        let got = call(
            &app,
            "POST",
            path,
            Some(t),
            Some(json!({"name": "あ", "sort_order": 50})),
        )
        .await;
        assert_eq!(got.status, 201, "{path}");
        let a = got.json();
        assert_eq!(
            keys(&a),
            ["created_at", "id", "name", "sort_order", "tenant_id"]
        );
        assert_eq!(
            (a["name"].clone(), a["sort_order"].clone()),
            (json!("あ"), json!(50))
        );
        assert_eq!(a["tenant_id"], json!(t));
        let a_id: Uuid = a["id"].as_str().unwrap().parse().unwrap();
        let got = call(&app, "POST", path, Some(t), Some(json!({"name": "い"}))).await;
        assert_eq!(got.status, 201);
        let b = got.json();
        assert_eq!(b["sort_order"], json!(0));
        let b_id: Uuid = b["id"].as_str().unwrap().parse().unwrap();

        // 空白だけの名前は 400、同じ名前は 409 (一意制約)
        let got = call(&app, "POST", path, Some(t), Some(json!({"name": "  "}))).await;
        assert_eq!((got.status, got.body.is_empty()), (400, true));
        let got = call(&app, "POST", path, Some(t), Some(json!({"name": "あ"}))).await;
        assert_eq!((got.status, got.body.is_empty()), (409, true));
        // 別 tenant では同じ名前を作れる
        let got = call(&app, "POST", path, Some(other), Some(json!({"name": "あ"}))).await;
        assert_eq!(got.status, 201);

        // 並びは sort_order, name
        let got = call(&app, "GET", path, Some(t), None).await;
        let mut want: Vec<String> = vec!["い".into()];
        want.extend(names(&list));
        want.push("あ".into());
        assert_eq!(names(&got.json()), want, "{path}");

        // 並び替え: 返るのは変えた行。別 tenant の行・無い行は 404
        let got = call(
            &app,
            "PUT",
            &format!("{path}/{a_id}"),
            Some(t),
            Some(json!({"sort_order": -1})),
        )
        .await;
        assert_eq!(got.status, 200);
        let moved = got.json();
        assert_eq!(moved["sort_order"], json!(-1));
        assert_eq!(
            (moved["id"].clone(), moved["created_at"].clone()),
            (a["id"].clone(), a["created_at"].clone())
        );
        assert_eq!(
            names(&call(&app, "GET", path, Some(t), None).await.json())[0],
            "あ"
        );
        let got = call(
            &app,
            "PUT",
            &format!("{path}/{a_id}"),
            Some(other),
            Some(json!({"sort_order": 1})),
        )
        .await;
        assert_eq!(got.status, 404);
        let got = call(
            &app,
            "PUT",
            &format!("{path}/{}", Uuid::new_v4()),
            Some(t),
            Some(json!({"sort_order": 1})),
        )
        .await;
        assert_eq!(got.status, 404);

        // 別 tenant の一覧には出ない (RLS)。別 tenant からは消せない
        let got = call(&app, "GET", path, Some(other), None).await;
        assert!(!got
            .json()
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["id"] == a["id"]));
        let got = call(&app, "DELETE", &format!("{path}/{a_id}"), Some(other), None).await;
        assert_eq!(got.status, 404);

        // 削除: 204 → もう一度は 404
        let got = call(&app, "DELETE", &format!("{path}/{a_id}"), Some(t), None).await;
        assert_eq!((got.status, got.body.is_empty()), (204, true));
        let got = call(&app, "DELETE", &format!("{path}/{a_id}"), Some(t), None).await;
        assert_eq!(got.status, 404);
        let got = call(&app, "DELETE", &format!("{path}/{b_id}"), Some(t), None).await;
        assert_eq!(got.status, 204);
        assert_eq!(call(&app, "GET", path, Some(t), None).await.json(), list);
    }
    assert!(app.logs().is_empty());
}

/// RLS は WHERE tenant_id が無くても効く: 別 tenant の行は DB から見えない (superuser で入れた行で確かめる)
#[tokio::test]
async fn rls_hides_other_tenant_rows() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;
    let su = admin().await;
    su.execute(
        "INSERT INTO alc_api.trouble_offices (tenant_id, name) VALUES ($1, '他社の営業所')",
        &[&other],
    )
    .await
    .unwrap();
    let got = call(&app, "GET", "/api/trouble/offices", Some(t), None).await;
    assert_eq!(got.json(), json!([]));
    let got = call(&app, "GET", "/api/trouble/offices", Some(other), None).await;
    assert_eq!(names(&got.json()), ["他社の営業所"]);
}

/// タスクの状態: 既定の 4 行 → 作成 (key の補い) → 部分の更新 → 削除。409・400・404 と別 tenant
#[tokio::test]
async fn task_statuses_crud() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;
    let path = "/api/trouble/task-statuses";

    let got = call(&app, "GET", path, Some(t), None).await;
    assert_eq!(got.status, 200);
    let list = got.json();
    let rows = list.as_array().unwrap();
    assert_eq!(rows.len(), 4);
    let ks: Vec<&str> = rows.iter().map(|s| s["key"].as_str().unwrap()).collect();
    assert_eq!(ks, ["open", "in_progress", "waiting", "done"]);
    for s in rows {
        assert_eq!(s["is_done"].as_bool().unwrap(), s["key"] == "done");
        assert_eq!(s["tenant_id"], json!(t));
    }
    assert_eq!(
        keys(&rows[0]),
        [
            "color",
            "created_at",
            "id",
            "is_done",
            "key",
            "name",
            "sort_order",
            "tenant_id",
            "updated_at"
        ]
    );
    assert_eq!(
        (
            rows[0]["name"].clone(),
            rows[0]["color"].clone(),
            rows[0]["sort_order"].clone()
        ),
        (json!("未着手"), json!("#9CA3AF"), json!(10))
    );
    assert_eq!(call(&app, "GET", path, Some(t), None).await.json(), list);

    // 作成 (rust の test_task_statuses_crud と同じ body)
    let got = call(
        &app,
        "POST",
        path,
        Some(t),
        Some(json!({"key": "on_hold", "name": "保留", "color": "#ABCDEF", "sort_order": 25, "is_done": false})),
    )
    .await;
    assert_eq!(got.status, 201);
    let created = got.json();
    assert_eq!(
        (
            created["key"].clone(),
            created["name"].clone(),
            created["color"].clone(),
            created["sort_order"].clone(),
            created["is_done"].clone()
        ),
        (
            json!("on_hold"),
            json!("保留"),
            json!("#ABCDEF"),
            json!(25),
            json!(false)
        )
    );
    let id = created["id"].as_str().unwrap().to_owned();

    // key が無い・空白だけ → 名前 (trim) が key。色・並び順・完了扱いは既定
    let got = call(
        &app,
        "POST",
        path,
        Some(t),
        Some(json!({"key": " ", "name": " 確認中 "})),
    )
    .await;
    assert_eq!(got.status, 201);
    let derived = got.json();
    assert_eq!(
        (
            derived["key"].clone(),
            derived["name"].clone(),
            derived["color"].clone(),
            derived["sort_order"].clone(),
            derived["is_done"].clone()
        ),
        (
            json!("確認中"),
            json!(" 確認中 "),
            json!("#9CA3AF"),
            json!(0),
            json!(false)
        )
    );

    // 400 / 409 (key の重複・名前の重複)
    let got = call(&app, "POST", path, Some(t), Some(json!({"name": "   "}))).await;
    assert_eq!(got.status, 400);
    let got = call(
        &app,
        "POST",
        path,
        Some(t),
        Some(json!({"key": "on_hold", "name": "別名"})),
    )
    .await;
    assert_eq!(got.status, 409);
    let got = call(
        &app,
        "POST",
        path,
        Some(t),
        Some(json!({"key": "other", "name": "保留"})),
    )
    .await;
    assert_eq!(got.status, 409);

    // 更新: 来た項目だけ変わる。key は変わらない
    let got = call(
        &app,
        "PUT",
        &format!("{path}/{id}"),
        Some(t),
        Some(json!({"sort_order": 99, "name": "保留中", "color": "#112233", "is_done": true})),
    )
    .await;
    assert_eq!(got.status, 200);
    let updated = got.json();
    assert_eq!(
        (
            updated["key"].clone(),
            updated["name"].clone(),
            updated["color"].clone(),
            updated["sort_order"].clone(),
            updated["is_done"].clone()
        ),
        (
            json!("on_hold"),
            json!("保留中"),
            json!("#112233"),
            json!(99),
            json!(true)
        )
    );
    assert_eq!(updated["created_at"], created["created_at"]);
    let got = call(
        &app,
        "PUT",
        &format!("{path}/{id}"),
        Some(t),
        Some(json!({"color": "#000000"})),
    )
    .await;
    let partial = got.json();
    assert_eq!(
        (
            partial["name"].clone(),
            partial["color"].clone(),
            partial["sort_order"].clone(),
            partial["is_done"].clone()
        ),
        (json!("保留中"), json!("#000000"), json!(99), json!(true))
    );
    // 名前の重複は 409、別 tenant・無い行は 404
    let got = call(
        &app,
        "PUT",
        &format!("{path}/{id}"),
        Some(t),
        Some(json!({"name": "完了"})),
    )
    .await;
    assert_eq!(got.status, 409);
    let got = call(
        &app,
        "PUT",
        &format!("{path}/{id}"),
        Some(other),
        Some(json!({"name": "x"})),
    )
    .await;
    assert_eq!(got.status, 404);
    let got = call(
        &app,
        "PUT",
        &format!("{path}/{}", Uuid::new_v4()),
        Some(t),
        Some(json!({})),
    )
    .await;
    assert_eq!(got.status, 404);

    // 別 tenant には既定の 4 行だけ (この tenant の行は見えない)
    let got = call(&app, "GET", path, Some(other), None).await;
    assert_eq!(got.json().as_array().unwrap().len(), 4);
    let got = call(&app, "DELETE", &format!("{path}/{id}"), Some(other), None).await;
    assert_eq!(got.status, 404);

    let got = call(&app, "DELETE", &format!("{path}/{id}"), Some(t), None).await;
    assert_eq!(got.status, 204);
    let got = call(&app, "DELETE", &format!("{path}/{id}"), Some(t), None).await;
    assert_eq!(got.status, 404);
    assert!(app.logs().is_empty());
}

/// 入力欄の表示設定: 無ければ空 → 保存 → 読む → 置き換える。別 tenant には出ない
#[tokio::test]
async fn field_layout_get_put() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;
    let path = "/api/trouble/field-layout";

    let got = call(&app, "GET", path, Some(t), None).await;
    assert_eq!((got.status, got.json()), (200, json!({"settings": []})));

    let body = json!({"settings": [
        {"key": "title", "visible": true, "width": "full", "sort_order": 1, "label": "件名"},
        {"key": "location", "visible": false, "width": "half", "sort_order": 2}
    ]});
    let want = json!({"settings": [
        {"key": "title", "visible": true, "width": "full", "sort_order": 1, "label": "件名"},
        {"key": "location", "visible": false, "width": "half", "sort_order": 2, "label": null}
    ]});
    let got = call(&app, "PUT", path, Some(t), Some(body)).await;
    assert_eq!((got.status, got.json()), (200, want.clone()));
    let got = call(&app, "GET", path, Some(t), None).await;
    assert_eq!(got.json(), want);

    let replaced = json!({"settings": [{"key": "title", "visible": false, "width": "half", "sort_order": 3, "label": null}]});
    let got = call(&app, "PUT", path, Some(t), Some(replaced.clone())).await;
    assert_eq!(got.json(), replaced);
    assert_eq!(
        call(&app, "GET", path, Some(t), None).await.json(),
        replaced
    );
    let n: i64 = admin()
        .await
        .query_one(
            "SELECT count(*) FROM alc_api.trouble_field_layouts WHERE tenant_id = $1",
            &[&t],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 1, "tenant ごとに 1 行");

    let got = call(&app, "GET", path, Some(other), None).await;
    assert_eq!(got.json(), json!({"settings": []}));
    // body の形が違えば axum の 422 (rust と同じ)
    let got = call(
        &app,
        "PUT",
        path,
        Some(t),
        Some(json!({"settings": [{"key": "x"}]})),
    )
    .await;
    assert_eq!(got.status, 422);
    assert!(app.logs().is_empty());
}

/// 保存されている設定が配列として読めない → 500 (ログは固定の語と kind だけ)
#[tokio::test]
async fn field_layout_unreadable_settings() {
    let app = app().await;
    let t = tenant().await;
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.trouble_field_layouts (tenant_id, settings) VALUES ($1, '{\"x\": 1}')",
            &[&t],
        )
        .await
        .unwrap();
    let got = call(&app, "GET", "/api/trouble/field-layout", Some(t), None).await;
    assert_eq!((got.status, got.body.is_empty()), (500, true));
    assert_eq!(
        app.logs(),
        ["trouble: field layout get failed: db (from_sql)"]
    );
}

/// 通知の設定: 400 → 作成 (既定) → 来た項目だけの更新 → 一覧の並び → 削除。別 tenant には出ない
#[tokio::test]
async fn notification_prefs_crud() {
    let app = app().await;
    let t = tenant().await;
    let other = tenant().await;
    let path = "/api/trouble/notification-prefs";

    let got = call(
        &app,
        "POST",
        path,
        Some(t),
        Some(json!({"event_type": "nope", "notify_channel": "lineworks"})),
    )
    .await;
    assert_eq!((got.status, got.body.is_empty()), (400, true));
    let got = call(
        &app,
        "POST",
        path,
        Some(t),
        Some(json!({"event_type": "trouble_created", "notify_channel": "webhook"})),
    )
    .await;
    assert_eq!(got.status, 400);

    let got = call(
        &app,
        "POST",
        path,
        Some(t),
        Some(json!({"event_type": "trouble_created", "notify_channel": "lineworks"})),
    )
    .await;
    assert_eq!(got.status, 201);
    let created = got.json();
    assert_eq!(
        keys(&created),
        [
            "created_at",
            "enabled",
            "event_type",
            "id",
            "lineworks_user_ids",
            "notify_admins",
            "notify_channel",
            "recipient_ids",
            "tenant_id",
            "updated_at"
        ]
    );
    assert_eq!(
        (
            created["enabled"].clone(),
            created["recipient_ids"].clone(),
            created["notify_admins"].clone(),
            created["lineworks_user_ids"].clone()
        ),
        (json!(true), json!([]), json!(false), json!([]))
    );

    // 同じ (event_type, channel) は同じ行を更新。来ない項目はそのまま
    let r = Uuid::new_v4();
    let got = call(
        &app,
        "POST",
        path,
        Some(t),
        Some(json!({"event_type": "trouble_created", "notify_channel": "lineworks", "recipient_ids": [r], "lineworks_user_ids": ["lw-1", "lw-2"]})),
    )
    .await;
    assert_eq!(got.status, 201);
    let updated = got.json();
    assert_eq!(updated["id"], created["id"]);
    assert_eq!(
        (
            updated["enabled"].clone(),
            updated["recipient_ids"].clone(),
            updated["notify_admins"].clone(),
            updated["lineworks_user_ids"].clone()
        ),
        (
            json!(true),
            json!([r]),
            json!(false),
            json!(["lw-1", "lw-2"])
        )
    );
    let got = call(
        &app,
        "POST",
        path,
        Some(t),
        Some(json!({"event_type": "trouble_created", "notify_channel": "lineworks", "enabled": false, "notify_admins": true})),
    )
    .await;
    let toggled = got.json();
    assert_eq!(
        (
            toggled["enabled"].clone(),
            toggled["recipient_ids"].clone(),
            toggled["notify_admins"].clone(),
            toggled["lineworks_user_ids"].clone()
        ),
        (
            json!(false),
            json!([r]),
            json!(true),
            json!(["lw-1", "lw-2"])
        )
    );
    let got = call(
        &app,
        "POST",
        path,
        Some(t),
        Some(json!({"event_type": "task_assigned", "notify_channel": "lineworks"})),
    )
    .await;
    let second = got.json();

    // 並びは event_type, notify_channel
    let got = call(&app, "GET", path, Some(t), None).await;
    assert_eq!(got.status, 200);
    let ev: Vec<Value> = got
        .json()
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["event_type"].clone())
        .collect();
    assert_eq!(ev, [json!("task_assigned"), json!("trouble_created")]);
    let got = call(&app, "GET", path, Some(other), None).await;
    assert_eq!(got.json(), json!([]));

    let id = created["id"].as_str().unwrap();
    let got = call(&app, "DELETE", &format!("{path}/{id}"), Some(other), None).await;
    assert_eq!(got.status, 404);
    let got = call(&app, "DELETE", &format!("{path}/{id}"), Some(t), None).await;
    assert_eq!(got.status, 204);
    let got = call(&app, "DELETE", &format!("{path}/{id}"), Some(t), None).await;
    assert_eq!(got.status, 404);
    let got = call(&app, "GET", path, Some(t), None).await;
    assert_eq!(got.json(), json!([second]));
    assert!(app.logs().is_empty());
}

/// 接続が切れた DB: どの口も 500 (本文は空)。ログは段の名前と kind だけで、識別子を含まない
#[tokio::test]
async fn db_failure_is_500_with_fixed_logs() {
    let t = tenant().await;
    let id = Uuid::new_v4();
    let cases: Vec<(&str, String, Option<Value>, &str)> = vec![
        ("GET", "/api/trouble/categories".into(), None, "master list"),
        (
            "POST",
            "/api/trouble/offices".into(),
            Some(json!({"name": "x"})),
            "master create",
        ),
        (
            "PUT",
            format!("/api/trouble/progress-statuses/{id}"),
            Some(json!({"sort_order": 1})),
            "master update",
        ),
        (
            "DELETE",
            format!("/api/trouble/task-types/{id}"),
            None,
            "master delete",
        ),
        (
            "GET",
            "/api/trouble/task-statuses".into(),
            None,
            "task status list",
        ),
        (
            "POST",
            "/api/trouble/task-statuses".into(),
            Some(json!({"name": "x"})),
            "task status create",
        ),
        (
            "PUT",
            format!("/api/trouble/task-statuses/{id}"),
            Some(json!({})),
            "task status update",
        ),
        (
            "DELETE",
            format!("/api/trouble/task-statuses/{id}"),
            None,
            "task status delete",
        ),
        (
            "GET",
            "/api/trouble/field-layout".into(),
            None,
            "field layout get",
        ),
        (
            "PUT",
            "/api/trouble/field-layout".into(),
            Some(json!({"settings": []})),
            "field layout put",
        ),
        (
            "POST",
            "/api/trouble/notification-prefs".into(),
            Some(json!({"event_type": "task_assigned", "notify_channel": "lineworks"})),
            "notification pref upsert",
        ),
        (
            "GET",
            "/api/trouble/notification-prefs".into(),
            None,
            "notification pref list",
        ),
        (
            "DELETE",
            format!("/api/trouble/notification-prefs/{id}"),
            None,
            "notification pref delete",
        ),
    ];
    for (method, path, body, stage) in cases {
        let app = build(broken_client().await);
        let got = call(&app, method, &path, Some(t), body).await;
        assert_eq!(
            (got.status, got.body.is_empty()),
            (500, true),
            "{method} {path}"
        );
        let logs = app.logs();
        assert_eq!(logs.len(), 1, "{method} {path}");
        assert!(
            logs[0].starts_with(&format!("trouble: {stage} failed: db (")),
            "{method} {path}"
        );
        assert_logs_clean(&app, &[t, id]);
    }
}

/// 作れない auto-seed (表を読めるが書けないロール): 作れなかった行は飛ばして 200 の空。ログは行ごとに固定の語
#[tokio::test]
async fn auto_seed_failure_is_skipped() {
    let t = tenant().await;
    let app = build(client_as(READ_ONLY_ROLE).await);
    let got = call(&app, "GET", "/api/trouble/categories", Some(t), None).await;
    assert_eq!((got.status, got.json()), (200, json!([])));
    let got = call(&app, "GET", "/api/trouble/task-statuses", Some(t), None).await;
    assert_eq!((got.status, got.json()), (200, json!([])));
    let mut want = vec!["trouble: master auto-seed failed: db (42501)"; 7];
    want.extend(["trouble: task status auto-seed failed: db (42501)"; 4]);
    assert_eq!(app.logs(), want);
    assert_logs_clean(&app, &[t]);
}
