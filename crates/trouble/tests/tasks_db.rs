//! タスクの口を、実 DB (`alc_api_rt` で接続) で通す。期待値は rust-alc-api の
//! tests/trouble_tasks_cross_ticket_test.rs (横断の一覧・並べ替え) と tests/trouble_test.rs のタスクの部分に合わせる。
//! 接続先は `TROUBLE_TEST_ADMIN_DATABASE_URL` (無ければ失敗する。tests/support)。

mod support;

use serde_json::{json, Value};
use support::*;
use uuid::Uuid;

const TASK_KEYS: &[&str] = &[
    "assigned_to",
    "completed_at",
    "created_at",
    "created_by",
    "description",
    "due_date",
    "id",
    "next_action",
    "next_action_by",
    "next_action_detail",
    "next_action_due",
    "occurred_at",
    "print_page_break_before",
    "sort_order",
    "status",
    "task_type",
    "tenant_id",
    "ticket_id",
    "title",
    "updated_at",
];

fn id_of(v: &Value) -> String {
    v["id"].as_str().unwrap().to_owned()
}

fn titles(v: &Value) -> Vec<String> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|t| t["title"].as_str().unwrap().to_owned())
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

async fn ticket(app: &App, t: Uuid, category: &str, person: &str) -> String {
    let got = call(
        app,
        "POST",
        "/api/trouble/tickets",
        Some(t),
        Some(json!({"category": category, "person_name": person})),
    )
    .await;
    assert_eq!(got.status, 201);
    id_of(&got.json())
}

async fn create(app: &App, t: Uuid, ticket: &str, body: Value) -> Value {
    let got = call(
        app,
        "POST",
        &format!("/api/trouble/tickets/{ticket}/tasks"),
        Some(t),
        Some(body),
    )
    .await;
    assert_eq!(got.status, 201, "{}", String::from_utf8_lossy(&got.body));
    got.json()
}

async fn by_ticket(app: &App, t: Uuid, ticket: &str) -> Value {
    let got = call(
        app,
        "GET",
        &format!("/api/trouble/tickets/{ticket}/tasks"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 200);
    got.json()
}

async fn list_all(app: &App, t: Uuid, query: &str) -> Value {
    let got = call(
        app,
        "GET",
        &format!("/api/trouble/tasks{query}"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 200, "{query}");
    got.json()
}

async fn update(app: &App, t: Uuid, id: &str, body: Value) -> Value {
    let got = call(
        app,
        "PUT",
        &format!("/api/trouble/tasks/{id}"),
        Some(t),
        Some(body),
    )
    .await;
    assert_eq!(got.status, 200, "{}", String::from_utf8_lossy(&got.body));
    got.json()
}

/// tenant のヘッダーが無ければ、足した口もどれも 401
#[tokio::test]
async fn tenant_header_is_required() {
    let app = app().await;
    let id = Uuid::new_v4();
    let paths = [
        ("GET", "/api/trouble/tasks".to_owned()),
        ("GET", format!("/api/trouble/tickets/{id}/tasks")),
        ("POST", format!("/api/trouble/tickets/{id}/tasks")),
        ("PUT", format!("/api/trouble/tickets/{id}/tasks/reorder")),
        ("PUT", format!("/api/trouble/tasks/{id}")),
        ("DELETE", format!("/api/trouble/tasks/{id}")),
    ];
    for (method, path) in &paths {
        let got = call(&app, method, path, None, None).await;
        assert_eq!(
            (got.status, got.body.is_empty()),
            (401, true),
            "{method} {path}"
        );
    }
    // ファイルの口はまだ無い (後続の PR)
    for (method, path) in [
        ("GET", format!("/api/trouble/tasks/{id}/files")),
        ("GET", format!("/api/trouble/task-files/{id}/download")),
        ("DELETE", format!("/api/trouble/task-files/{id}")),
    ] {
        let t = Uuid::new_v4();
        let got = call(&app, method, &path, Some(t), None).await;
        assert_eq!(got.status, 404, "{method} {path}");
    }
    assert!(app.logs().is_empty());
}

/// 作成・ticket ごとの一覧・部分更新・削除 (rust の repo と同じ意味)
#[tokio::test]
async fn task_crud() {
    let app = app().await;
    let t = tenant().await;
    let tk = ticket(&app, t, "貨物事故", "ドライバーA").await;

    // 題名が空 (空白だけ) は 400、task_type が無いのは JSON が読めない (axum の 422)
    for body in [
        json!({"task_type": "連絡", "title": "  "}),
        json!({"task_type": "連絡", "title": ""}),
    ] {
        let got = call(
            &app,
            "POST",
            &format!("/api/trouble/tickets/{tk}/tasks"),
            Some(t),
            Some(body),
        )
        .await;
        assert_eq!((got.status, got.body.is_empty()), (400, true));
    }
    let got = call(
        &app,
        "POST",
        &format!("/api/trouble/tickets/{tk}/tasks"),
        Some(t),
        Some(json!({"title": "x"})),
    )
    .await;
    assert_eq!(got.status, 422);

    // 省いた項目は既定 (status は open・文字列は空・sort_order は末尾)
    let a = create(&app, t, &tk, json!({"task_type": "連絡", "title": "1件目"})).await;
    let mut k: Vec<String> = a.as_object().unwrap().keys().cloned().collect();
    k.sort();
    assert_eq!(k, TASK_KEYS);
    assert_eq!(a["tenant_id"], json!(t));
    assert_eq!(a["ticket_id"], json!(tk));
    assert_eq!(
        (
            a["status"].clone(),
            a["description"].clone(),
            a["next_action"].clone(),
            a["next_action_detail"].clone(),
            a["sort_order"].clone(),
            a["print_page_break_before"].clone(),
            a["created_by"].clone(),
            a["assigned_to"].clone(),
        ),
        (
            json!("open"),
            json!(""),
            json!(""),
            json!(""),
            json!(0),
            json!(false),
            Value::Null,
            Value::Null
        )
    );
    // 全部の項目を指定 (日時・UUID の `""` は None)
    let person = Uuid::new_v4();
    let b = create(
        &app,
        t,
        &tk,
        json!({
            "task_type": "修理手配", "title": "2件目", "description": "説明",
            "assigned_to": person, "due_date": "2026-05-01T09:00:00Z",
            "next_action": "電話", "next_action_detail": "折り返し", "next_action_by": "青井 健",
            "next_action_due": "2026-05-02T09:00:00Z", "occurred_at": "2026-04-30T09:00:00Z"
        }),
    )
    .await;
    assert_eq!(b["sort_order"], 1);
    assert_eq!(b["assigned_to"], json!(person));
    assert_eq!(b["due_date"], "2026-05-01T09:00:00Z");
    assert_eq!(b["next_action_by"], "青井 健");
    assert_eq!(b["next_action_due"], "2026-05-02T09:00:00Z");
    assert_eq!(b["occurred_at"], "2026-04-30T09:00:00Z");
    // sort_order を指定すればその値
    let c = create(
        &app,
        t,
        &tk,
        json!({"task_type": "報告", "title": "先頭", "sort_order": -1, "assigned_to": "", "due_date": ""}),
    )
    .await;
    assert_eq!(c["sort_order"], -1);
    assert_eq!(
        (c["assigned_to"].clone(), c["due_date"].clone()),
        (Value::Null, Value::Null)
    );

    // ticket ごとの一覧は sort_order・作成の順
    let list = by_ticket(&app, t, &tk).await;
    assert_eq!(titles(&list), vec!["先頭", "1件目", "2件目"]);
    assert_eq!(list[1], a);
    // 無いチケットは空
    assert_eq!(
        by_ticket(&app, t, &Uuid::new_v4().to_string()).await,
        json!([])
    );

    // 部分更新: 来た項目だけ変わる
    let b_id = id_of(&b);
    let u = update(&app, t, &b_id, json!({"status": "in_progress"})).await;
    assert_eq!(u["status"], "in_progress");
    for key in [
        "title",
        "description",
        "assigned_to",
        "due_date",
        "next_action_by",
        "next_action_due",
        "occurred_at",
        "sort_order",
    ] {
        assert_eq!(u[key], b[key], "{key}");
    }
    // `""` / `null` で空にできる項目
    let u = update(
        &app,
        t,
        &b_id,
        json!({"assigned_to": "", "due_date": null, "next_action_by": "", "next_action_due": "", "occurred_at": null}),
    )
    .await;
    for key in [
        "assigned_to",
        "due_date",
        "next_action_by",
        "next_action_due",
        "occurred_at",
    ] {
        assert_eq!(u[key], Value::Null, "{key}");
    }
    // COALESCE の項目は null では空にならない
    let u = update(
        &app,
        t,
        &b_id,
        json!({"title": null, "description": null, "next_action": null}),
    )
    .await;
    assert_eq!(
        (
            u["title"].clone(),
            u["description"].clone(),
            u["next_action"].clone()
        ),
        (json!("2件目"), json!("説明"), json!("電話"))
    );
    // 全部の項目を変える
    let other = Uuid::new_v4();
    let u = update(
        &app,
        t,
        &b_id,
        json!({
            "task_type": "報告", "title": "改", "description": "改説明", "status": "done",
            "assigned_to": other, "due_date": "2026-06-01T00:00:00Z",
            "completed_at": "2026-06-02T00:00:00Z", "sort_order": 9,
            "next_action": "訪問", "next_action_detail": "詳細", "next_action_by": "赤井",
            "next_action_due": "2026-06-03T00:00:00Z", "occurred_at": "2026-06-04T00:00:00Z",
            "print_page_break_before": true
        }),
    )
    .await;
    assert_eq!(
        (
            u["task_type"].clone(),
            u["title"].clone(),
            u["description"].clone(),
            u["status"].clone(),
            u["assigned_to"].clone(),
            u["due_date"].clone(),
            u["completed_at"].clone(),
            u["sort_order"].clone(),
        ),
        (
            json!("報告"),
            json!("改"),
            json!("改説明"),
            json!("done"),
            json!(other),
            json!("2026-06-01T00:00:00Z"),
            json!("2026-06-02T00:00:00Z"),
            json!(9)
        )
    );
    assert_eq!(
        (
            u["next_action"].clone(),
            u["next_action_detail"].clone(),
            u["next_action_by"].clone(),
            u["next_action_due"].clone(),
            u["occurred_at"].clone(),
            u["print_page_break_before"].clone(),
        ),
        (
            json!("訪問"),
            json!("詳細"),
            json!("赤井"),
            json!("2026-06-03T00:00:00Z"),
            json!("2026-06-04T00:00:00Z"),
            json!(true)
        )
    );
    assert_ne!(u["updated_at"], b["updated_at"]);
    // completed_at も空にできる
    let u = update(&app, t, &b_id, json!({"completed_at": ""})).await;
    assert_eq!(u["completed_at"], Value::Null);
    // 無いタスクの更新は 404
    let missing = Uuid::new_v4();
    let got = call(
        &app,
        "PUT",
        &format!("/api/trouble/tasks/{missing}"),
        Some(t),
        Some(json!({"title": "x"})),
    )
    .await;
    assert_eq!((got.status, got.body.is_empty()), (404, true));

    // 削除は物理削除。2 回目は 404
    let path = format!("/api/trouble/tasks/{b_id}");
    let got = call(&app, "DELETE", &path, Some(t), None).await;
    assert_eq!((got.status, got.body.is_empty()), (204, true));
    let got = call(&app, "DELETE", &path, Some(t), None).await;
    assert_eq!(got.status, 404);
    assert_eq!(
        titles(&by_ticket(&app, t, &tk).await),
        vec!["先頭", "1件目"]
    );
    let n: i64 = admin()
        .await
        .query_one(
            "SELECT count(*) FROM alc_api.trouble_tasks WHERE id = $1",
            &[&b_id.parse::<Uuid>().unwrap()],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 0);
    assert!(app.logs().is_empty());
}

/// 横断の一覧 (rust の test_list_all_tasks_cross_ticket と同じ組み立て・期待値) と、残りの絞り込み・並び
#[tokio::test]
async fn list_all_cross_ticket() {
    let app = app().await;
    let t = tenant().await;
    let setup = call(&app, "POST", "/api/trouble/workflow/setup", Some(t), None).await;
    assert_eq!(setup.status, 200);
    let ticket_a = ticket(&app, t, "貨物事故", "ドライバーA").await;
    let ticket_b = ticket(&app, t, "その他", "ドライバーB").await;
    let person = Uuid::new_v4();
    let a1 = create(
        &app,
        t,
        &ticket_a,
        json!({"title": "修理手配 alpha", "task_type": "修理手配",
               "due_date": "2026-03-01T00:00:00Z", "occurred_at": "2026-02-01T00:00:00Z",
               "next_action_due": "2026-03-05T00:00:00Z", "assigned_to": person}),
    )
    .await;
    let a2 = create(
        &app,
        t,
        &ticket_a,
        json!({"title": "連絡 beta", "task_type": "連絡",
               "due_date": "2026-04-01T00:00:00Z", "occurred_at": "2026-01-01T00:00:00Z"}),
    )
    .await;
    let a3 = create(
        &app,
        t,
        &ticket_a,
        json!({"title": "報告 gamma", "task_type": "報告", "description": "BETA を含む説明"}),
    )
    .await;
    let b1 = create(
        &app,
        t,
        &ticket_b,
        json!({"title": "修理手配 delta", "task_type": "修理手配"}),
    )
    .await;
    let b2 = create(
        &app,
        t,
        &ticket_b,
        json!({"title": "連絡 epsilon", "task_type": "連絡"}),
    )
    .await;
    let b3 = create(
        &app,
        t,
        &ticket_b,
        json!({"title": "報告 zeta", "task_type": "報告"}),
    )
    .await;
    update(&app, t, &id_of(&a1), json!({"status": "in_progress"})).await;
    update(&app, t, &id_of(&a2), json!({"status": "done"})).await;

    // 既定: 6 件、created_at の降順
    let body = list_all(&app, t, "").await;
    assert_eq!(
        (
            body["total"].clone(),
            body["page"].clone(),
            body["per_page"].clone()
        ),
        (json!(6), json!(1), json!(50))
    );
    let all: Vec<String> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(id_of)
        .collect();
    let created = [&a1, &a2, &a3, &b1, &b2, &b3].map(id_of);
    let mut newest_first = created.to_vec();
    newest_first.reverse();
    assert_eq!(all, newest_first);

    let ids =
        |q: &Value| -> Vec<String> { q["items"].as_array().unwrap().iter().map(id_of).collect() };
    // status=open → 4 件
    let body = list_all(&app, t, "?status=open").await;
    assert_eq!(body["total"], 4);
    assert!(body["items"]
        .as_array()
        .unwrap()
        .iter()
        .all(|i| i["status"] == "open"));
    // ticket_id → 3 件
    let body = list_all(&app, t, &format!("?ticket_id={ticket_a}")).await;
    assert_eq!(body["total"], 3);
    assert!(body["items"]
        .as_array()
        .unwrap()
        .iter()
        .all(|i| i["ticket_id"] == json!(ticket_a)));
    // q は題名と説明 (大文字小文字を見ない)
    let body = list_all(&app, t, "?q=beta").await;
    assert_eq!(ids(&body), vec![id_of(&a3), id_of(&a2)]);
    // task_type・assigned_to
    let body = list_all(&app, t, "?task_type=%E5%A0%B1%E5%91%8A").await;
    assert_eq!(ids(&body), vec![id_of(&b3), id_of(&a3)]);
    let body = list_all(&app, t, &format!("?assigned_to={person}")).await;
    assert_eq!(ids(&body), vec![id_of(&a1)]);
    // 期限・発生の範囲 (両端を含む)
    let body = list_all(
        &app,
        t,
        "?due_from=2026-03-01T00:00:00Z&due_to=2026-03-31T00:00:00Z",
    )
    .await;
    assert_eq!(ids(&body), vec![id_of(&a1)]);
    let body = list_all(&app, t, "?due_from=2026-03-02T00:00:00Z").await;
    assert_eq!(ids(&body), vec![id_of(&a2)]);
    let body = list_all(&app, t, "?occurred_from=2026-01-01T00:00:00Z").await;
    assert_eq!(body["total"], 2);
    let body = list_all(&app, t, "?occurred_to=2026-01-31T00:00:00Z").await;
    assert_eq!(ids(&body), vec![id_of(&a2)]);
    // 絞り込みを重ねる
    let body = list_all(
        &app,
        t,
        &format!("?ticket_id={ticket_b}&status=open&task_type=%E9%80%A3%E7%B5%A1&q=eps"),
    )
    .await;
    assert_eq!(ids(&body), vec![id_of(&b2)]);
    // 値は bind (SQL として解釈されない)
    let body = list_all(&app, t, "?q=%27%3B%20DROP%20TABLE%20x%3B%20--").await;
    assert_eq!(body["total"], 0);

    // ページ: per_page=2&page=2 → 2 件、total は 6
    let body = list_all(&app, t, "?per_page=2&page=2").await;
    assert_eq!(
        (
            body["total"].clone(),
            body["page"].clone(),
            body["per_page"].clone()
        ),
        (json!(6), json!(2), json!(2))
    );
    assert_eq!(ids(&body), newest_first[2..4].to_vec());
    // per_page は 1〜200、page は 1 から
    let body = list_all(&app, t, "?per_page=0&page=0").await;
    assert_eq!(
        (body["page"].clone(), body["per_page"].clone()),
        (json!(1), json!(1))
    );
    assert_eq!(body["items"].as_array().unwrap().len(), 1);
    let body = list_all(&app, t, "?per_page=1000").await;
    assert_eq!(body["per_page"], 200);

    // 許可リストに無い sort_by → 400 (本文は空)
    let got = call(
        &app,
        "GET",
        "/api/trouble/tasks?sort_by=bogus_column",
        Some(t),
        None,
    )
    .await;
    assert_eq!((got.status, got.body.is_empty()), (400, true));
    // status の昇順 → 先頭は done
    let body = list_all(&app, t, "?sort_by=status&sort_desc=false").await;
    let statuses: Vec<&str> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["status"].as_str().unwrap())
        .collect();
    assert_eq!(
        statuses,
        vec!["done", "in_progress", "open", "open", "open", "open"]
    );
    // 値の無い行は向きによらず後ろ (NULLS LAST)、同じ値は created_at の降順
    let body = list_all(&app, t, "?sort_by=due_date&sort_desc=false").await;
    assert_eq!(&ids(&body)[..3], &[id_of(&a1), id_of(&a2), id_of(&b3)]);
    let body = list_all(&app, t, "?sort_by=due_date").await;
    assert_eq!(&ids(&body)[..3], &[id_of(&a2), id_of(&a1), id_of(&b3)]);
    let body = list_all(&app, t, "?sort_by=occurred_at&sort_desc=false").await;
    assert_eq!(&ids(&body)[..2], &[id_of(&a2), id_of(&a1)]);
    let body = list_all(&app, t, "?sort_by=next_action_due").await;
    assert_eq!(ids(&body)[0], id_of(&a1));
    let body = list_all(&app, t, "?sort_by=created_at&sort_desc=false").await;
    assert_eq!(ids(&body), created.to_vec());
    // 読めない query は axum の 400
    let got = call(
        &app,
        "GET",
        "/api/trouble/tasks?ticket_id=nope",
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 400);
    assert!(app.logs().is_empty());
}

/// 並べ替え (rust の test_reorder_tasks と同じ組み立て・期待値)
#[tokio::test]
async fn reorder_tasks() {
    let app = app().await;
    let t = tenant().await;
    let tk = ticket(&app, t, "貨物事故", "ドライバーA").await;
    let other_ticket = ticket(&app, t, "その他", "ドライバーB").await;
    let t1 = create(&app, t, &tk, json!({"title": "1件目", "task_type": "連絡"})).await;
    let t2 = create(&app, t, &tk, json!({"title": "2件目", "task_type": "連絡"})).await;
    let t3 = create(&app, t, &tk, json!({"title": "3件目", "task_type": "報告"})).await;
    let foreign = create(
        &app,
        t,
        &other_ticket,
        json!({"title": "別チケット", "task_type": "連絡"}),
    )
    .await;
    let (id1, id2, id3) = (id_of(&t1), id_of(&t2), id_of(&t3));
    assert_eq!(
        [&t1, &t2, &t3].map(|x| x["sort_order"].as_i64().unwrap()),
        [0, 1, 2]
    );
    let url = format!("/api/trouble/tickets/{tk}/tasks/reorder");
    let reorder = |ids: Value| {
        let app = &app;
        let url = url.clone();
        async move { call(app, "PUT", &url, Some(t), Some(json!({"task_ids": ids}))).await }
    };

    let got = reorder(json!([id3, id1, id2])).await;
    assert_eq!(got.status, 200);
    let body = got.json();
    assert_eq!(titles(&body), vec!["3件目", "1件目", "2件目"]);
    let orders: Vec<i64> = body
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["sort_order"].as_i64().unwrap())
        .collect();
    assert_eq!(orders, vec![0, 1, 2]);
    assert_eq!(
        titles(&by_ticket(&app, t, &tk).await),
        vec!["3件目", "1件目", "2件目"]
    );

    // 新規は末尾
    let t4 = create(
        &app,
        t,
        &tk,
        json!({"title": "4件目", "task_type": "その他"}),
    )
    .await;
    assert_eq!(t4["sort_order"], 3);

    // 空・重複 → 400
    for ids in [json!([]), json!([id1, id1])] {
        let got = reorder(ids).await;
        assert_eq!((got.status, got.body.is_empty()), (400, true));
    }
    // 別チケットの task_id・無い id が混ざる → 404 で並びは変わらない
    let before = by_ticket(&app, t, &tk).await;
    for ids in [
        json!([id1, id2, id_of(&foreign)]),
        json!([id1, Uuid::new_v4()]),
    ] {
        let got = reorder(ids).await;
        assert_eq!((got.status, got.body.is_empty()), (404, true));
    }
    let after = by_ticket(&app, t, &tk).await;
    assert_eq!(after, before);
    assert_eq!(titles(&after), vec!["3件目", "1件目", "2件目", "4件目"]);
    // 別チケットの行も手付かず
    assert_eq!(by_ticket(&app, t, &other_ticket).await, json!([foreign]));

    // 一部だけ渡すと、渡した行だけ 0 から振り直す (rust と同じ。同じ sort_order は作成の順)
    let got = reorder(json!([id_of(&t4), id3])).await;
    assert_eq!(got.status, 200);
    let body = got.json();
    let pairs: Vec<(String, i64)> = body
        .as_array()
        .unwrap()
        .iter()
        .map(|v| {
            (
                v["title"].as_str().unwrap().to_owned(),
                v["sort_order"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        pairs,
        vec![
            ("4件目".to_owned(), 0),
            ("1件目".to_owned(), 1),
            ("3件目".to_owned(), 1),
            ("2件目".to_owned(), 2)
        ]
    );
    // 読めない本文は axum の 4xx
    let got = reorder(json!(["nope"])).await;
    assert_eq!(got.status, 422);
    assert!(app.logs().is_empty());
}

/// 別の tenant の行は見えない・変えられない・並べ替えられない
#[tokio::test]
async fn rls_hides_other_tenant_rows() {
    let app = app().await;
    let a = tenant().await;
    let b = tenant().await;
    let tk = ticket(&app, a, "貨物事故", "A").await;
    let task = create(&app, a, &tk, json!({"title": "A の", "task_type": "連絡"})).await;
    let id = id_of(&task);

    assert_eq!(list_all(&app, a, "").await["total"], 1);
    let body = list_all(&app, b, "").await;
    assert_eq!(
        (body["total"].clone(), body["items"].clone()),
        (json!(0), json!([]))
    );
    assert_eq!(
        list_all(&app, b, &format!("?ticket_id={tk}")).await["total"],
        0
    );
    assert_eq!(by_ticket(&app, b, &tk).await, json!([]));
    let path = format!("/api/trouble/tasks/{id}");
    let got = call(&app, "PUT", &path, Some(b), Some(json!({"title": "x"}))).await;
    assert_eq!(got.status, 404);
    assert_eq!(call(&app, "DELETE", &path, Some(b), None).await.status, 404);
    let got = call(
        &app,
        "PUT",
        &format!("/api/trouble/tickets/{tk}/tasks/reorder"),
        Some(b),
        Some(json!({"task_ids": [id]})),
    )
    .await;
    assert_eq!(got.status, 404);
    // A の行は手付かず
    assert_eq!(by_ticket(&app, a, &tk).await, json!([task]));
    assert!(app.logs().is_empty());
}

/// 接続が切れた DB・書けないロール: どの口も 500 (本文は空)。ログは段の名前と kind だけ
#[tokio::test]
async fn db_failure_is_500_with_fixed_logs() {
    let t = tenant().await;
    let id = Uuid::new_v4();
    let cases: Vec<(&str, String, Option<Value>, &str)> = vec![
        ("GET", "/api/trouble/tasks".into(), None, "task list all"),
        (
            "POST",
            format!("/api/trouble/tickets/{id}/tasks"),
            Some(json!({"task_type": "連絡", "title": "x"})),
            "task create",
        ),
        (
            "GET",
            format!("/api/trouble/tickets/{id}/tasks"),
            None,
            "task list",
        ),
        (
            "PUT",
            format!("/api/trouble/tickets/{id}/tasks/reorder"),
            Some(json!({"task_ids": [id]})),
            "task reorder",
        ),
        (
            "PUT",
            format!("/api/trouble/tasks/{id}"),
            Some(json!({})),
            "task update",
        ),
        (
            "DELETE",
            format!("/api/trouble/tasks/{id}"),
            None,
            "task delete",
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
            "{method} {path}: {}",
            logs[0]
        );
        assert_logs_clean(&app, &[t, id]);
    }

    // 無いチケットへの作成は外部キーの違反 (rust と同じく 500)
    let rw = app().await;
    let got = call(
        &rw,
        "POST",
        &format!("/api/trouble/tickets/{id}/tasks"),
        Some(t),
        Some(json!({"task_type": "連絡", "title": "x"})),
    )
    .await;
    assert_eq!((got.status, got.body.is_empty()), (500, true));
    assert_eq!(rw.logs(), vec!["trouble: task create failed: db (23503)"]);

    // 読めるが書けないロール: 作成・更新・削除・並べ替えは 42501
    let rw = app().await;
    let tk = ticket(&rw, t, "その他", "x").await;
    let task = id_of(&create(&rw, t, &tk, json!({"task_type": "連絡", "title": "x"})).await);
    let app = build(client_as(READ_ONLY_ROLE).await);
    let cases: Vec<(&str, String, Option<Value>, &str)> = vec![
        (
            "POST",
            format!("/api/trouble/tickets/{tk}/tasks"),
            Some(json!({"task_type": "連絡", "title": "x"})),
            "task create",
        ),
        (
            "PUT",
            format!("/api/trouble/tickets/{tk}/tasks/reorder"),
            Some(json!({"task_ids": [task]})),
            "task reorder",
        ),
        (
            "PUT",
            format!("/api/trouble/tasks/{task}"),
            Some(json!({"title": "y"})),
            "task update",
        ),
        (
            "DELETE",
            format!("/api/trouble/tasks/{task}"),
            None,
            "task delete",
        ),
    ];
    for (method, path, body, stage) in cases {
        let got = call(&app, method, &path, Some(t), body).await;
        assert_eq!(
            (got.status, got.body.is_empty()),
            (500, true),
            "{method} {path}"
        );
        assert_eq!(
            app.logs().last().unwrap(),
            &format!("trouble: {stage} failed: db (42501)")
        );
    }
    assert_eq!(app.logs().len(), 4);
    // 読むのはできる
    assert_eq!(list_all(&app, t, "").await["total"], 1);
    assert_logs_clean(&app, &[t]);
}
