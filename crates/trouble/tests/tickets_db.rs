//! チケット・状態の流れの口を、実 DB (`alc_api_rt` で接続) で通す。期待値は rust-alc-api の tests/trouble_test.rs
//! (応答の形・並び・検索) に合わせる。
//! 接続先は `TROUBLE_TEST_ADMIN_DATABASE_URL` (無ければ失敗する。tests/support)。

mod support;

use serde_json::{json, Value};
use support::*;
use uuid::Uuid;

const TICKET_KEYS: &[&str] = &[
    "allowance",
    "assigned_to",
    "category",
    "company_name",
    "compensation_amount",
    "confirmation_notice",
    "counterparty",
    "counterparty_insurance",
    "counterparty_vehicle",
    "created_at",
    "created_by",
    "custom_fields",
    "damage_amount",
    "deleted_at",
    "department",
    "description",
    "disciplinary_action",
    "disciplinary_committee",
    "disciplinary_content",
    "due_date",
    "id",
    "location",
    "occurred_at",
    "occurred_date",
    "office_name",
    "overdue_notified_at",
    "person_id",
    "person_is_external",
    "person_name",
    "progress_notes",
    "registration_number",
    "road_service_cost",
    "status_id",
    "tenant_id",
    "ticket_no",
    "title",
    "updated_at",
];

fn keys(v: &Value) -> Vec<String> {
    let mut k: Vec<String> = v.as_object().unwrap().keys().cloned().collect();
    k.sort();
    k
}

fn ids(body: &Value) -> Vec<String> {
    body["tickets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["id"].as_str().unwrap().to_owned())
        .collect()
}

fn id_of(v: &Value) -> String {
    v["id"].as_str().unwrap().to_owned()
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

/// 既定の状態の流れを作り、(名前 → id) を返す
async fn setup(app: &App, t: Uuid) -> std::collections::HashMap<String, String> {
    let got = call(app, "POST", "/api/trouble/workflow/setup", Some(t), None).await;
    assert_eq!(got.status, 200);
    got.json()
        .as_array()
        .unwrap()
        .iter()
        .map(|s| (s["name"].as_str().unwrap().to_owned(), id_of(s)))
        .collect()
}

async fn create(app: &App, t: Uuid, body: Value) -> Value {
    let got = call(app, "POST", "/api/trouble/tickets", Some(t), Some(body)).await;
    assert_eq!(got.status, 201, "{}", String::from_utf8_lossy(&got.body));
    got.json()
}

async fn list(app: &App, t: Uuid, query: &str) -> Value {
    let got = call(
        app,
        "GET",
        &format!("/api/trouble/tickets{query}"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 200, "{query}");
    got.json()
}

/// tenant のヘッダーが無ければ、足した口もどれも 401
#[tokio::test]
async fn tenant_header_is_required() {
    let app = app().await;
    let id = Uuid::new_v4();
    let paths = [
        ("GET", "/api/trouble/tickets".to_owned()),
        ("POST", "/api/trouble/tickets".to_owned()),
        ("GET", "/api/trouble/tickets/csv".to_owned()),
        ("GET", format!("/api/trouble/tickets/{id}")),
        ("PUT", format!("/api/trouble/tickets/{id}")),
        ("DELETE", format!("/api/trouble/tickets/{id}")),
        ("POST", format!("/api/trouble/tickets/{id}/transition")),
        ("GET", format!("/api/trouble/tickets/{id}/history")),
        ("GET", "/api/trouble/workflow/states".to_owned()),
        ("DELETE", format!("/api/trouble/workflow/states/{id}")),
        ("POST", "/api/trouble/workflow/transitions".to_owned()),
        ("DELETE", format!("/api/trouble/workflow/transitions/{id}")),
        ("POST", "/api/trouble/workflow/setup".to_owned()),
    ];
    for (method, path) in &paths {
        let got = call(&app, method, path, None, None).await;
        assert_eq!(
            (got.status, got.body.is_empty()),
            (401, true),
            "{method} {path}"
        );
    }
    assert!(app.logs().is_empty());
}

/// 作成 → 一覧 → 取得 → 更新 (登録番号の空にする・保つ) → 削除 → 404。応答の形
#[tokio::test]
async fn ticket_crud() {
    let app = app().await;
    let t = tenant().await;
    let states = setup(&app, t).await;

    let ticket = create(
        &app,
        t,
        json!({
            "category": "貨物事故",
            "title": "テスト事故報告",
            "occurred_at": "2026-07-20T10:00:00Z",
            "occurred_date": "2026-07-20",
            "company_name": "会社",
            "office_name": "営業所",
            "department": "運行課",
            "person_name": "テスト太郎",
            "person_id": "",
            "person_is_external": true,
            "registration_number": "品川300あ1234",
            "location": "東京",
            "description": "テスト説明",
            "assigned_to": "",
            "damage_amount": 1234.5,
            "compensation_amount": 10,
            "road_service_cost": 0.125,
            "counterparty": "相手",
            "counterparty_insurance": "保険",
            "custom_fields": {"k": [1, 2]},
            "due_date": ""
        }),
    )
    .await;
    assert_eq!(keys(&ticket), TICKET_KEYS);
    let id = id_of(&ticket);
    let ticket_id: Uuid = id.parse().unwrap();
    assert_eq!(ticket["tenant_id"], json!(t));
    assert!(ticket["ticket_no"].as_i64().unwrap() > 0);
    assert_eq!(ticket["status_id"], json!(states["new"]));
    assert_eq!(ticket["occurred_at"], "2026-07-20T10:00:00Z");
    assert_eq!(ticket["occurred_date"], "2026-07-20");
    assert_eq!(ticket["person_is_external"], true);
    assert_eq!(ticket["person_id"], Value::Null);
    assert_eq!(ticket["assigned_to"], Value::Null);
    assert_eq!(ticket["damage_amount"], "1234.50");
    assert_eq!(ticket["compensation_amount"], "10.00");
    assert_eq!(ticket["road_service_cost"], "0.13");
    assert_eq!(ticket["custom_fields"], json!({"k": [1, 2]}));
    assert_eq!(ticket["due_date"], Value::Null);
    assert_eq!(ticket["created_by"], Value::Null);
    assert_eq!(ticket["deleted_at"], Value::Null);
    for k in [
        "progress_notes",
        "allowance",
        "counterparty_vehicle",
        "disciplinary_committee",
    ] {
        assert_eq!(ticket[k], "", "{k}");
    }

    // 項目を省いた作成は空文字・既定値
    let minimal = create(&app, t, json!({"category": "その他"})).await;
    assert_eq!(minimal["title"], "");
    assert_eq!(minimal["person_is_external"], false);
    assert_eq!(minimal["custom_fields"], json!({}));
    assert_eq!(minimal["damage_amount"], Value::Null);
    assert!(minimal["ticket_no"].as_i64() > ticket["ticket_no"].as_i64());

    let body = list(&app, t, "").await;
    assert_eq!(body["total"], 2);
    assert_eq!(
        (body["page"].clone(), body["per_page"].clone()),
        (json!(1), json!(50))
    );
    assert_eq!(body["tickets"][1], ticket);

    let got = call(
        &app,
        "GET",
        &format!("/api/trouble/tickets/{id}"),
        Some(t),
        None,
    )
    .await;
    assert_eq!((got.status, got.json()), (200, ticket.clone()));

    let put = |body: Value| {
        let app = &app;
        let path = format!("/api/trouble/tickets/{id}");
        async move { call(app, "PUT", &path, Some(t), Some(body)).await }
    };
    let got = put(json!({
        "progress_notes": "対応中",
        "registration_number": "横浜500さ5678",
        "damage_amount": 1,
        "disciplinary_committee": "委員会",
        "counterparty_vehicle": "車",
        "occurred_at": ""
    }))
    .await;
    assert_eq!(got.status, 200);
    let updated = got.json();
    assert_eq!(updated["progress_notes"], "対応中");
    assert_eq!(updated["registration_number"], "横浜500さ5678");
    assert_eq!(updated["damage_amount"], "1.00");
    assert_eq!(updated["disciplinary_committee"], "委員会");
    assert_eq!(updated["counterparty_vehicle"], "車");
    // 来なかった・空文字の項目はそのまま
    assert_eq!(updated["occurred_at"], ticket["occurred_at"]);
    assert_eq!(updated["category"], "貨物事故");
    assert_eq!(updated["created_at"], ticket["created_at"]);
    assert_ne!(updated["updated_at"], ticket["updated_at"]);

    let cleared = put(json!({"registration_number": null})).await.json();
    assert_eq!(cleared["registration_number"], "");
    assert_eq!(cleared["progress_notes"], "対応中");
    let kept = put(json!({"progress_notes": "再対応", "assigned_to": ticket_id}))
        .await
        .json();
    assert_eq!(kept["registration_number"], "");
    assert_eq!(kept["assigned_to"], json!(ticket_id));

    let other = Uuid::new_v4();
    let got = call(
        &app,
        "PUT",
        &format!("/api/trouble/tickets/{other}"),
        Some(t),
        Some(json!({})),
    )
    .await;
    assert_eq!((got.status, got.body.is_empty()), (404, true));

    let del = format!("/api/trouble/tickets/{id}");
    assert_eq!(call(&app, "DELETE", &del, Some(t), None).await.status, 204);
    assert_eq!(call(&app, "GET", &del, Some(t), None).await.status, 404);
    assert_eq!(call(&app, "DELETE", &del, Some(t), None).await.status, 404);
    assert_eq!(put(json!({})).await.status, 404);
    assert_eq!(list(&app, t, "").await["total"], 1);
    // 不正な body・path は axum の拒否 (rust と同じ)
    let got = call(
        &app,
        "GET",
        "/api/trouble/tickets/not-a-uuid",
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 400);
    let got = call(
        &app,
        "POST",
        "/api/trouble/tickets",
        Some(t),
        Some(json!({"category": "その他", "person_id": "nope"})),
    )
    .await;
    assert_eq!(got.status, 422);
    assert!(app.logs().is_empty());
}

/// 分類の検査: マスタが空なら既定の 7 つ、在ればマスタの名前だけ。状態の流れが無ければ status_id は null で履歴も無い
#[tokio::test]
async fn ticket_category_and_no_workflow() {
    let app = app().await;
    let t = tenant().await;
    let got = call(
        &app,
        "POST",
        "/api/trouble/tickets",
        Some(t),
        Some(json!({"category": "invalid_category_name", "title": "should fail"})),
    )
    .await;
    assert_eq!((got.status, got.body.is_empty()), (400, true));

    let ticket = create(&app, t, json!({"category": "人身事故"})).await;
    assert_eq!(ticket["status_id"], Value::Null);
    let id = id_of(&ticket);
    let got = call(
        &app,
        "GET",
        &format!("/api/trouble/tickets/{id}/history"),
        Some(t),
        None,
    )
    .await;
    assert_eq!((got.status, got.json()), (200, json!([])));

    // tenant の分類を作ると、既定の分類は通らない
    let got = call(
        &app,
        "POST",
        "/api/trouble/categories",
        Some(t),
        Some(json!({"name": "独自分類"})),
    )
    .await;
    assert_eq!(got.status, 201);
    let got = call(
        &app,
        "POST",
        "/api/trouble/tickets",
        Some(t),
        Some(json!({"category": "人身事故"})),
    )
    .await;
    assert_eq!(got.status, 400);
    create(&app, t, json!({"category": "独自分類"})).await;
    assert!(app.logs().is_empty());
}

/// 一覧の絞り込み 8 種とページ
#[tokio::test]
async fn ticket_list_filters() {
    let app = app().await;
    let t = tenant().await;
    let states = setup(&app, t).await;
    let a = create(
        &app,
        t,
        json!({
            "category": "貨物事故",
            "person_name": "山田太郎",
            "company_name": "大石運輸",
            "office_name": "本社営業所",
            "occurred_date": "2026-07-01",
            "registration_number": "品川５００あ１２３４",
            "description": "フェンダー凹み",
            "location": "環七"
        }),
    )
    .await;
    let b = create(
        &app,
        t,
        json!({
            "category": "被害事故",
            "person_name": "佐藤花子",
            "company_name": "別会社",
            "office_name": "支店",
            "occurred_date": "2026-07-10",
            "registration_number": "多摩300う5678",
            "description": "バンパー傷"
        }),
    )
    .await;
    let c = create(
        &app,
        t,
        json!({
            "category": "貨物事故",
            "person_name": "鈴木",
            "occurred_date": "2026-07-20",
            "registration_number": "京都１２－３４",
            "description": "ドア凹み"
        }),
    )
    .await;
    let (a, b, c) = (id_of(&a), id_of(&b), id_of(&c));
    // b だけ対応中にする
    let got = call(
        &app,
        "POST",
        &format!("/api/trouble/tickets/{b}/transition"),
        Some(t),
        Some(json!({"to_state_id": states["in_progress"]})),
    )
    .await;
    assert_eq!(got.status, 200);

    let cases: Vec<(String, Vec<&String>)> = vec![
        ("".into(), vec![&c, &b, &a]),
        (
            "?category=%E8%B2%A8%E7%89%A9%E4%BA%8B%E6%95%85".into(),
            vec![&c, &a],
        ),
        (format!("?status_id={}", states["in_progress"]), vec![&b]),
        (format!("?status_id={}", states["new"]), vec![&c, &a]),
        // ILIKE の部分一致 (大文字小文字は問わない・前後どちらでも)
        ("?person_name=%E5%A4%AA%E9%83%8E".into(), vec![&a]),
        ("?company_name=%E9%81%8B%E8%BC%B8".into(), vec![&a]),
        ("?office_name=%E6%94%AF".into(), vec![&b]),
        ("?date_from=2026-07-10".into(), vec![&c, &b]),
        ("?date_to=2026-07-10".into(), vec![&b, &a]),
        ("?date_from=2026-07-02&date_to=2026-07-19".into(), vec![&b]),
        // q: 説明・場所・当事者・登録番号 (全角 / 半角・ハイフンを揃える)
        ("?q=%E5%87%B9%E3%81%BF".into(), vec![&c, &a]),
        ("?q=%E7%92%B0%E4%B8%83".into(), vec![&a]),
        ("?q=%E4%BD%90%E8%97%A4".into(), vec![&b]),
        ("?q=1234".into(), vec![&a]),
        ("?q=%EF%BC%95%EF%BC%96%EF%BC%97%EF%BC%98".into(), vec![&b]),
        ("?q=12-34".into(), vec![&c]),
        // % と _ は利用者の値の中ではワイルドカードとして効く (rust と同じ)
        ("?q=%25".into(), vec![&c, &b, &a]),
        ("?q=%27%3B%20DROP".into(), vec![]),
        (
            "?category=%E8%B2%A8%E7%89%A9%E4%BA%8B%E6%95%85&q=%E3%83%89%E3%82%A2".into(),
            vec![&c],
        ),
    ];
    for (query, want) in cases {
        let body = list(&app, t, &query).await;
        let want: Vec<String> = want.into_iter().cloned().collect();
        assert_eq!(ids(&body), want, "{query}");
        assert_eq!(body["total"], want.len(), "{query}");
    }

    // ページ: total は絞り込み全体の件数、page < 1 は 1
    let body = list(&app, t, "?per_page=2&page=2").await;
    assert_eq!(ids(&body), vec![a.clone()]);
    assert_eq!(
        (
            body["total"].clone(),
            body["page"].clone(),
            body["per_page"].clone()
        ),
        (json!(3), json!(2), json!(2))
    );
    let body = list(&app, t, "?per_page=2&page=0").await;
    assert_eq!(ids(&body), vec![c.clone(), b.clone()]);
    assert_eq!(body["page"], 1);
    let body = list(&app, t, "?per_page=20000").await;
    assert_eq!(body["per_page"], 10000);
    // 読めない値は 400 (axum の Query の拒否)
    let got = call(
        &app,
        "GET",
        "/api/trouble/tickets?status_id=x",
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 400);
    let got = call(
        &app,
        "GET",
        "/api/trouble/tickets?date_from=2026-13-01",
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 400);
    assert!(app.logs().is_empty());
}

/// 並び (rust の test_trouble_ticket_list_sort と同じ)
#[tokio::test]
async fn ticket_list_sort() {
    let app = app().await;
    let t = tenant().await;
    setup(&app, t).await;
    let mut created = Vec::new();
    for (occurred_at, occurred_date) in [
        (Some("2026-07-20T10:00:00Z"), "2026-07-20"),
        (Some("2026-07-01T09:00:00Z"), "2026-07-01"),
        (None, "2026-07-10"),
    ] {
        let mut body = json!({"category": "貨物事故", "occurred_date": occurred_date});
        if let Some(at) = occurred_at {
            body["occurred_at"] = json!(at);
        }
        created.push(id_of(&create(&app, t, body).await));
    }
    // 発生日の無い行は occurred の並びで最後
    let none = id_of(&create(&app, t, json!({"category": "貨物事故"})).await);
    let (new, old, mid) = (&created[0], &created[1], &created[2]);
    let cases: Vec<(&str, Vec<&String>)> = vec![
        (
            "?sort_by=occurred&sort_desc=true",
            vec![new, mid, old, &none],
        ),
        ("?sort_by=occurred", vec![new, mid, old, &none]),
        (
            "?sort_by=occurred&sort_desc=false",
            vec![old, mid, new, &none],
        ),
        (
            "?sort_by=ticket_no&sort_desc=false",
            vec![new, old, mid, &none],
        ),
        (
            "?sort_by=ticket_no&sort_desc=true",
            vec![&none, mid, old, new],
        ),
        ("", vec![&none, mid, old, new]),
        ("?sort_desc=false", vec![&none, mid, old, new]),
        (
            "?sort_by=person_name%3B%20DROP%20TABLE%20trouble_tickets",
            vec![&none, mid, old, new],
        ),
    ];
    for (query, want) in cases {
        let want: Vec<String> = want.into_iter().cloned().collect();
        assert_eq!(ids(&list(&app, t, query).await), want, "{query}");
    }
}

/// CSV: BOM・見出し・列・エスケープ (csv crate の既定)・Content-Type / Content-Disposition。絞り込みも効く
#[tokio::test]
async fn ticket_csv() {
    let app = app().await;
    let t = tenant().await;
    let a = create(
        &app,
        t,
        json!({
            "category": "人身事故",
            "occurred_date": "2026-07-01",
            "company_name": "会社,A",
            "office_name": "営業所",
            "department": "課",
            "person_name": "CSV太郎",
            "registration_number": "品川1",
            "location": "場所",
            "description": "言った \"こと\"\n2 行目",
            "damage_amount": 100,
            "road_service_cost": 5.5,
            "counterparty": "相手",
            "counterparty_insurance": "保険"
        }),
    )
    .await;
    let b = create(&app, t, json!({"category": "その他"})).await;
    let put = call(
        &app,
        "PUT",
        &format!("/api/trouble/tickets/{}", id_of(&a)),
        Some(t),
        Some(json!({
            "progress_notes": "進捗",
            "allowance": "手当",
            "compensation_amount": 7,
            "confirmation_notice": "確認",
            "disciplinary_content": "検討",
            "disciplinary_action": "処分"
        })),
    )
    .await;
    assert_eq!(put.status, 200);

    let req = axum::http::Request::builder()
        .uri("/api/trouble/tickets/csv")
        .header("X-Tenant-ID", t.to_string())
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = tower::ServiceExt::oneshot(app.router.clone(), req)
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.headers()["content-type"], "text/csv; charset=utf-8");
    assert_eq!(
        resp.headers()["content-disposition"],
        "attachment; filename=\"trouble_tickets.csv\""
    );
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let mut want = vec![0xEF, 0xBB, 0xBF];
    want.extend_from_slice(
        format!(
            "No,発生日,所属会社名,営業所名,運行課,当事者名,登録番号,事故等分類,発生場所,内容,進捗状況,手当等,損害額,賠償額,確認書_決定通知書,処分検討内容,処分内容,ロードサービス費用,相手,相手保険会社\n\
             {},,,,,,,その他,,,,,,,,,,,,\n\
             {},2026-07-01,\"会社,A\",営業所,課,CSV太郎,品川1,人身事故,場所,\"言った \"\"こと\"\"\n2 行目\",進捗,手当,100.00,7.00,確認,検討,処分,5.50,相手,保険\n",
            b["ticket_no"], a["ticket_no"]
        )
        .as_bytes(),
    );
    assert_eq!(
        String::from_utf8_lossy(&body),
        String::from_utf8_lossy(&want)
    );

    // 絞り込みが効き、ページの指定は無視する (全件を 1 ページで)
    let got = call(
        &app,
        "GET",
        "/api/trouble/tickets/csv?q=CSV&per_page=1&page=5",
        Some(t),
        None,
    )
    .await;
    let text = String::from_utf8(got.body[3..].to_vec()).unwrap();
    assert_eq!(text.lines().filter(|l| l.contains("CSV太郎")).count(), 1);
    assert!(!text.contains("その他"));
    assert!(app.logs().is_empty());
}

/// 状態の遷移: 許された移り方だけ (無ければ 422)・履歴。状態の無いチケットは初期の状態へだけ
#[tokio::test]
async fn ticket_transition_and_history() {
    let app = app().await;
    let t = tenant().await;
    // 状態の流れの前に作ったチケット (status_id = null)
    let bare = id_of(&create(&app, t, json!({"category": "被害事故"})).await);
    let states = setup(&app, t).await;
    let ticket = create(
        &app,
        t,
        json!({"category": "被害事故", "title": "遷移テスト"}),
    )
    .await;
    let id = id_of(&ticket);

    let transition = |ticket: String, to: String, comment: Option<&str>| {
        let app = &app;
        let mut body = json!({"to_state_id": to});
        if let Some(c) = comment {
            body["comment"] = json!(c);
        }
        async move {
            call(
                app,
                "POST",
                &format!("/api/trouble/tickets/{ticket}/transition"),
                Some(t),
                Some(body),
            )
            .await
        }
    };

    let got = transition(id.clone(), states["in_progress"].clone(), Some("開始")).await;
    assert_eq!(got.status, 200);
    let updated = got.json();
    assert_eq!(updated["status_id"], json!(states["in_progress"]));
    assert_eq!(updated["ticket_no"], ticket["ticket_no"]);

    // 遷移の表に無い (対応中 → 対応中) は 422
    let got = transition(id.clone(), states["in_progress"].clone(), None).await;
    assert_eq!((got.status, got.body.is_empty()), (422, true));
    // 完了へは直接行ける (migration 111 の (1,3))
    assert_eq!(
        transition(id.clone(), states["closed"].clone(), None)
            .await
            .status,
        200
    );
    // 完了から先は無い
    assert_eq!(
        transition(id.clone(), states["new"].clone(), None)
            .await
            .status,
        422
    );
    // 知らない状態・知らないチケット
    assert_eq!(
        transition(id.clone(), Uuid::new_v4().to_string(), None)
            .await
            .status,
        422
    );
    let got = transition(Uuid::new_v4().to_string(), states["new"].clone(), None).await;
    assert_eq!((got.status, got.body.is_empty()), (404, true));
    // 状態の無いチケットは初期の状態へだけ
    assert_eq!(
        transition(bare.clone(), states["resolved"].clone(), None)
            .await
            .status,
        422
    );
    assert_eq!(
        transition(bare.clone(), states["new"].clone(), None)
            .await
            .status,
        200
    );

    let got = call(
        &app,
        "GET",
        &format!("/api/trouble/tickets/{id}/history"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 200);
    let history = got.json();
    let steps: Vec<(Value, Value, Value)> = history
        .as_array()
        .unwrap()
        .iter()
        .map(|h| {
            (
                h["from_state_id"].clone(),
                h["to_state_id"].clone(),
                h["comment"].clone(),
            )
        })
        .collect();
    assert_eq!(
        steps,
        vec![
            (Value::Null, json!(states["new"]), json!("")),
            (
                json!(states["new"]),
                json!(states["in_progress"]),
                json!("開始")
            ),
            (
                json!(states["in_progress"]),
                json!(states["closed"]),
                json!("")
            ),
        ]
    );
    let mut k: Vec<&String> = history[0].as_object().unwrap().keys().collect();
    k.sort();
    assert_eq!(
        k,
        [
            "changed_by",
            "comment",
            "created_at",
            "from_state_id",
            "id",
            "tenant_id",
            "ticket_id",
            "to_state_id"
        ]
    );
    assert_eq!(history[0]["changed_by"], Value::Null);
    assert_eq!(history[0]["ticket_id"], json!(id));
    let got = call(
        &app,
        "GET",
        &format!("/api/trouble/tickets/{bare}/history"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.json().as_array().unwrap().len(), 1);
    assert!(app.logs().is_empty());
}

/// 状態の流れ: setup (2 回目は作らない)・状態と遷移の CRUD・409・404・500
#[tokio::test]
async fn workflow_crud() {
    let app = app().await;
    let t = tenant().await;
    let got = call(&app, "GET", "/api/trouble/workflow/states", Some(t), None).await;
    assert_eq!((got.status, got.json()), (200, json!([])));

    let got = call(&app, "POST", "/api/trouble/workflow/setup", Some(t), None).await;
    assert_eq!(got.status, 200);
    let states = got.json();
    let summary: Vec<(Value, Value, Value, Value, Value, Value)> = states
        .as_array()
        .unwrap()
        .iter()
        .map(|s| {
            (
                s["name"].clone(),
                s["label"].clone(),
                s["color"].clone(),
                s["sort_order"].clone(),
                s["is_initial"].clone(),
                s["is_terminal"].clone(),
            )
        })
        .collect();
    assert_eq!(
        summary,
        vec![
            (
                json!("new"),
                json!("新規"),
                json!("#3B82F6"),
                json!(1),
                json!(true),
                json!(false)
            ),
            (
                json!("in_progress"),
                json!("対応中"),
                json!("#F59E0B"),
                json!(2),
                json!(false),
                json!(false)
            ),
            (
                json!("resolved"),
                json!("解決"),
                json!("#10B981"),
                json!(3),
                json!(false),
                json!(false)
            ),
            (
                json!("closed"),
                json!("完了"),
                json!("#6B7280"),
                json!(4),
                json!(false),
                json!(true)
            ),
        ]
    );
    assert_eq!(
        keys(&states[0]),
        [
            "color",
            "created_at",
            "id",
            "is_initial",
            "is_terminal",
            "label",
            "name",
            "sort_order",
            "tenant_id"
        ]
    );
    let sid = |i: usize| states[i]["id"].clone();
    let got = call(
        &app,
        "GET",
        "/api/trouble/workflow/transitions",
        Some(t),
        None,
    )
    .await;
    let transitions = got.json();
    let mut pairs: Vec<(Value, Value, Value)> = transitions
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            (
                r["from_state_id"].clone(),
                r["to_state_id"].clone(),
                r["label"].clone(),
            )
        })
        .collect();
    let mut want = vec![
        (sid(0), sid(1), json!("対応開始")),
        (sid(1), sid(2), json!("解決")),
        (sid(2), sid(3), json!("完了")),
        (sid(1), sid(0), json!("差し戻し")),
        (sid(2), sid(1), json!("再対応")),
        (sid(0), sid(3), json!("完了")),
        (sid(1), sid(3), json!("完了")),
    ];
    let key = |p: &(Value, Value, Value)| format!("{}{}", p.0, p.1);
    pairs.sort_by_key(key);
    want.sort_by_key(key);
    assert_eq!(pairs, want);
    assert_eq!(
        keys(&transitions[0]),
        [
            "created_at",
            "from_state_id",
            "id",
            "label",
            "tenant_id",
            "to_state_id"
        ]
    );

    // 2 回目の setup は作らずにいまの状態を返す
    let got = call(&app, "POST", "/api/trouble/workflow/setup", Some(t), None).await;
    assert_eq!((got.status, got.json()), (200, states.clone()));
    let got = call(
        &app,
        "GET",
        "/api/trouble/workflow/transitions",
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.json().as_array().unwrap().len(), 7);

    // 状態の追加 (省いた項目は既定値)・名前の重複は 409
    let got = call(
        &app,
        "POST",
        "/api/trouble/workflow/states",
        Some(t),
        Some(json!({"name": "on_hold", "label": "保留"})),
    )
    .await;
    assert_eq!(got.status, 201);
    let on_hold = got.json();
    assert_eq!(
        (
            on_hold["color"].clone(),
            on_hold["sort_order"].clone(),
            on_hold["is_initial"].clone(),
            on_hold["is_terminal"].clone()
        ),
        (json!("#6B7280"), json!(0), json!(false), json!(false))
    );
    let got = call(
        &app,
        "POST",
        "/api/trouble/workflow/states",
        Some(t),
        Some(json!({"name": "pending", "label": "保留", "color": "#FF0000", "sort_order": 9, "is_initial": true, "is_terminal": true})),
    )
    .await;
    assert_eq!(got.status, 201);
    let pending = got.json();
    assert_eq!(
        (
            pending["color"].clone(),
            pending["sort_order"].clone(),
            pending["is_initial"].clone(),
            pending["is_terminal"].clone()
        ),
        (json!("#FF0000"), json!(9), json!(true), json!(true))
    );
    let got = call(
        &app,
        "POST",
        "/api/trouble/workflow/states",
        Some(t),
        Some(json!({"name": "on_hold", "label": "x"})),
    )
    .await;
    assert_eq!((got.status, got.body.is_empty()), (409, true));
    let got = call(&app, "GET", "/api/trouble/workflow/states", Some(t), None).await;
    let names: Vec<Value> = got
        .json()
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].clone())
        .collect();
    assert_eq!(
        names,
        vec![
            json!("on_hold"),
            json!("new"),
            json!("in_progress"),
            json!("resolved"),
            json!("closed"),
            json!("pending")
        ]
    );

    // 遷移の追加・削除
    let got = call(
        &app,
        "POST",
        "/api/trouble/workflow/transitions",
        Some(t),
        Some(json!({"from_state_id": sid(0), "to_state_id": on_hold["id"], "label": "保留にする"})),
    )
    .await;
    assert_eq!(got.status, 201);
    let tr = got.json();
    assert_eq!(tr["label"], "保留にする");
    let got = call(
        &app,
        "POST",
        "/api/trouble/workflow/transitions",
        Some(t),
        Some(json!({"from_state_id": on_hold["id"], "to_state_id": sid(0)})),
    )
    .await;
    assert_eq!(
        (got.status, got.json()["label"].clone()),
        (201, Value::Null)
    );
    let path = format!("/api/trouble/workflow/transitions/{}", id_of(&tr));
    assert_eq!(call(&app, "DELETE", &path, Some(t), None).await.status, 204);
    assert_eq!(call(&app, "DELETE", &path, Some(t), None).await.status, 404);
    assert!(app.logs().is_empty());

    // 知らない状態への遷移・重複は DB のエラー → 500 (rust と同じ。ログは固定の語)
    for body in [
        json!({"from_state_id": sid(0), "to_state_id": Uuid::new_v4()}),
        json!({"from_state_id": sid(0), "to_state_id": sid(1)}),
    ] {
        let got = call(
            &app,
            "POST",
            "/api/trouble/workflow/transitions",
            Some(t),
            Some(body),
        )
        .await;
        assert_eq!((got.status, got.body.is_empty()), (500, true));
    }
    assert_eq!(
        app.logs(),
        vec![
            "trouble: workflow transition create failed: db (23503)",
            "trouble: workflow transition create failed: db (23505)",
        ]
    );

    // 状態の削除 (遷移は CASCADE で消える)
    let path = format!("/api/trouble/workflow/states/{}", id_of(&on_hold));
    assert_eq!(call(&app, "DELETE", &path, Some(t), None).await.status, 204);
    assert_eq!(call(&app, "DELETE", &path, Some(t), None).await.status, 404);
    let got = call(
        &app,
        "GET",
        "/api/trouble/workflow/transitions",
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.json().as_array().unwrap().len(), 7);
    assert_logs_clean(&app, &[t]);
}

/// 別 tenant のチケット・状態・遷移・履歴は見えず、触れない (RLS と WHERE の tenant_id)
#[tokio::test]
async fn rls_hides_other_tenant_rows() {
    let app = app().await;
    let a = tenant().await;
    let b = tenant().await;
    let states = setup(&app, a).await;
    let ticket = create(
        &app,
        a,
        json!({"category": "貨物事故", "description": "A の"}),
    )
    .await;
    let id = id_of(&ticket);

    assert_eq!(list(&app, a, "").await["total"], 1);
    let body = list(&app, b, "").await;
    assert_eq!(
        (body["total"].clone(), body["tickets"].clone()),
        (json!(0), json!([]))
    );
    let got = call(&app, "GET", "/api/trouble/tickets/csv", Some(b), None).await;
    assert_eq!(String::from_utf8_lossy(&got.body).lines().count(), 1);
    let path = format!("/api/trouble/tickets/{id}");
    assert_eq!(call(&app, "GET", &path, Some(b), None).await.status, 404);
    assert_eq!(
        call(&app, "PUT", &path, Some(b), Some(json!({"title": "x"})))
            .await
            .status,
        404
    );
    assert_eq!(call(&app, "DELETE", &path, Some(b), None).await.status, 404);
    let got = call(
        &app,
        "POST",
        &format!("{path}/transition"),
        Some(b),
        Some(json!({"to_state_id": states["in_progress"]})),
    )
    .await;
    assert_eq!(got.status, 404);
    let got = call(&app, "GET", &format!("{path}/history"), Some(b), None).await;
    assert_eq!(got.json(), json!([]));
    for p in [
        "/api/trouble/workflow/states",
        "/api/trouble/workflow/transitions",
    ] {
        assert_eq!(
            call(&app, "GET", p, Some(b), None).await.json(),
            json!([]),
            "{p}"
        );
    }
    let got = call(
        &app,
        "DELETE",
        &format!("/api/trouble/workflow/states/{}", states["new"]),
        Some(b),
        None,
    )
    .await;
    assert_eq!(got.status, 404);
    // B の setup は B の行を作り、A の状態は使えない
    let b_states = setup(&app, b).await;
    assert_ne!(b_states["new"], states["new"]);
    let other = create(&app, b, json!({"category": "貨物事故"})).await;
    let got = call(
        &app,
        "POST",
        &format!("/api/trouble/tickets/{}/transition", id_of(&other)),
        Some(b),
        Some(json!({"to_state_id": states["in_progress"]})),
    )
    .await;
    assert_eq!(got.status, 422);
    // A のチケットは手付かず
    let got = call(&app, "GET", &path, Some(a), None).await;
    assert_eq!(got.json(), ticket);
    assert!(app.logs().is_empty());
}

/// 履歴が書けなくても、チケットの作成・遷移は成功する (rust と同じ)。ログは固定の語
#[tokio::test]
async fn history_failure_does_not_fail_ticket() {
    let t = tenant().await;
    let setup_app = app().await;
    let states = setup(&setup_app, t).await;
    let app = build(client_as(NO_HISTORY_ROLE).await);
    let ticket = create(&app, t, json!({"category": "貨物事故"})).await;
    assert_eq!(ticket["status_id"], json!(states["new"]));
    let got = call(
        &app,
        "POST",
        &format!("/api/trouble/tickets/{}/transition", id_of(&ticket)),
        Some(t),
        Some(json!({"to_state_id": states["in_progress"]})),
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(
        app.logs(),
        vec!["trouble: ticket history failed: db (42501)"; 2]
    );
    assert_logs_clean(&app, &[t]);
}

/// 接続が切れた DB・書けないロール: どの口も 500 (本文は空)。ログは段の名前と kind だけ
#[tokio::test]
async fn db_failure_is_500_with_fixed_logs() {
    let t = tenant().await;
    let id = Uuid::new_v4();
    let cases: Vec<(&str, String, Option<Value>, &str)> = vec![
        // 分類のマスタが読めないのは既定の分類で続ける (rust と同じ) ので、初期の状態の段で落ちる
        (
            "POST",
            "/api/trouble/tickets".into(),
            Some(json!({"category": "その他"})),
            "ticket initial state",
        ),
        ("GET", "/api/trouble/tickets".into(), None, "ticket list"),
        ("GET", "/api/trouble/tickets/csv".into(), None, "ticket csv"),
        (
            "GET",
            format!("/api/trouble/tickets/{id}"),
            None,
            "ticket get",
        ),
        (
            "PUT",
            format!("/api/trouble/tickets/{id}"),
            Some(json!({})),
            "ticket update",
        ),
        (
            "DELETE",
            format!("/api/trouble/tickets/{id}"),
            None,
            "ticket delete",
        ),
        (
            "POST",
            format!("/api/trouble/tickets/{id}/transition"),
            Some(json!({"to_state_id": id})),
            "ticket get",
        ),
        (
            "GET",
            format!("/api/trouble/tickets/{id}/history"),
            None,
            "ticket history list",
        ),
        (
            "GET",
            "/api/trouble/workflow/states".into(),
            None,
            "workflow state list",
        ),
        (
            "POST",
            "/api/trouble/workflow/states".into(),
            Some(json!({"name": "x", "label": "x"})),
            "workflow state create",
        ),
        (
            "DELETE",
            format!("/api/trouble/workflow/states/{id}"),
            None,
            "workflow state delete",
        ),
        (
            "GET",
            "/api/trouble/workflow/transitions".into(),
            None,
            "workflow transition list",
        ),
        (
            "DELETE",
            format!("/api/trouble/workflow/transitions/{id}"),
            None,
            "workflow transition delete",
        ),
        (
            "POST",
            "/api/trouble/workflow/setup".into(),
            None,
            "workflow setup",
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

    // 読めるが書けないロール: 作成・setup・更新系は 42501
    let rw = app().await;
    let states = setup(&rw, t).await;
    let ticket = id_of(&create(&rw, t, json!({"category": "その他"})).await);
    let app = build(client_as(READ_ONLY_ROLE).await);
    let cases: Vec<(&str, String, Option<Value>, &str)> = vec![
        (
            "POST",
            "/api/trouble/tickets".into(),
            Some(json!({"category": "その他"})),
            "ticket create",
        ),
        (
            "POST",
            format!("/api/trouble/tickets/{ticket}/transition"),
            Some(json!({"to_state_id": states["in_progress"]})),
            "ticket status update",
        ),
        (
            "POST",
            "/api/trouble/workflow/setup".into(),
            Some(json!({})),
            "workflow setup",
        ),
    ];
    let other = tenant().await;
    for (method, path, body, stage) in cases {
        // setup はまだ状態の無い tenant で
        let who = if stage == "workflow setup" { other } else { t };
        let got = call(&app, method, &path, Some(who), body).await;
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
    assert_eq!(app.logs().len(), 3);
    assert_logs_clean(&app, &[t, other]);
}
