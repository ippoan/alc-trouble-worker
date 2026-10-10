//! 添付の口を、実 DB (`alc_api_rt` で接続) と R2 のモックで通す。期待値は rust-alc-api の
//! tests/trouble_test.rs の `test_trouble_file_metadata_crud`・`test_trouble_task_file_crud` に合わせる。
//! 接続先は `TROUBLE_TEST_ADMIN_DATABASE_URL` (無ければ失敗する。tests/support)。

mod support;

use std::sync::atomic::Ordering;

use axum::http::header;
use serde_json::{json, Value};
use support::*;
use uuid::Uuid;

const FILE_KEYS: &[&str] = &[
    "content_type",
    "created_at",
    "deleted_at",
    "filename",
    "id",
    "size_bytes",
    "storage_key",
    "task_id",
    "tenant_id",
    "ticket_id",
];

const BOUNDARY: &str = "trouble-test-boundary";

fn multipart_type() -> String {
    format!("multipart/form-data; boundary={BOUNDARY}")
}

/// `file` の 1 つだけの multipart の本文。名前・Content-Type は省ける
fn multipart(filename: Option<&str>, content_type: Option<&str>, data: &[u8]) -> Vec<u8> {
    let mut head = format!("--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"file\"");
    if let Some(f) = filename {
        head.push_str(&format!("; filename=\"{f}\""));
    }
    head.push_str("\r\n");
    if let Some(ct) = content_type {
        head.push_str(&format!("Content-Type: {ct}\r\n"));
    }
    head.push_str("\r\n");
    let mut body = head.into_bytes();
    body.extend_from_slice(data);
    body.extend_from_slice(format!("\r\n--{BOUNDARY}--\r\n").as_bytes());
    body
}

async fn upload(app: &App, t: Uuid, uri: &str, body: Vec<u8>) -> Got {
    call_raw(app, "POST", uri, Some(t), Some(&multipart_type()), body).await
}

async fn status(app: &App, method: &str, uri: &str, t: Uuid) -> u16 {
    call(app, method, uri, Some(t), None).await.status
}

fn id_of(v: &Value) -> String {
    v["id"].as_str().unwrap().to_owned()
}

fn len_of(got: &Got) -> usize {
    assert_eq!(got.status, 200);
    got.json().as_array().unwrap().len()
}

/// ログに識別子・名前が出ていないこと (固定の語と kind だけ)
fn assert_logs_clean(app: &App, secrets: &[String]) {
    for line in app.logs() {
        assert!(line.starts_with("trouble: "), "固定の頭で始まらないログ");
        for s in secrets {
            assert!(!line.contains(s.as_str()), "ログに識別子・名前が出た");
        }
    }
}

async fn ticket(app: &App, t: Uuid) -> String {
    let got = call(
        app,
        "POST",
        "/api/trouble/tickets",
        Some(t),
        Some(json!({"category": "その他", "person_name": "ファイルテスト"})),
    )
    .await;
    assert_eq!(got.status, 201);
    id_of(&got.json())
}

async fn task(app: &App, t: Uuid, ticket: &str) -> String {
    let got = call(
        app,
        "POST",
        &format!("/api/trouble/tickets/{ticket}/tasks"),
        Some(t),
        Some(json!({"title": "修理手配", "task_type": "修理手配"})),
    )
    .await;
    assert_eq!(got.status, 201);
    id_of(&got.json())
}

async fn file_rows(t: Uuid) -> i64 {
    admin()
        .await
        .query_one(
            "SELECT count(*) FROM alc_api.trouble_files WHERE tenant_id = $1",
            &[&t],
        )
        .await
        .unwrap()
        .get(0)
}

#[tokio::test]
async fn ticket_file_upload_list_download_delete_trash_restore() {
    let app = app().await;
    let t = tenant().await;
    let tk = ticket(&app, t).await;
    let files = format!("/api/trouble/tickets/{tk}/files");
    let trash = format!("{files}/trash");
    assert_eq!(len_of(&call(&app, "GET", &files, Some(t), None).await), 0);

    let got = upload(
        &app,
        t,
        &files,
        multipart(
            Some("報告書 最終.v2.pdf"),
            Some("application/pdf"),
            b"hello world",
        ),
    )
    .await;
    assert_eq!(got.status, 201, "{}", String::from_utf8_lossy(&got.body));
    let file = got.json();
    let mut keys: Vec<&str> = file
        .as_object()
        .unwrap()
        .keys()
        .map(|k| k.as_str())
        .collect();
    keys.sort();
    assert_eq!(keys, FILE_KEYS);
    let id = id_of(&file);
    assert_eq!(file["filename"], "報告書 最終.v2.pdf");
    assert_eq!(file["content_type"], "application/pdf");
    assert_eq!(file["size_bytes"], 11);
    assert_eq!(file["ticket_id"], tk.as_str());
    assert_eq!(file["tenant_id"], t.to_string());
    assert!(file["task_id"].is_null() && file["deleted_at"].is_null());
    // R2 のキーは rust と同じ `{tenant}/trouble/{ticket}/{uuid}.{ext}`
    let key = file["storage_key"].as_str().unwrap().to_owned();
    let prefix = format!("{t}/trouble/{tk}/");
    let name = key.strip_prefix(&prefix).expect("rust と同じキーの頭");
    let (uuid, ext) = name.split_once('.').unwrap();
    assert!(Uuid::parse_str(uuid).is_ok());
    assert_eq!(ext, "pdf");
    assert_eq!(app.store.keys(), vec![key.clone()]);
    assert_eq!(
        app.store.object(&key).unwrap(),
        (b"hello world".to_vec(), "application/pdf".to_owned())
    );

    let list = call(&app, "GET", &files, Some(t), None).await;
    assert_eq!(len_of(&list), 1);
    assert_eq!(list.json()[0], file);

    // 本体と、rust と同じヘッダ (名前は UTF-8 のまま、`"` だけ `_`)
    let download = format!("/api/trouble/files/{id}/download");
    let got = call(&app, "GET", &download, Some(t), None).await;
    assert_eq!(got.status, 200);
    assert_eq!(got.body, b"hello world");
    assert_eq!(got.headers[header::CONTENT_TYPE], "application/pdf");
    // ASCII だけ (rust と違う): filename* に UTF-8 の percent-encoding、filename に ASCII の代替
    assert_eq!(
        got.headers[header::CONTENT_DISPOSITION],
        "attachment; filename*=UTF-8''%E5%A0%B1%E5%91%8A%E6%9B%B8%20%E6%9C%80%E7%B5%82.v2.pdf; \
         filename=\"___ __.v2.pdf\""
    );
    // `"`・`\` 入りの名前 (multipart では送れないので行を書き換える)
    admin()
        .await
        .execute(
            "UPDATE alc_api.trouble_files SET filename = $1 WHERE id = $2::text::uuid",
            &[&"見積 \"改\\訂\".pdf", &id],
        )
        .await
        .unwrap();
    let got = call(&app, "GET", &download, Some(t), None).await;
    assert_eq!(
        got.headers[header::CONTENT_DISPOSITION],
        "attachment; filename*=UTF-8''%E8%A6%8B%E7%A9%8D%20%22%E6%94%B9%5C%E8%A8%82%22.pdf; \
         filename=\"__ _____.pdf\""
    );

    // 論理削除: 一覧から消えて trash に出る。R2 の実体は残る
    let delete = format!("/api/trouble/files/{id}");
    assert_eq!(status(&app, "DELETE", &delete, t).await, 204);
    assert_eq!(status(&app, "DELETE", &delete, t).await, 404);
    assert_eq!(len_of(&call(&app, "GET", &files, Some(t), None).await), 0);
    let got = call(&app, "GET", &trash, Some(t), None).await;
    assert_eq!(len_of(&got), 1);
    assert!(got.json()[0]["deleted_at"].as_str().is_some());
    assert_eq!(app.store.keys(), vec![key.clone()]);
    // 論理削除したものも落とせる (rust の get は deleted_at を見ない)
    assert_eq!(status(&app, "GET", &download, t).await, 200);

    let restore = format!("/api/trouble/files/{id}/restore");
    assert_eq!(status(&app, "POST", &restore, t).await, 204);
    assert_eq!(status(&app, "POST", &restore, t).await, 404);
    assert_eq!(len_of(&call(&app, "GET", &files, Some(t), None).await), 1);
    assert_eq!(len_of(&call(&app, "GET", &trash, Some(t), None).await), 0);

    // 名前・Content-Type の無い part は rust と同じく "unknown" / application/octet-stream
    let got = upload(&app, t, &files, multipart(None, None, b"x")).await;
    assert_eq!(got.status, 201);
    let file = got.json();
    assert_eq!(file["filename"], "unknown");
    assert_eq!(file["content_type"], "application/octet-stream");
    assert!(file["storage_key"].as_str().unwrap().ends_with(".unknown"));
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn task_file_upload_list_download_delete() {
    let app = app().await;
    let t = tenant().await;
    let tk = ticket(&app, t).await;
    let ts = task(&app, t, &tk).await;
    let files = format!("/api/trouble/tasks/{ts}/files");
    assert_eq!(len_of(&call(&app, "GET", &files, Some(t), None).await), 0);

    let got = upload(
        &app,
        t,
        &files,
        multipart(
            Some("report.pdf"),
            Some("application/pdf"),
            b"task file data",
        ),
    )
    .await;
    assert_eq!(got.status, 201);
    let file = got.json();
    let id = id_of(&file);
    assert_eq!(file["filename"], "report.pdf");
    assert_eq!(file["task_id"], ts.as_str());
    assert_eq!(file["ticket_id"], tk.as_str());
    let key = file["storage_key"].as_str().unwrap().to_owned();
    assert!(key.starts_with(&format!("{t}/trouble/tasks/{ts}/")) && key.ends_with(".pdf"));
    assert_eq!(app.store.keys(), vec![key]);

    assert_eq!(len_of(&call(&app, "GET", &files, Some(t), None).await), 1);
    // チケットの一覧・trash にはタスクの添付を出さない (rust の task_id IS NULL)
    let ticket_files = format!("/api/trouble/tickets/{tk}/files");
    assert_eq!(
        len_of(&call(&app, "GET", &ticket_files, Some(t), None).await),
        0
    );

    let got = call(
        &app,
        "GET",
        &format!("/api/trouble/task-files/{id}/download"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 200);
    assert_eq!(got.body, b"task file data");
    assert_eq!(
        got.headers[header::CONTENT_DISPOSITION],
        "attachment; filename*=UTF-8''report.pdf; filename=\"report.pdf\""
    );

    let delete = format!("/api/trouble/task-files/{id}");
    assert_eq!(status(&app, "DELETE", &delete, t).await, 204);
    assert_eq!(status(&app, "DELETE", &delete, t).await, 404);
    assert_eq!(len_of(&call(&app, "GET", &files, Some(t), None).await), 0);
    assert_eq!(
        len_of(&call(&app, "GET", &format!("{ticket_files}/trash"), Some(t), None).await),
        0
    );
    assert!(app.logs().is_empty());
}

/// 別の tenant の親へは置けない (R2 にも DB にも残らない)。別の tenant のファイルは見えない・変えられない
#[tokio::test]
async fn other_tenant_parent_and_files_are_404() {
    let app = app().await;
    let a = tenant().await;
    let b = tenant().await;
    let tk = ticket(&app, a).await;
    let ts = task(&app, a, &tk).await;
    let got = upload(
        &app,
        a,
        &format!("/api/trouble/tickets/{tk}/files"),
        multipart(Some("a.txt"), Some("text/plain"), b"a"),
    )
    .await;
    assert_eq!(got.status, 201);
    let id = id_of(&got.json());
    let keys = app.store.keys();

    for uri in [
        format!("/api/trouble/tickets/{tk}/files"),
        format!("/api/trouble/tasks/{ts}/files"),
    ] {
        let got = upload(&app, b, &uri, multipart(Some("b.txt"), None, b"b")).await;
        assert_eq!((got.status, got.body.len()), (404, 0), "{uri}");
    }
    assert_eq!(app.store.keys(), keys, "R2 に書いていない");
    assert_eq!(file_rows(b).await, 0, "DB に残っていない");

    for (method, uri) in [
        ("GET", format!("/api/trouble/files/{id}/download")),
        ("GET", format!("/api/trouble/task-files/{id}/download")),
        ("POST", format!("/api/trouble/files/{id}/restore")),
        ("DELETE", format!("/api/trouble/files/{id}")),
        ("DELETE", format!("/api/trouble/task-files/{id}")),
    ] {
        let got = call(&app, method, &uri, Some(b), None).await;
        assert_eq!((got.status, got.body.len()), (404, 0), "{method} {uri}");
    }
    assert_eq!(
        len_of(
            &call(
                &app,
                "GET",
                &format!("/api/trouble/tickets/{tk}/files"),
                Some(b),
                None
            )
            .await
        ),
        0
    );
    // a のファイルはそのまま
    assert_eq!(
        len_of(
            &call(
                &app,
                "GET",
                &format!("/api/trouble/tickets/{tk}/files"),
                Some(a),
                None
            )
            .await
        ),
        1
    );
    // X-Tenant-ID が無ければ 401
    let got = call_raw(
        &app,
        "POST",
        &format!("/api/trouble/tickets/{tk}/files"),
        None,
        Some(&multipart_type()),
        multipart(Some("c.txt"), None, b"c"),
    )
    .await;
    assert_eq!(got.status, 401);
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn missing_or_deleted_parent_is_404() {
    let app = app().await;
    let t = tenant().await;
    let body = || multipart(Some("x.txt"), None, b"x");
    for uri in [
        format!("/api/trouble/tickets/{}/files", Uuid::new_v4()),
        format!("/api/trouble/tasks/{}/files", Uuid::new_v4()),
    ] {
        assert_eq!(upload(&app, t, &uri, body()).await.status, 404);
    }
    // 論理削除したチケット (rust の tickets.get は deleted_at IS NULL)
    let tk = ticket(&app, t).await;
    let del = call(
        &app,
        "DELETE",
        &format!("/api/trouble/tickets/{tk}"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(del.status, 204);
    let got = upload(&app, t, &format!("/api/trouble/tickets/{tk}/files"), body()).await;
    assert_eq!(got.status, 404);
    let unknown = Uuid::new_v4();
    for (method, uri) in [
        ("GET", format!("/api/trouble/files/{unknown}/download")),
        ("POST", format!("/api/trouble/files/{unknown}/restore")),
        ("DELETE", format!("/api/trouble/files/{unknown}")),
    ] {
        assert_eq!(call(&app, method, &uri, Some(t), None).await.status, 404);
    }
    assert!(app.store.keys().is_empty());
}

/// 読めない multipart・空・20MB 超は 400 で、R2 にも DB にも残らない
#[tokio::test]
async fn bad_or_too_large_multipart_is_400() {
    let app = app().await;
    let t = tenant().await;
    let tk = ticket(&app, t).await;
    let uri = format!("/api/trouble/tickets/{tk}/files");

    // multipart でない (axum の Multipart の拒否)
    let got = call_raw(
        &app,
        "POST",
        &uri,
        Some(t),
        Some("text/plain"),
        b"x".to_vec(),
    )
    .await;
    assert_eq!(got.status, 400);
    // 区切りの壊れた本文 (next_field の失敗)
    let got = upload(&app, t, &uri, b"--nope\r\n\r\n".to_vec()).await;
    assert_eq!(got.status, 400);
    // part が無い
    let got = upload(&app, t, &uri, format!("--{BOUNDARY}--\r\n").into_bytes()).await;
    assert_eq!(got.status, 400);
    // 20MB を超える (rust の DefaultBodyLimit と同じ。本体の読みの失敗)
    let big = vec![b'a'; trouble::BODY_LIMIT + 1];
    let got = upload(&app, t, &uri, multipart(Some("big.bin"), None, &big)).await;
    assert_eq!(got.status, 400);
    // ちょうど上限に収まるものは通る (multipart の枠の分を引く)
    let fits = vec![b'a'; trouble::BODY_LIMIT - 1024];
    let got = upload(&app, t, &uri, multipart(Some("fits.bin"), None, &fits)).await;
    assert_eq!(got.status, 201);

    assert_eq!(app.store.keys().len(), 1);
    assert_eq!(file_rows(t).await, 1);
    assert!(app.logs().is_empty());
}

/// R2 の binding が無い worker: 添付の読み書きは 503 (rust の storage 無し)。一覧は DB だけなので動く
#[tokio::test]
async fn no_r2_binding_is_503() {
    let app = build_with(rt_client().await, false);
    let t = tenant().await;
    let tk = ticket(&app, t).await;
    let uri = format!("/api/trouble/tickets/{tk}/files");
    let got = upload(&app, t, &uri, multipart(Some("x.txt"), None, b"x")).await;
    assert_eq!(got.status, 503);
    assert_eq!(len_of(&call(&app, "GET", &uri, Some(t), None).await), 0);

    // 行だけ在るファイル (R2 の binding が在る worker で作ったもの)
    let with = support::app().await;
    let got = upload(&with, t, &uri, multipart(Some("x.txt"), None, b"x")).await;
    assert_eq!(got.status, 201);
    let id = id_of(&got.json());
    let got = call(
        &app,
        "GET",
        &format!("/api/trouble/files/{id}/download"),
        Some(t),
        None,
    )
    .await;
    assert_eq!(got.status, 503);
    assert!(app.logs().is_empty());
}

#[tokio::test]
async fn r2_failures_are_500_with_fixed_words() {
    let app = app().await;
    let t = tenant().await;
    let tk = ticket(&app, t).await;
    let uri = format!("/api/trouble/tickets/{tk}/files");

    app.store.fail_put.store(true, Ordering::SeqCst);
    let got = upload(&app, t, &uri, multipart(Some("秘密の名前.txt"), None, b"x")).await;
    assert_eq!((got.status, got.body.len()), (500, 0));
    assert_eq!(file_rows(t).await, 0, "R2 に置けなければ行を作らない");
    app.store.fail_put.store(false, Ordering::SeqCst);

    let got = upload(&app, t, &uri, multipart(Some("秘密の名前.txt"), None, b"x")).await;
    assert_eq!(got.status, 201);
    let file = got.json();
    let id = id_of(&file);
    let download = format!("/api/trouble/files/{id}/download");
    app.store.fail_get.store(true, Ordering::SeqCst);
    let got = call(&app, "GET", &download, Some(t), None).await;
    assert_eq!((got.status, got.body.len()), (500, 0));
    app.store.fail_get.store(false, Ordering::SeqCst);
    // R2 に実体が無い (rust の download の失敗と同じ 500)
    app.store.objects.lock().unwrap().clear();
    assert_eq!(status(&app, "GET", &download, t).await, 500);

    assert_eq!(
        app.logs(),
        vec![
            "trouble: file upload failed: r2 (put)",
            "trouble: file download failed: r2 (get)",
            "trouble: file download failed: r2 (missing)",
        ]
    );
    assert_logs_clean(
        &app,
        &[
            t.to_string(),
            tk.clone(),
            id,
            "秘密の名前".to_owned(),
            file["storage_key"].as_str().unwrap().to_owned(),
        ],
    );
}

#[tokio::test]
async fn db_failures_are_500_with_fixed_words() {
    let t = tenant().await;
    let id = Uuid::new_v4();
    {
        let app = build(broken_client().await);
        for (method, uri) in [
            ("GET", format!("/api/trouble/tickets/{id}/files")),
            ("GET", format!("/api/trouble/files/{id}/download")),
            ("DELETE", format!("/api/trouble/files/{id}")),
            ("POST", format!("/api/trouble/files/{id}/restore")),
        ] {
            let got = call(&app, method, &uri, Some(t), None).await;
            assert_eq!((got.status, got.body.len()), (500, 0), "{method} {uri}");
        }
        let got = upload(
            &app,
            t,
            &format!("/api/trouble/tickets/{id}/files"),
            multipart(Some("x.txt"), None, b"x"),
        )
        .await;
        assert_eq!(got.status, 500);
        let logs = app.logs();
        assert_eq!(logs.len(), 5);
        for (line, stage) in logs.iter().zip([
            "file list",
            "file download",
            "file delete",
            "file restore",
            "file upload parent",
        ]) {
            assert!(
                line.starts_with(&format!("trouble: {stage} failed: db (")),
                "{line}"
            );
        }
        assert_logs_clean(&app, &[t.to_string(), id.to_string()]);
    }

    // 行を作れないロール: 親の確かめと R2 は通り、行の作成で 500 (rust と同じく R2 の実体は残る)
    let tk = ticket(&app().await, t).await;
    let app = build(client_as(READ_ONLY_ROLE).await);
    let got = upload(
        &app,
        t,
        &format!("/api/trouble/tickets/{tk}/files"),
        multipart(Some("x.txt"), None, b"x"),
    )
    .await;
    assert_eq!((got.status, got.body.len()), (500, 0));
    assert_eq!(app.store.keys().len(), 1);
    assert_eq!(app.logs(), vec!["trouble: file create failed: db (42501)"]);
    assert_eq!(file_rows(t).await, 0);
}
