//! 実 DB のテストの準備・接続 (ippoan/alc-lineworks-worker の tests/support の型)。
//!
//! - **`TROUBLE_TEST_ADMIN_DATABASE_URL` が未設定なら失敗する** (skip して緑にしない)。値は準備用の superuser の
//!   接続文字列で、**使い捨ての DB (database 名 `postgres`) に向けること** (ロール・schema・全 migration を作る)。表示はしない
//! - 準備の SQL は ippoan/alc-migrations から版を固定して取る (`scripts/ALC_MIGRATIONS_REV`、
//!   `bash scripts/fetch-migrations.sh` が repo 直下の `.alc-migrations/` に取り出す)。流す順は init_local_db.sql →
//!   空の `_sqlx_migrations` → migrations (1 ファイル 1 transaction) → local_app_grants.sql
//! - 本体のテストは、本番の worker と同じ実行用ロール `alc_api_rt` (migration 158。表の所有者ではない・`NOSUPERUSER`・
//!   `NOBYPASSRLS`) で繋ぐ ([`client_as`] が毎回確かめる)。パスワードは使い捨ての DB 専用の固定の文字列で、秘密ではない

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use alc_worker_db::PgClient;
use axum::body::Body;
use axum::http::{HeaderMap, Request};
use axum::Router;
use futures_util::future::BoxFuture;
use futures_util::lock::Mutex;
use tokio::sync::OnceCell;
use tokio_postgres::{Client, Config, NoTls};
use tower::ServiceExt;
use trouble::alarm::Alarm;
use trouble::files::{FileStore, SharedStore, StoreError};
use trouble::service::{Service, ServiceRequest, ServiceResponse, SharedService, Unreachable};
use trouble::{router, AppState};
use uuid::Uuid;

pub const ADMIN_URL_ENV: &str = "TROUBLE_TEST_ADMIN_DATABASE_URL";
pub const RT_ROLE: &str = "alc_api_rt";
/// trouble の表を読めるが書けないロール (auto-seed・チケットの作成などが失敗する経路を通すため)
pub const READ_ONLY_ROLE: &str = "trouble_test_read_only";
/// 状態の変更の履歴 (trouble_status_history) にだけ書けないロール (履歴が失敗してもチケットの口は成功する経路)
pub const NO_HISTORY_ROLE: &str = "trouble_test_no_history";
/// 使い捨ての DB 専用 (秘密ではない)
const PASSWORD: &str = "trouble-test-throwaway";
const SEARCH_PATH: &str = "-c search_path=alc_api,public";
/// 準備を直列にする advisory lock の鍵 (並列に走るテストの binary どうし)
const PREPARE_LOCK: i64 = 1_012_006;

/// 口の表 (読み取り専用のロールに SELECT だけ付ける)
pub const TABLES: &[&str] = &[
    "trouble_categories",
    "trouble_offices",
    "trouble_progress_statuses",
    "trouble_task_types",
    "trouble_task_statuses",
    "trouble_field_layouts",
    "trouble_notification_prefs",
    "trouble_tickets",
    "trouble_workflow_states",
    "trouble_workflow_transitions",
    "trouble_status_history",
    "trouble_tasks",
    "trouble_files",
    "trouble_schedules",
    // カメラ停止の自動チケットの口がカメラの在る・無しを引く
    "cameras",
];

/// テストの alarm の secret (モックが受け取る値。秘密ではない)
pub const ALARM_SECRET: &str = "alarm-test-secret";

fn migrations_dir() -> std::path::PathBuf {
    let dir = std::path::PathBuf::from(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../.alc-migrations"
    ));
    assert!(
        dir.join("migrations").is_dir(),
        ".alc-migrations が無い。先に `bash scripts/fetch-migrations.sh` を流す"
    );
    dir
}

pub fn admin_config() -> Config {
    let url = std::env::var(ADMIN_URL_ENV).unwrap_or_else(|_| {
        panic!("{ADMIN_URL_ENV} が未設定。実 DB のテストは接続先が無ければ失敗させる (skip しない)")
    });
    url.parse()
        .unwrap_or_else(|_| panic!("{ADMIN_URL_ENV} が接続文字列として読めない"))
}

async fn raw_connect(config: &Config) -> Client {
    let (client, connection) = config
        .connect(NoTls)
        .await
        .unwrap_or_else(|_| panic!("{ADMIN_URL_ENV} の DB に繋げない"));
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// superuser の接続 (準備と、テストの行を作る・見るのだけに使う)
pub async fn admin() -> Client {
    let mut config = admin_config();
    config.options(SEARCH_PATH);
    raw_connect(&config).await
}

/// 準備をプロセスの中で 1 回だけ流す。既に読み取り専用のロールが在れば (別の binary が流した) 何もしない
pub async fn prepare() {
    static PREPARED: OnceCell<()> = OnceCell::const_new();
    PREPARED
        .get_or_init(|| async {
            let su = admin().await;
            su.batch_execute(&format!("SELECT pg_advisory_lock({PREPARE_LOCK})"))
                .await
                .unwrap();
            let done = su
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)",
                    &[&READ_ONLY_ROLE],
                )
                .await
                .unwrap()
                .get::<_, bool>(0);
            if !done {
                migrate(&su).await;
            }
            su.batch_execute(&format!("SELECT pg_advisory_unlock({PREPARE_LOCK})"))
                .await
                .unwrap();
            eprintln!("[trouble-db] 実 DB で準備済み (migrations は {})", rev());
        })
        .await;
}

fn rev() -> String {
    std::fs::read_to_string(migrations_dir().join(".rev"))
        .unwrap()
        .trim()
        .to_owned()
}

async fn migrate(su: &Client) {
    let dir = migrations_dir();
    let read = |p: std::path::PathBuf| std::fs::read_to_string(p).unwrap();
    su.batch_execute(&read(dir.join("scripts/init_local_db.sql")))
        .await
        .unwrap();
    su.batch_execute("CREATE TABLE IF NOT EXISTS alc_api._sqlx_migrations (version BIGINT PRIMARY KEY, description TEXT NOT NULL, installed_on TIMESTAMPTZ NOT NULL DEFAULT now(), success BOOLEAN NOT NULL, checksum BYTEA NOT NULL, execution_time BIGINT NOT NULL)")
        .await
        .unwrap();
    let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(dir.join("migrations"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .collect();
    files.sort();
    assert!(
        files.iter().any(|f| f
            .file_name()
            .unwrap()
            .to_string_lossy()
            .ends_with("_runtime_role.sql")),
        "取り出した migrations に 158 (alc_api_rt) が無い (scripts/ALC_MIGRATIONS_REV が古い)"
    );
    for file in files {
        let name = file.file_name().unwrap().to_owned();
        if let Err(e) = su
            .batch_execute(&format!("BEGIN;\n{}\n;COMMIT;", read(file)))
            .await
        {
            panic!("migration {name:?}: {:?}", e.code());
        }
    }
    su.batch_execute(&read(dir.join("scripts/local_app_grants.sql")))
        .await
        .unwrap();
    // 実行用ロール (158 が NOLOGIN で作る) と、trouble の表を読むだけのロール
    let tables = TABLES
        .iter()
        .map(|t| format!("alc_api.{t}"))
        .collect::<Vec<_>>()
        .join(", ");
    su.batch_execute(&format!(
        "ALTER ROLE {RT_ROLE} LOGIN PASSWORD '{PASSWORD}';
         CREATE ROLE {READ_ONLY_ROLE} LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD '{PASSWORD}';
         GRANT USAGE ON SCHEMA alc_api TO {READ_ONLY_ROLE};
         GRANT SELECT ON {tables} TO {READ_ONLY_ROLE};
         CREATE ROLE {NO_HISTORY_ROLE} LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD '{PASSWORD}';
         GRANT USAGE ON SCHEMA alc_api TO {NO_HISTORY_ROLE};
         GRANT SELECT, INSERT, UPDATE, DELETE ON {tables} TO {NO_HISTORY_ROLE};
         REVOKE INSERT ON alc_api.trouble_status_history FROM {NO_HISTORY_ROLE};
         GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA alc_api TO {NO_HISTORY_ROLE};"
    ))
    .await
    .unwrap();
}

/// `role` の接続 (worker と同じく `PgClient` に包む)。**superuser / BYPASSRLS / 触る表の所有者なら panic**
pub async fn client_as(role: &str) -> PgClient {
    prepare().await;
    let mut config = admin_config();
    config.user(role).password(PASSWORD);
    let client = raw_connect(&config).await;
    let row = client
        .query_one(
            "SELECT (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user), \
             (SELECT coalesce(bool_or(tableowner = current_user), false) FROM pg_tables \
              WHERE schemaname = 'alc_api' AND tablename = ANY($1))",
            &[&TABLES],
        )
        .await
        .unwrap();
    assert!(!row.get::<_, bool>(0), "RLS を素通りするロールで繋いでいる");
    assert!(!row.get::<_, bool>(1), "表の所有者で繋いでいる");
    PgClient::new(client)
}

pub async fn rt_client() -> PgClient {
    client_as(RT_ROLE).await
}

/// 接続が切れた `alc_api_rt` の接続 (backend を落とした接続)
pub async fn broken_client() -> PgClient {
    let mut pg = rt_client().await;
    let pid: i32 = pg
        .tenant_tx(Uuid::nil(), |tx| {
            Box::pin(async move {
                let row = tx.query_typed_one("SELECT pg_backend_pid()", &[]).await?;
                Ok(row.get::<_, i32>(0))
            })
        })
        .await
        .unwrap();
    let terminated: bool = admin()
        .await
        .query_one("SELECT pg_terminate_backend($1)", &[&pid])
        .await
        .unwrap()
        .get(0);
    assert!(terminated);
    pg
}

/// tenant を 1 つ作る (superuser で。名前は乱数)。
/// migration 098 の seed は「その時点で在る tenant」だけに入るので、ここで作る tenant のタスクの状態は空
pub async fn tenant() -> Uuid {
    prepare().await;
    let id = Uuid::new_v4();
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.tenants (id, name) VALUES ($1, $2)",
            &[&id, &format!("trouble-test-{id}")],
        )
        .await
        .unwrap();
    id
}

pub struct App {
    pub router: Router,
    pub logs: Arc<StdMutex<Vec<String>>>,
    pub store: Arc<MockStore>,
    /// alc-lineworks-worker の代わり
    pub lineworks: Arc<MockService>,
    /// alarm の DO の代わり
    pub alarm: Arc<MockService>,
}

impl App {
    pub fn logs(&self) -> Vec<String> {
        self.logs.lock().unwrap().clone()
    }
}

/// R2 の代わり (キー → (本体, Content-Type))。`fail_put` / `fail_get` で失敗させる
#[derive(Default)]
pub struct MockStore {
    pub objects: StdMutex<HashMap<String, (Vec<u8>, String)>>,
    pub fail_put: AtomicBool,
    pub fail_get: AtomicBool,
}

impl MockStore {
    pub fn keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.objects.lock().unwrap().keys().cloned().collect();
        keys.sort();
        keys
    }

    pub fn object(&self, key: &str) -> Option<(Vec<u8>, String)> {
        self.objects.lock().unwrap().get(key).cloned()
    }
}

impl FileStore for MockStore {
    fn put<'a>(
        &'a self,
        key: &'a str,
        bytes: Vec<u8>,
        content_type: &'a str,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            if self.fail_put.load(Ordering::SeqCst) {
                return Err(StoreError("put"));
            }
            self.objects
                .lock()
                .unwrap()
                .insert(key.to_owned(), (bytes, content_type.to_owned()));
            Ok(())
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Vec<u8>, StoreError>> {
        Box::pin(async move {
            if self.fail_get.load(Ordering::SeqCst) {
                return Err(StoreError("get"));
            }
            self.object(key).map(|o| o.0).ok_or(StoreError("missing"))
        })
    }
}

/// Service Binding の代わり。受けたリクエストを残し、`status` と `body` で応える (`unreachable` なら届かない)
pub struct MockService {
    pub calls: StdMutex<Vec<ServiceRequest>>,
    pub status: AtomicU16,
    pub body: StdMutex<Vec<u8>>,
    pub unreachable: AtomicBool,
}

impl MockService {
    pub fn new(status: u16) -> Self {
        Self {
            calls: StdMutex::new(Vec::new()),
            status: AtomicU16::new(status),
            body: StdMutex::new(Vec::new()),
            unreachable: AtomicBool::new(false),
        }
    }

    pub fn calls(&self) -> Vec<ServiceRequest> {
        self.calls.lock().unwrap().clone()
    }

    pub fn respond(&self, status: u16, body: &[u8]) {
        self.status.store(status, Ordering::SeqCst);
        *self.body.lock().unwrap() = body.to_vec();
    }
}

impl Service for MockService {
    fn call(&self, req: ServiceRequest) -> BoxFuture<'_, Result<ServiceResponse, Unreachable>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(req);
            if self.unreachable.load(Ordering::SeqCst) {
                return Err(Unreachable("fetch"));
            }
            Ok(ServiceResponse {
                status: self.status.load(Ordering::SeqCst),
                body: self.body.lock().unwrap().clone(),
            })
        })
    }
}

/// 組み立ての有無 (worker の binding が在るか)
#[derive(Clone, Copy)]
pub struct Bindings {
    pub store: bool,
    pub lineworks: bool,
    pub alarm: bool,
}

pub const ALL: Bindings = Bindings {
    store: true,
    lineworks: true,
    alarm: true,
};

/// worker の src/lib.rs と同じ組み立て (R2・Service Binding はモック)
pub fn build(pg: PgClient) -> App {
    build_bindings(pg, ALL)
}

/// `with_store` が false なら R2 の binding が無い worker (添付の口は 503)
pub fn build_with(pg: PgClient, with_store: bool) -> App {
    build_bindings(
        pg,
        Bindings {
            store: with_store,
            ..ALL
        },
    )
}

pub fn build_bindings(pg: PgClient, b: Bindings) -> App {
    let logs = Arc::new(StdMutex::new(Vec::new()));
    let sink = logs.clone();
    let store = Arc::new(MockStore::default());
    // 送信の口は 204、alarm は 200 (lineworks-worker・DO の成功の応答)
    let lineworks = Arc::new(MockService::new(204));
    let alarm = Arc::new(MockService::new(200));
    let state = AppState {
        pg: Arc::new(Mutex::new(pg)),
        log: Arc::new(move |m: &str| sink.lock().unwrap().push(m.to_owned())),
        files: b.store.then(|| store.clone() as SharedStore),
        lineworks: b.lineworks.then(|| lineworks.clone() as SharedService),
        alarm: b.alarm.then(|| Alarm {
            service: alarm.clone() as SharedService,
            secret: Arc::from(ALARM_SECRET),
        }),
    };
    App {
        router: router().with_state(state),
        logs,
        store,
        lineworks,
        alarm,
    }
}

/// カメラを 1 台入れる (superuser で)
pub async fn camera(tenant: Uuid) -> Uuid {
    let id = Uuid::new_v4();
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.cameras (id, tenant_id, name) VALUES ($1, $2, 'cam')",
            &[&id, &tenant],
        )
        .await
        .unwrap();
    id
}

/// 通知の設定を 1 行入れる (superuser で)
pub async fn pref(tenant: Uuid, event_type: &str, enabled: bool, to: &[&str]) {
    let to: Vec<String> = to.iter().map(|s| s.to_string()).collect();
    admin()
        .await
        .execute(
            "INSERT INTO alc_api.trouble_notification_prefs \
             (tenant_id, event_type, notify_channel, enabled, lineworks_user_ids) \
             VALUES ($1, $2, 'lineworks', $3, $4)",
            &[&tenant, &event_type, &enabled, &to],
        )
        .await
        .unwrap();
}

/// lineworks-worker へ送った (宛先の user_id, 本文) の並び。各リクエストの形 (path・method・X-Tenant-ID・body のキー) も確かめる
pub fn sent(app: &App, tenant: Uuid) -> Vec<(String, String)> {
    app.lineworks
        .calls()
        .into_iter()
        .map(|req| {
            assert_eq!(req.method, trouble::service::Method::Post);
            assert_eq!(req.path, "/api/internal/lineworks/messages");
            assert!(req.headers.contains(&("X-Tenant-ID", tenant.to_string())));
            let v: serde_json::Value = serde_json::from_str(req.body.as_deref().unwrap()).unwrap();
            let obj = v.as_object().unwrap();
            // bot_config_id は渡さない (tenant の既定の bot)
            assert_eq!(obj.keys().collect::<Vec<_>>(), ["content", "to"]);
            assert_eq!(v["content"]["type"], "text");
            (
                v["to"]["user_id"].as_str().unwrap().to_owned(),
                v["content"]["text"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

pub async fn app() -> App {
    build(rt_client().await)
}

pub struct Got {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

impl Got {
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap()
    }
}

/// 1 リクエストを打つ。`tenant` が在れば `X-Tenant-ID` を付ける。`body` が在れば JSON で送る
pub async fn call(
    app: &App,
    method: &str,
    uri: &str,
    tenant: Option<Uuid>,
    body: Option<serde_json::Value>,
) -> Got {
    match body {
        Some(v) => {
            let bytes = serde_json::to_vec(&v).unwrap();
            call_raw(app, method, uri, tenant, Some("application/json"), bytes).await
        }
        None => call_raw(app, method, uri, tenant, None, Vec::new()).await,
    }
}

/// 本文と Content-Type をそのまま送る
pub async fn call_raw(
    app: &App,
    method: &str,
    uri: &str,
    tenant: Option<Uuid>,
    content_type: Option<&str>,
    body: Vec<u8>,
) -> Got {
    let mut b = Request::builder().method(method).uri(uri);
    if let Some(t) = tenant {
        b = b.header("X-Tenant-ID", t.to_string());
    }
    if let Some(ct) = content_type {
        b = b.header("content-type", ct);
    }
    let resp = app
        .router
        .clone()
        .oneshot(b.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let headers = resp.headers().clone();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    Got {
        status,
        headers,
        body,
    }
}
