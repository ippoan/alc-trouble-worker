# alc-trouble-worker

トラブル管理の Worker `alc-trouble` (Refs ippoan/rust-alc-api#747)。rust-alc-api の `crates/alc-trouble` を移す先で、
path・method・JSON の形・status は rust と同じ (nuxt-trouble がそのまま使う)。いまの口は `/api/trouble/` の下の
マスタ系: `categories`・`offices`・`progress-statuses`・`task-types` (`crates/trouble/src/master.rs`)・
`task-statuses`・`field-layout`・`notification-prefs` と、`tickets` (`tickets.rs`)・`workflow` と `tickets/{id}/history`
(`workflow.rs`)・`tasks` と `tickets/{id}/tasks` (`tasks.rs`。ファイルの口を除く)。files (タスクのファイルの口を含む) /
schedules / members / LINE WORKS の通知は後続の PR (通知の呼び位置は `TODO(#747 T5)`)。
直下 = Worker (wasm 専用)、`crates/trouble/` = 口・SQL。

rust の trouble_* webhook (`trouble_created` / `trouble_status_changed`) は移さない (`webhook_configs.event_type` の CHECK で設定できず、rust でも発火しない死んだ経路)。

**呼び手は auth-worker の Service Binding だけ** (auth-worker が JWT を検証し `X-Tenant-ID` を付け直す。この Worker は
認証しない)。auth-worker の振り分け (`ALC_BINDING_ROUTES`) は別の PR。

## コマンド
```bash
bash scripts/check-exposure.sh && bash scripts/check-exposure-test.sh
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --target wasm32-unknown-unknown --release -- -D warnings
bash scripts/fetch-migrations.sh   # 版は scripts/ALC_MIGRATIONS_REV
TROUBLE_TEST_ADMIN_DATABASE_URL=postgresql://postgres:<pw>@127.0.0.1:<port>/postgres cargo test --workspace
worker-build --release && npx wrangler@4.144.0 deploy --dry-run
```
DB テストは使い捨ての postgres:17 へ。URL が無ければ失敗 (skip しない)。接続ロールは `alc_api_rt`。

## 規範
- 公開範囲を弱めない: 本番・staging とも Service Binding だけ (どの env も `workers_dev`/`preview_urls` = false、route 無し)。
  Hyperdrive (`TROUBLE_HYPERDRIVE`) は本番だけ。`wrangler.toml` の表の順を変えない (新しい表は placement より後)。
- **ログ・エラー応答は固定の語だけ。** エラーの文 (`{e}`)・tenant_id などの識別子・行の中身を出さない。
  DB の失敗は `trouble: <段> failed: db (<kind>)` (kind は SQLSTATE か固定の label)。応答の本文は rust と同じく空。
- role は見ない (rust も `X-Tenant-ID` だけ)。
- DB は `PgClient::tenant_tx` + `query_typed` 系だけ。名前付き prepared statement 禁止。Row を tx の外へ出さない。
  WHERE に tenant_id も明示する。SQL の意味は rust の `crates/alc-trouble/src/repo/*.rs` と同じにする。
- マスタの表の名前は `MasterTable::TABLE` のリテラルだけ (SQL に埋め込むので、実行時の文字列を入れない)。
- `coverage_100.toml` の登録を外さない。CI の本数 (lib 12・api_db 9・tickets_db 11・tasks_db 7) はテストを足したら上げる。
- public repo: ホスト名・tenant UUID・個人情報を書かない。`v*` タグ = 本番 (Tag Release 手動)。
- kit の rev は直下 `Cargo.toml` の 1 か所 (alc-lineworks-worker と同じ rev)。
