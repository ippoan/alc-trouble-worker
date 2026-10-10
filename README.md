# alc-trouble-worker

トラブル管理 (障害チケット) の Cloudflare Worker `alc-trouble`。rust-alc-api の `crates/alc-trouble` から分割する
(Refs ippoan/rust-alc-api#747)。型は ippoan/alc-lineworks-worker と同じ。

## 口

すべて `X-Tenant-ID` が要る (auth-worker が付ける。無い・UUID でない → 401)。path・method・JSON の形・status は
rust-alc-api と同じ。

| path | method | 内容 |
|---|---|---|
| `/api/trouble/categories[/{id}]` | GET / POST / PUT / DELETE | 分類。1 行も無ければ既定の 7 行を作って返す |
| `/api/trouble/offices[/{id}]` | GET / POST / PUT / DELETE | 営業所 |
| `/api/trouble/progress-statuses[/{id}]` | GET / POST / PUT / DELETE | 進捗 |
| `/api/trouble/task-types[/{id}]` | GET / POST / PUT / DELETE | タスクの種類。1 行も無ければ既定の 8 行を作って返す |
| `/api/trouble/task-statuses[/{id}]` | GET / POST / PUT / DELETE | タスクの状態。1 行も無ければ既定の 4 行を作って返す |
| `/api/trouble/field-layout` | GET / PUT | 入力欄の表示設定 |
| `/api/trouble/notification-prefs[/{id}]` | GET / POST / DELETE | 通知の設定 |

マスタ 4 本の `PUT /{id}` は `{"sort_order": n}` だけを変える。作成は 201、名前の重複は 409、空白だけの名前は 400。

## 構成

- 直下 (`src/`) — Worker 本体 (wasm 専用)。DB への経路 (`src/db.rs`: staging = Workers VPC → PgBouncer、
  本番 = Hyperdrive `TROUBLE_HYPERDRIVE`、ローカル = `DATABASE_URL`) と workers-rs への載せ方
- `crates/trouble/` — 口と SQL (native でもビルドでき、`tests/api_db.rs` が実 DB で通す)
- `scripts/` — 公開範囲の検査 (`check-exposure.sh` と陰性対照)、migration の取得、coverage の gate

デプロイ: PR → `wrangler deploy --dry-run`、main への merge → staging、タグ `v*` (Tag Release を手動) → 本番。
