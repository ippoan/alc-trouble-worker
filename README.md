# alc-trouble-worker

トラブル管理 (障害チケット) の Cloudflare Worker `alc-trouble`。rust-alc-api の `crates/alc-trouble` から分割する
(Refs ippoan/rust-alc-api#747)。型は ippoan/alc-lineworks-worker と同じ。

## 口

発火の口 (`/api/internal/...`) を除き `X-Tenant-ID` が要る (auth-worker が付ける。無い・UUID でない → 401)。path・method・JSON の形・status は
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
| `/api/trouble/tickets` | GET / POST | チケットの一覧 (絞り込み・並び・ページ) と作成 (201) |
| `/api/trouble/tickets/csv` | GET | 一覧と同じ絞り込みで BOM 付きの CSV |
| `/api/trouble/tickets/{id}` | GET / PUT / DELETE | 取得・部分の更新・論理削除 |
| `/api/trouble/tickets/{id}/transition` | POST | 状態の遷移 (表に無い移り方は 422) |
| `/api/trouble/tickets/{id}/history` | GET | 状態の変更の履歴 |
| `/api/trouble/workflow/states[/{id}]` | GET / POST / DELETE | 状態 |
| `/api/trouble/workflow/transitions[/{id}]` | GET / POST / DELETE | 遷移 |
| `/api/trouble/workflow/setup` | POST | 状態が無ければ既定の 4 つと遷移 7 本を作る |
| `/api/trouble/tasks` | GET | タスクのチケット横断の一覧 (絞り込み 9 種・並び 5 種・ページ) |
| `/api/trouble/tickets/{id}/tasks` | GET / POST | チケットのタスクの一覧 (並び順) と作成 (201) |
| `/api/trouble/tickets/{id}/tasks/reorder` | PUT | `task_ids` の順に並び順を振り直す (そのチケットに無い id は 404) |
| `/api/trouble/tasks/{id}` | PUT / DELETE | タスクの部分の更新・削除 |
| `/api/trouble/tickets/{id}/files` | GET / POST | チケットの添付の一覧と、multipart の 1 つを R2 に置く (201) |
| `/api/trouble/tickets/{id}/files/trash` | GET | 論理削除した添付 |
| `/api/trouble/files/{id}` | DELETE | 添付の論理削除 (R2 の実体は残す) |
| `/api/trouble/files/{id}/download` | GET | 本体 (`Content-Type` は保存した値、`Content-Disposition` は ASCII だけ。下の注) |
| `/api/trouble/files/{id}/restore` | POST | 論理削除を戻す |
| `/api/trouble/tasks/{id}/files` | GET / POST | タスクの添付の一覧と作成 |
| `/api/trouble/task-files/{id}`[`/download`] | DELETE / GET | タスクの添付の論理削除・本体 |
| `/api/trouble/schedules` | POST | 予約通知の作成 (201。過去・30 日より先・空の本文・宛先なしは 400、チケットが無ければ 404)。alarm を登録する |
| `/api/trouble/tickets/{id}/schedules` | GET | チケットの予約通知 (予約の時刻の順) |
| `/api/trouble/schedules/{id}` | DELETE | 予約の取消 (204。pending でなければ 409)。alarm を取り消す |
| `/api/internal/trouble/schedules/{id}/fire` | POST | 予約の発火 (alarm の DO が auth-worker の alc-internal-proxy 経由で呼ぶ。`X-Tenant-ID` 不要) |
| `/api/trouble/lineworks/members` | GET | LINE WORKS の組織のメンバー (alc-lineworks-worker の中継) |

rust がチケットの作成・遷移で発火していた webhook は移さない (`webhook_configs` の CHECK でトラブルの設定が作れず、rust でも発火しない)。

タスクの作成 (`POST /api/trouble/tickets/{id}/tasks`) は、チケットがその tenant に無ければ (無い・別の tenant) 404 で作らない。**rust と違う** (rust は確かめず、無いチケットは外部キーの違反で 500、別の tenant のチケットには作れてしまう)。

添付は R2 の binding `TROUBLE_R2` (本番 `trouble-files`・staging `trouble-files-staging`、キーは rust と同じ) に置く。binding が無ければ添付の読み書きは 503。上限は rust と同じ 20MB (超えると 400)、拡張子の許可リストは rust にも無い。アップロードは親 (チケット / タスク) がその tenant に無ければ R2 に書かずに 404。
`Content-Disposition` は **rust と違う**: rust は日本語の名前を生の UTF-8 で返すが、ここは ASCII だけで `attachment; filename*=UTF-8''<percent-encoding>; filename="<非 ASCII・制御文字・"・\ を _ にした名前>"` (RFC 6266。Workers が非 ASCII のヘッダ値を値入りの警告にしてログに出すため)。

LINE WORKS への送信はすべて ippoan/alc-lineworks-worker の Service Binding `ALC_LINEWORKS` 経由 (`POST /api/internal/lineworks/messages`、tenant の既定の bot。Bot の秘密値はこの Worker に来ない)。チケットの作成・担当者の設定・状態の遷移、タスクの担当者の設定、予約の発火で、通知の設定 (`notification-prefs`) が有効な宛先へ送る (文面・条件は rust と同じ)。送信の失敗はログだけで、口の応答は成功のまま。
予約の alarm は ippoan/nuxt-notify の schedule-alarm (DO) を Service Binding `SCHEDULE_ALARM` で呼ぶ (`PUT`/`DELETE /alarms/{id}`、`X-Internal-Shared-Secret` は Secrets Store の `INTERNAL_SHARED_SECRET`)。binding か secret が無ければ登録しない (rust の `SCHEDULE_ALARM_URL` 未設定と同じ)。
発火は `get_trouble_schedule` (SECURITY DEFINER) が pending の行だけを返すので、送信済み・取消済み・無い id は 404 (rust と同じ)。
`GET /api/trouble/lineworks/members` の失敗は **rust と違う**: lineworks-worker が断った status と語 (`missing_scope` の `scope` など) を写し、届かない・binding 無しは 502 (rust は bot 無し 503・一覧の失敗 500 で本文なし)。

マスタ 4 本の `PUT /{id}` は `{"sort_order": n}` だけを変える。作成は 201、名前の重複は 409、空白だけの名前は 400。

## 構成

- 直下 (`src/`) — Worker 本体 (wasm 専用)。DB への経路 (`src/db.rs`: staging = Workers VPC → PgBouncer、
  本番 = Hyperdrive `TROUBLE_HYPERDRIVE`、ローカル = `DATABASE_URL`)・R2 の binding (`src/r2.rs`)・Service Binding
  (`src/service.rs`) と workers-rs への載せ方
- `crates/trouble/` — 口と SQL (native でもビルドでき、`tests/api_db.rs`・`tests/tickets_db.rs`・`tests/tasks_db.rs`・`tests/files_db.rs`・
  `tests/schedules_db.rs`・`tests/notify_db.rs` が実 DB で通す。R2・lineworks-worker・alarm の DO はモック)
- `scripts/` — 公開範囲の検査 (`check-exposure.sh` と陰性対照)、migration の取得、coverage の gate

デプロイ: PR → `wrangler deploy --dry-run`、main への merge → staging、タグ `v*` (Tag Release を手動) → 本番。
