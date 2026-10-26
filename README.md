# as — anthropic to stream

`as-shim` 是跑在 Claude Code 那台機器上的本地小代理（127.0.0.1）。它讓 Claude Code 對 Anthropic 相容閘道（例如架在 Cloudflare 後面的 sub2api）的請求**一律走串流**，Cloudflare 因此不會因為 125 秒沒有資料而回 **524**。

如果上游是**實作了 [as resume protocol v1](docs/RESUME.md) 的伺服器**，串流中途斷線時 as-shim 還會從斷掉的那個 byte 接回去（斷線續傳），Claude Code 不會看到 "Connection lost"。每個請求也會記到本機的 SQLite 檔，方便事後查詢。

> 指令叫 `as-shim`，不叫 `as`，因為 `/usr/bin/as` 是 GNU assembler。

## 一行安裝

```bash
curl -fsSL https://raw.githubusercontent.com/123hi123/as/main/install.sh | bash
```

裝好後會馬上問 shim 要指向哪個上游，接著一路裝完。

- 安裝位置：`~/.local/bin/as-shim`，不需要 sudo。
- 支援平台：Linux x86_64、aarch64（靜態 musl binary）。
- 下載後會用 `SHA256SUMS` 驗證。

可用環境變數：

| 變數 | 用途 |
|---|---|
| `AS_SHIM_UPSTREAM=https://你的閘道` | 指定上游，不再詢問 |
| `AS_SHIM_PORT=8787` | 指定本地 port |
| `AS_SHIM_NONINTERACTIVE=1` | 不詢問任何問題，全部用預設值 |
| `AS_SHIM_VERSION=v0.1.0` | 安裝指定版本（預設是最新版） |
| `AS_SHIM_BIN_DIR=~/bin` | 改變安裝目錄 |

移除：

```bash
curl -fsSL https://raw.githubusercontent.com/123hi123/as/main/install.sh | bash -s -- --uninstall
```

## 為什麼需要它

Cloudflare 的規則是：一個請求如果 **125 秒內沒有任何 body byte**，就直接回 524。

- sub2api 這類閘道對**串流**請求每 10 秒送一次 SSE ping，所以不會被切。
- **非串流**請求要等整個回應生成完才有第一個 byte，生成超過 125 秒就會 524。
- Claude Code 2.1.300 的串流如果在任何內容出現前就斷掉，會**立刻改用非串流重送**，結果又撞上 524。斷掉的情況包括：只收到 ping 就 EOF、或收到沒有 `event:` 行的 `data: {"type":"error",...}`。

as-shim 從 client 端處理這兩種狀況：

1. **非串流請求（PATH N）**：改成用串流向上游送，再把結果聚合回標準的非串流 Message JSON 回給 Claude Code。聚合邏輯比照官方 SDK 的 accumulator。
2. **串流請求（PATH S）**：一般情況直接轉送。在任何內容出現之前，shim 只放行 ping、其餘先扣住；如果這段期間串流壞掉，就**悄悄重送一次**，Claude Code 不會察覺，也不會被逼進非串流 fallback。

## 斷線續傳（resume）

條件：上游是實作了 [as resume protocol v1](docs/RESUME.md) 的伺服器，而且設定檔 `resume = true`（預設）。一般的 Anthropic 相容閘道會忽略相關 header，這時 as-shim 的行為和以前完全一樣。

1. as-shim 每次向上游送 `/v1/messages` 都帶一個新的隨機 `X-AS-Resume-Id`（128 bit），並要求 `Accept-Encoding: identity`。
2. 伺服器回 `X-AS-Resume: 1` 表示這個回應可以續傳；它會把回應的原始 bytes 暫存起來。
3. as-shim 計算已收到幾個 byte，並留意 `message_stop` 出現了沒（只看、不改任何 byte）。`message_stop` 要連同結尾的空行都收到才算數：SSE 解析器在串流結束時會丟掉沒有空行結尾的事件。
4. 斷線的判定：讀取錯誤、完整的 `message_stop` 之前就 EOF（包括斷在 `message_stop` 那一幀裡、空行之前），或上游 **45 秒**（`resume_idle_secs`）沒有任何 byte（閘道每 10 秒左右會送 ping，安靜這麼久代表連線已經死了，例如 NAT 狀態遺失又沒收到 RST）。
5. 斷線後 as-shim 保持和 Claude Code 的連線不動，用同一個 id 和 offset 呼叫 `GET /__as/resume/{id}?offset=N`，把後續 bytes 原封不動接上，即使斷在一個 SSE 事件的中間也一樣。續傳和 ack / cancel 請求一律開新連線（斷線時連線池裡的閒置連線通常也一起死了），每次等 response header 最多 10 秒。失敗就退避重試（0.5、1、2、4、8、8… 秒）。每次斷線只有**一個硬期限**：從 Claude Code 收到最後一個 byte 起算 `resume_idle_secs + resume_budget_secs`（預設 45 + **225** = **270 秒**）。續傳期間的每一段等待（等 response header、退避、接回來的串流又沒資料）都不會超過這個期限，所以 Claude Code 最多 270 秒收不到資料，低於它 300 秒的閒置上限。只要有新的 byte 進來，這次斷線就算結束；之後再斷線，從那時的最後一個 byte 重新起算。
6. 接不回來（404 / 410 / 416 / 超過時間）就回到沒有續傳時的行為：I/O 錯誤或閒置會中斷 Claude Code 的連線，讓它自己重試；原本就是正常 EOF 的，維持正常結束。
7. 內容出現之前斷線也是續傳，**不重送**，避免伺服器那邊同時跑兩份生成。還沒收到 response header 就失敗（連不上等）則照舊悄悄重送；若請求可能已送達伺服器（不是連線建立失敗），重送前會先對那次嘗試的 id 送 `DELETE …?reason=cancel`，免得伺服器上留下一份沒人要的生成繼續跑。
8. 完成後送 `DELETE …?reason=ack`；Claude Code 先斷線（例如按 Esc）時送 `DELETE …?reason=cancel`，讓伺服器停止生成。還在等 POST 的 response header 時就斷線也一樣：伺服器可能已經收下這次嘗試、開始生成，所以照樣對那個 id 送 cancel。兩者都是 fire-and-forget（傳輸錯誤時重試一次，例如剛好碰上伺服器關掉的 keep-alive 連線）。

驗證身分用的是原請求的 `x-api-key`（或 `Authorization: Bearer`）；續傳與刪除請求帶同一個憑證，別人拿到 id 也沒用。as-shim 不會把憑證或 body 寫進任何 log。

## 上游前的請求淨化（sanitize）

Claude Code 會把 API 錯誤寫進 session 歷史，最麻煩的是 `"API Error: Anthropic refused this request under its usage policy."` 這種被寫成正式訊息的（`model: "<synthetic>"` 的 assistant 訊息，或含錯誤文字的 `tool_result`）。這些訊息之後每個請求都會跟著送上去，可能反覆觸發 usage policy 拒絕。

as-shim 在請求往上游送之前會自動：

1. 移除 `model: "<synthetic>"` 的訊息；
2. 把任何 content 裡的 `Anthropic refused this request under its usage policy. Do not retry this request.` 字樣換成 `[error]`（tool_use / tool_result 配對結構不動）。

回應方向的錯誤照舊由 errmap 正規化成標準 Anthropic 錯誤類型，不需要另外處理。

## `setup` 做了什麼

1. **決定上游 URL。** 預設依序取：Claude Code `settings.json` 裡的 `env.ANTHROPIC_BASE_URL` → 之前設定過的上游 → 環境變數 `ANTHROPIC_BASE_URL`。如果 settings 已經指向 127.0.0.1，就沿用之前的上游。
   - settings 位置是 `$CLAUDE_CONFIG_DIR/settings.json` 或 `~/.claude/settings.json`。
2. **選 port。** 預設 8787；被占用時會提議下一個可用的 port。
3. **寫設定檔** `~/.config/as-shim/config.toml`（有設 `XDG_CONFIG_HOME` 時以它為準）。
4. **安裝並啟動 systemd user service** `~/.config/systemd/user/as-shim.service`。
   - 想在沒登入時也能用（cron、headless），執行 `loginctl enable-linger "$USER"`。
   - 沒有 systemd --user 的環境，會改為提示你手動執行 `as-shim run`。
5. **健康檢查**：`GET http://127.0.0.1:<port>/__as/health`。
6. **詢問「Point Claude Code to the shim? [Y/n]」。**
   - 改之前先備份成 `settings.json.bak-as-shim-<時間戳>`。
   - 只改 `env.ANTHROPIC_BASE_URL`，其他鍵和順序都不動。
   - 原本的值會記下來，`uninstall` 時還原。
   - 如果 shell 也 export 了 `ANTHROPIC_BASE_URL`，會提醒你它可能蓋過 settings。

## 指令

| 指令 | 說明 |
|---|---|
| `as-shim setup [--upstream URL] [--port N] [--yes] [--no-service] [--no-claude]` | 互動式安裝（問題從 `/dev/tty` 讀，所以 `curl \| bash` 也能用） |
| `as-shim run [--upstream URL] [--listen 127.0.0.1:8787]` | 前景執行（systemd 用的就是這個） |
| `as-shim status` | 顯示設定、服務狀態、健康檢查、Claude Code 目前指向哪裡 |
| `as-shim set-upstream <URL>` | 換上游並重啟服務 |
| `as-shim uninstall [--yes]` | 停用並刪除服務、還原 Claude Code 設定、刪除設定檔 |
| `as-shim update` | 從 GitHub 最新 release 下載新 binary（SHA256 驗證後自替換），裝有 systemd 服務時自動重啟並做健康檢查。可用 `AS_SHIM_DOWNLOAD_BASE` 覆寫下載來源 |
| `as-shim --version` | 顯示版本 |

## 設定檔

```toml
upstream = "https://your-gateway.example.com"
listen = "127.0.0.1:8787"          # 只接受 loopback
silent_retries = 1                 # 內容出現前失敗時，悄悄重送幾次
silent_retry_max_elapsed_secs = 60 # 請求開始超過這麼久就不再悄悄重送
silent_retry_delay_ms = 1000
connect_timeout_secs = 30          # 只限制連線建立；整體請求沒有時間上限
max_body_mib = 64
log_level = "info"                 # log 輸出到 stderr / journald，不記錄 header、key、body
use_env_proxy = false              # true = 使用 HTTPS_PROXY / ALL_PROXY / NO_PROXY 環境變數
resume = true                      # 伺服器支援 as resume protocol v1 時啟用斷線續傳
resume_idle_secs = 45              # 上游安靜這麼久就當成斷線（0 = 不偵測）
resume_budget_secs = 225           # 偵測到斷線後最多再花這麼久續傳；idle + budget 要低於 300
log_retention_days = 30            # SQLite 請求記錄保留天數（0 = 永久保留）
# log_db = "/path/to/log.sqlite3"  # 預設 $XDG_STATE_HOME/as-shim/log.sqlite3；"off" = 不記錄
```

0.1 版的設定檔不用改，缺少的鍵會用上面的預設值。

改完設定後要重啟才會生效：`systemctl --user restart as-shim`。

## 行為對照表

| 狀況 | as-shim 的處理 |
|---|---|
| 非串流請求 | 改成 `stream: true` 送出，聚合成非串流 JSON 回傳（保留上游 headers、`request-id`） |
| 非串流：上游回 JSON（忽略了 stream） | 原樣轉送 |
| 非串流：內容出現前 EOF、I/O 錯誤、暫時性錯誤幀 | 悄悄重送 1 次；仍失敗就回對應的 HTTP 錯誤（overloaded → 529、api_error → 500、rate_limit → 429 …） |
| 非串流：`message_delta` 已帶 `stop_reason` 但缺 `message_stop` | 視為完成，正常回傳 |
| 非串流：tool input 被 `max_tokens` 截斷 | 照 SDK 的 partial-JSON 修補方式處理，仍回 200 `stop_reason: max_tokens` |
| 串流請求、一切正常 | 逐 byte 轉送 |
| 串流：內容出現前失敗（只有 ping 就 EOF、data-only `upstream_error`、I/O 錯誤） | 丟掉已扣住的幀，悄悄重送；重試用完就送一個標準的 `event: error` overloaded_error |
| 串流：內容出現前收到非暫時性錯誤幀（例如 `invalid_request_error`） | 補上 `event: error` 行後轉送 |
| 串流：內容出現之後 | 純轉送，不改寫任何幀；上游 I/O 錯誤時直接中斷 client 連線，讓 Claude Code 自己重試串流 |
| 可續傳的回應（`X-AS-Resume: 1`）中途斷線 / 提早 EOF / 45 秒無資料 | 從斷點續傳，輸出與上游 byte-for-byte 相同；續傳失敗才照上一列處理 |
| 可續傳的回應在內容出現之前斷線 | 續傳，不重送 POST |
| Claude Code 先斷線（包括還在等 response header 時） | 關閉上游連線；送出過 `X-AS-Resume-Id` 的請求另外送 `DELETE …?reason=cancel` |
| Cloudflare **524** | **不在本地重試**，改回 HTTP 529 overloaded_error，並帶上 `retry-after`（上游沒有就補 120）。Claude Code 因此也不會自動重打，因為伺服器那邊的請求可能還在跑 |
| 其他 Cloudflare 52x / HTML 錯誤頁 | 狀態碼不變，body 轉成 JSON api_error |
| 同一上游短時間內連續在內容出現前失敗（>3 次 / 60 秒） | 暫停悄悄重送，避免把一次上游故障放大成更多請求 |
| `/v1/messages/count_tokens`、`/v1/models`、其他路徑 | 透明轉送 |

## 請求記錄（SQLite）

每個請求結束後寫一列到 SQLite（WAL 模式），由背景執行緒非同步寫入；寫入失敗只會在 stderr 警告，不會影響代理。

- 位置：`$XDG_STATE_HOME/as-shim/log.sqlite3`，沒設定時是 `~/.local/state/as-shim/log.sqlite3`。用 `log_db` 改路徑，`log_db = "off"` 關閉。
- 保留：`log_retention_days`，預設 **30** 天；`0` = 永久保留。啟動時清一次，之後每小時清一次。
- 不記錄 header、API key、request / response body。

表 `requests`（一個請求一列）：

| 欄位 | 說明 |
|---|---|
| `ts_start_ms` / `ts_end_ms` | 開始 / 結束時間（Unix 毫秒） |
| `method`, `path` | 請求方法與路徑（不含 query） |
| `model`, `client_stream` | 請求的 model，以及 Claude Code 是否要求串流 |
| `mode` | `N`（非串流轉串流）、`S`（串流）、`passthrough` |
| `status`, `upstream_status` | 回給 Claude Code 的狀態碼、上游最後一次的狀態碼 |
| `duration_ms`, `ttfb_ms`, `bytes_out` | 總時長、第一個 byte 送給 Claude Code 的時間、送出的 body bytes |
| `outcome`, `error` | 結果代碼（`ok`、`client_closed`、`cf524_not_retried`…）與簡短錯誤訊息 |
| `silent_retries` | 悄悄重送了幾次 |
| `resume_id`, `resume_attempts`, `resume_recovered` | 可續傳回應的 id、續傳請求次數、是否靠續傳完成 |
| `upstream_request_id` | 上游的 `request-id` / `x-request-id` |

表 `resume_events`（每次續傳嘗試一列）：`request_row`（對應 `requests.id`）、`resume_id`、`ts_ms`、`attempt`、`offset`、`trigger`（`io_error` / `eof_no_stop` / `idle` / `pre_content`）、`result`（`ok` / `http_<code>` / `io_error` / `budget`）、`error`。

查詢範例（服務執行中也可以直接讀）：

```bash
DB=~/.local/state/as-shim/log.sqlite3

# 最近 20 個請求
sqlite3 -header -column "$DB" "
  SELECT datetime(ts_start_ms/1000, 'unixepoch', 'localtime') AS time, mode, model, status,
         outcome, duration_ms, ttfb_ms, resume_attempts AS resumes, resume_recovered AS recovered
  FROM requests ORDER BY id DESC LIMIT 20;"

# 今天各種結果的數量
sqlite3 "$DB" "
  SELECT outcome, count(*) FROM requests
  WHERE ts_start_ms >= strftime('%s', 'now', 'start of day') * 1000
  GROUP BY outcome ORDER BY 2 DESC;"

# 靠續傳救回來的請求，以及每次續傳的細節
sqlite3 -header -column "$DB" "
  SELECT r.id, datetime(e.ts_ms/1000, 'unixepoch', 'localtime') AS time, e.attempt,
         e.offset, e.trigger, e.result, e.error
  FROM resume_events e JOIN requests r ON r.id = e.request_row
  ORDER BY e.id DESC LIMIT 20;"

# 各 model 最近 7 天的請求數、平均 TTFB 與時長
sqlite3 -header -column "$DB" "
  SELECT model, count(*) AS n, round(avg(ttfb_ms)) AS ttfb_ms, round(avg(duration_ms)) AS dur_ms
  FROM requests
  WHERE mode IN ('N', 'S') AND ts_start_ms >= (strftime('%s', 'now') - 7 * 86400) * 1000
  GROUP BY model ORDER BY n DESC;"
```

## 限制

- **伺服器在送出 response header 之前就卡超過 125 秒的情況救不了。** 例如上游排隊太久、連一個 byte 都還沒送，Cloudflare 一樣會 524。這要在伺服器端修。
- **只對執行它的那台機器有效**，其他機器上的 Claude Code 不受影響。
- Claude Code 的非串流請求有自己的總逾時：`API_TIMEOUT_MS`，預設 300 秒。shim 讓 Cloudflare 不再切斷連線，但超過 300 秒的非串流生成仍會被 Claude Code 自己放棄並重送。需要的話可以在 settings 的 `env` 調大 `API_TIMEOUT_MS`，例如 `900000`。
- 悄悄重送時如果剛好拿到 524，此時 HTTP 200 標頭已經送出，只能改送 `event: error`（overloaded_error）。Claude Code 收到後會再重送一次，這一次才會直接收到 529 + `retry-after` 而停下。所以這種情況上游最多多收到 1 個請求。
- 斷線續傳需要伺服器端支援（[as resume protocol v1](docs/RESUME.md)），否則只能中斷連線讓 Claude Code 重試。續傳期間 Claude Code 收不到任何 byte，所以閒置偵測加上續傳時限（預設 45 + 225 = 270 秒，從最後一個 byte 起算的硬期限）要保持在 Claude Code 的 300 秒閒置上限以下；設定超過時 as-shim 啟動時會警告。
- 預設**不使用** `HTTPS_PROXY` / `ALL_PROXY` 環境變數，因為 systemd 服務和互動式 shell 的環境不同，結果會不一致。需要經過 proxy 的話，在設定檔加 `use_env_proxy = true`，並在 unit 裡設定環境變數，例如用 `systemctl --user edit as-shim` 加 `Environment=HTTPS_PROXY=...`。

## 開發

```bash
cargo test
CC_x86_64_unknown_linux_musl=musl-gcc cargo build --release --target x86_64-unknown-linux-musl
```

SQLite 是用 `rusqlite` 的 `bundled` feature 一起編進去的（需要 C 編譯器；musl 靜態版要用 `musl-gcc`，Debian / Ubuntu 的 `musl-tools` 套件）。

推送 `v*` tag 會觸發 GitHub Actions 建置 x86_64 與 aarch64 的 musl binary，並和 `SHA256SUMS` 一起上傳到 Release。

---

## English

**as = anthropic to stream.** `as-shim` is a tiny local proxy (127.0.0.1) for Claude Code talking to an Anthropic-compatible gateway behind Cloudflare. Cloudflare returns 524 when no body byte flows for 125 s.

- **Non-stream requests** are sent upstream as streams, so the gateway's SSE pings keep the connection alive. The SSE is then aggregated back into the exact non-stream Message JSON.
- **Stream requests** are passed through. If the stream dies before any content arrives, as-shim silently re-sends once, so Claude Code does not fall back to a non-stream request that would 524.
- A Cloudflare 524 is never retried locally. It becomes a 529 with `retry-after`, so Claude Code does not hammer a request that may still be running upstream.
- **Resume.** When the upstream is a server implementing the [as resume protocol v1](docs/RESUME.md), every `/v1/messages` request carries a fresh random `X-AS-Resume-Id`. If the response advertises `X-AS-Resume: 1` and the connection breaks (I/O error, EOF before a complete `message_stop` frame, or no bytes for `resume_idle_secs` = 45 s), as-shim keeps Claude Code's connection open, fetches `GET /__as/resume/{id}?offset=N` and splices the remaining bytes in, byte-identical, even mid-event. It retries with backoff until one hard deadline, `resume_idle_secs + resume_budget_secs` = 45 + 225 = 270 s after the last byte Claude Code received (every wait, including a resumed body that stays silent, ends by then; this keeps it below Claude Code's 300 s idle watchdog), then falls back to the old behavior (abort the client connection). Breaks before any content are resumed rather than re-sent, so no duplicate generation is started. Finished responses are acked (`DELETE …?reason=ack`); if Claude Code disconnects first, even while the response headers are still pending, the server is told to cancel (`reason=cancel`). Other gateways simply ignore the headers.
- **Request log.** One row per request (plus one per resume attempt) goes to SQLite in WAL mode, written asynchronously so it never affects proxying: `$XDG_STATE_HOME/as-shim/log.sqlite3` or `~/.local/state/as-shim/log.sqlite3`. Rows are kept for `log_retention_days` = 30 days (`0` = forever); set `log_db = "off"` to disable. No headers, keys or bodies are stored. Example:

  ```bash
  sqlite3 -header -column ~/.local/state/as-shim/log.sqlite3 \
    "SELECT datetime(ts_start_ms/1000,'unixepoch','localtime') AS time, mode, model, status, outcome,
            duration_ms, ttfb_ms, resume_attempts, resume_recovered
     FROM requests ORDER BY id DESC LIMIT 20;"
  ```

New config keys (all optional; v0.1 config files keep working): `resume`, `resume_idle_secs`, `resume_budget_secs`, `log_db`, `log_retention_days`.

Install (Linux x86_64/aarch64). The installer asks for your upstream, installs a systemd user service, and optionally points Claude Code at the shim:

```bash
curl -fsSL https://raw.githubusercontent.com/123hi123/as/main/install.sh | bash
```

Commands: `as-shim setup | run | status | set-upstream <URL> | uninstall | --version`.

Limitations:
- It cannot fix a server that sends nothing, not even headers, for more than 125 s.
- Resume needs server support; without it a broken stream still aborts the client connection.
- It only helps the machine it runs on.
- Claude Code's own non-stream timeout (`API_TIMEOUT_MS`, default 300 s) still applies.

License: MIT.
