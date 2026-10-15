# as — anthropic to stream

`as-shim` 是跑在 Claude Code 那台機器上的本地小代理（127.0.0.1）。它讓 Claude Code 對 Anthropic 相容閘道（例如架在 Cloudflare 後面的 sub2api）的請求**一律走串流**，Cloudflare 因此不會因為 125 秒沒有資料而回 **524**。

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
```

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
| Cloudflare **524** | **不在本地重試**，改回 HTTP 529 overloaded_error，並帶上 `retry-after`（上游沒有就補 120）。Claude Code 因此也不會自動重打，因為伺服器那邊的請求可能還在跑 |
| 其他 Cloudflare 52x / HTML 錯誤頁 | 狀態碼不變，body 轉成 JSON api_error |
| 同一上游短時間內連續在內容出現前失敗（>3 次 / 60 秒） | 暫停悄悄重送，避免把一次上游故障放大成更多請求 |
| `/v1/messages/count_tokens`、`/v1/models`、其他路徑 | 透明轉送 |

## 限制

- **伺服器在送出 response header 之前就卡超過 125 秒的情況救不了。** 例如上游排隊太久、連一個 byte 都還沒送，Cloudflare 一樣會 524。這要在伺服器端修。
- **只對執行它的那台機器有效**，其他機器上的 Claude Code 不受影響。
- Claude Code 的非串流請求有自己的總逾時：`API_TIMEOUT_MS`，預設 300 秒。shim 讓 Cloudflare 不再切斷連線，但超過 300 秒的非串流生成仍會被 Claude Code 自己放棄並重送。需要的話可以在 settings 的 `env` 調大 `API_TIMEOUT_MS`，例如 `900000`。
- 悄悄重送時如果剛好拿到 524，此時 HTTP 200 標頭已經送出，只能改送 `event: error`（overloaded_error）。Claude Code 收到後會再重送一次，這一次才會直接收到 529 + `retry-after` 而停下。所以這種情況上游最多多收到 1 個請求。
- 預設**不使用** `HTTPS_PROXY` / `ALL_PROXY` 環境變數，因為 systemd 服務和互動式 shell 的環境不同，結果會不一致。需要經過 proxy 的話，在設定檔加 `use_env_proxy = true`，並在 unit 裡設定環境變數，例如用 `systemctl --user edit as-shim` 加 `Environment=HTTPS_PROXY=...`。

## 開發

```bash
cargo test
cargo build --release --target x86_64-unknown-linux-musl
```

推送 `v*` tag 會觸發 GitHub Actions 建置 x86_64 與 aarch64 的 musl binary，並和 `SHA256SUMS` 一起上傳到 Release。

---

## English

**as = anthropic to stream.** `as-shim` is a tiny local proxy (127.0.0.1) for Claude Code talking to an Anthropic-compatible gateway behind Cloudflare. Cloudflare returns 524 when no body byte flows for 125 s.

- **Non-stream requests** are sent upstream as streams, so the gateway's SSE pings keep the connection alive. The SSE is then aggregated back into the exact non-stream Message JSON.
- **Stream requests** are passed through. If the stream dies before any content arrives, as-shim silently re-sends once, so Claude Code does not fall back to a non-stream request that would 524.
- A Cloudflare 524 is never retried locally. It becomes a 529 with `retry-after`, so Claude Code does not hammer a request that may still be running upstream.

Install (Linux x86_64/aarch64). The installer asks for your upstream, installs a systemd user service, and optionally points Claude Code at the shim:

```bash
curl -fsSL https://raw.githubusercontent.com/123hi123/as/main/install.sh | bash
```

Commands: `as-shim setup | run | status | set-upstream <URL> | uninstall | --version`.

Limitations:
- It cannot fix a server that sends nothing, not even headers, for more than 125 s.
- It only helps the machine it runs on.
- Claude Code's own non-stream timeout (`API_TIMEOUT_MS`, default 300 s) still applies.

License: MIT.
