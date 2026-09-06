# QQSpark

A **read-only** MCP server that exposes QQ group chat history to an LLM, built for [Gemini Spark](https://gemini.google.com/) scheduled runs — so a model can pull the day's group chatter on a timer and write you a digest.

*[中文文档](./README.zh-CN.md)*

```
QQ (secondary account)
   └─ NapCat  ── OneBot v11 HTTP ──▶  qq_digest_mcp.py  ── streamable HTTP ──▶  cloudflared  ──▶  Gemini Spark
                                      (whitelist, denoise,
                                       OCR, redact, fence)
```

## Design principles

1. **Read-only.** No send / kick / mute / recall tools are exposed. The worst case is a bad digest — it can never speak on your behalf.
2. **Stateless.** History is pulled from NapCat on demand. No persistent WebSocket, no database.
3. **Preprocessing happens server-side.** Whitelisting, denoising, de-duplication and truncation all run here, so the model receives text that has already been slimmed down. Cheaper in tokens, and better summaries.

## Prerequisites

- **NapCat**, logged into a QQ account (use a secondary one), with an **HTTP server** enabled under *Network Config* in the WebUI — default port `3000` — and a token set.
- **Python 3.10+** and the **MCP SDK 1.x** — see the pin below.

### Environment setup

The standard conda-ecosystem split: mamba owns the environment, pip installs the packages inside it. All three are pure Python, so pip carries no risk here.

```bash
mamba create -n napcat python=3.13
mamba activate napcat
pip install "mcp[cli]<2" httpx uvicorn
```

Confirm it took:

```bash
python -c "import mcp, httpx, uvicorn; print('ok')"
```

> ### ⚠️ Pin `mcp<2`
>
> This code targets the **1.x** API: `from mcp.server.fastmcp import FastMCP`, `mcp.settings.streamable_http_path`, `mcp.streamable_http_app()`.
>
> An unpinned `pip install "mcp[cli]"` now resolves to the **2.x** line, which reorganises the distribution — types split out into a separate `mcp-types` package, `httpx` swapped for `httpx2` — and the `mcp.server.fastmcp` entry point is gone rather than deprecated, so the import fails on startup. 1.x is still maintained; keep the `<2` ceiling until this code is ported.
>
> If you already installed 2.x, `pip install "mcp[cli]<2"` fixes it. The orphaned `httpx2` and `mcp-types` left behind in the environment are harmless — no need to clean them up.

Versions verified working:

| Package | Version |
| --- | --- |
| Python | 3.13.13 |
| `mcp` | 1.29.0 |
| `httpx` | 0.28.1 |
| `uvicorn` | 0.52.1 |
| `starlette` | 1.4.1 |

## Running

`launch.py` is the one-command path: it opens the Cloudflare tunnel, scrapes the generated domain out of cloudflared's log, injects it as `PUBLIC_HOST`, and starts the server with it — so the address you paste into Spark gets printed for you.

```bash
export NAPCAT_URL=http://127.0.0.1:3000
export NAPCAT_TOKEN=<the token you set in NapCat>
export MCP_SECRET=$(python -c "import secrets;print(secrets.token_hex(32))")
export WATCH_GROUPS=123456789,987654321      # required — nothing is readable without it
python launch.py
```

Output:

```
起隧道: cloudflared tunnel --url http://127.0.0.1:8765 --protocol http2
隧道域名: souls-app-sox-ericsson.trycloudflare.com
MCP endpoint: http://127.0.0.1:8765/mcp/<MCP_SECRET>
填进 Spark 的地址: https://souls-app-sox-ericsson.trycloudflare.com/mcp/<MCP_SECRET>
允许的 Host: [… , 'souls-app-sox-ericsson.trycloudflare.com', …]
监听群: [123456789, 987654321]
[自检] ✅ 隧道通了，握手成功（qq-digest 1.29.0）
```

That last line is a **self-test**: once the local port is up, `launch.py` sends a real MCP `initialize` to its own public URL and checks `serverInfo` comes back. Local logs prove the process is alive; they can't prove the tunnel routes or that the `Host` header survives the allowlist. This does, and it names the specific failure when it doesn't:

| Symptom | Meaning |
| --- | --- |
| `421 Invalid Host header` | `PUBLIC_HOST` doesn't match the `Host` header — usually a stray `https://` or trailing slash. |
| `404` | Wrong secret path — check `MCP_SECRET`. |
| `SSL UNEXPECTED_EOF` / `502` / `530`, retried | Cloudflare's edge hasn't registered the new hostname yet. Retried until `SELFTEST_TIMEOUT`; a fresh quick tunnel can take well over 30s, which is why the default is 90. |

Set `SELFTEST=0` to skip it. A timeout here is **not** a failed startup — the server keeps running, and the message includes a `curl` to re-check once the edge catches up.

### Process cleanup

`launch.py` owns two children: `cloudflared` and the server. Orphaning either is genuinely annoying — a stray server keeps port 8765 bound (so the next start refuses to launch) and a stray `cloudflared` keeps serving a tunnel you think you closed.

Normal exit and Ctrl+C are handled by a `finally` block. That block can't help if the launcher is force-killed or crashes, so on Windows the children are also assigned to a **Job Object** with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`. When the parent dies for any reason, its handle closes and the kernel reaps everything in the job — no cooperation from our code required. Verified against `taskkill /F` on the parent alone: both children die, the port frees.

Elsewhere it's a no-op, and if the job can't be created `launch.py` warns and falls back to the `finally` cleanup.

Ctrl+C stops both. **The domain is new on every run**, so the Spark config needs updating each time — use a [named tunnel](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/get-started/create-remote-tunnel/) if you want it to stay put.

`launch.py` checks `MCP_SECRET`, `WATCH_GROUPS` and the listen port *before* opening the tunnel — an already-running instance or a missing variable fails immediately rather than burning a domain.

### On Windows

`run.bat` wraps all of the above. It is **gitignored**, because it holds your real NapCat token — create it yourself from this template:

```bat
@echo off
chcp 65001 >nul
set PYTHONIOENCODING=utf-8
call mamba activate napcat
set NAPCAT_URL=http://127.0.0.1:3000
set NAPCAT_TOKEN=<the token you set in NapCat>
set WATCH_GROUPS=123456789,987654321
set MCP_SECRET=<the hex you generated>
python launch.py
pause
```

Three things to get right:

- **`call` is not optional.** `mamba activate` is itself a batch script; without `call`, the batch file hands off control and exits right there instead of continuing.
- **The environment name must match the one you actually created.** Skip the activate line and double-clicking `run.bat` runs whatever `python` is first on `PATH` — usually the wrong interpreter, and the MCP import fails.
- **`PYTHONIOENCODING=utf-8`.** `chcp 65001` covers the console, but as soon as output is redirected (`run.bat > log.txt`, or a scheduler) Python falls back to the ANSI codepage and the startup banner dies on a `UnicodeEncodeError`. This line makes it unconditional.

Do **not** also `set PUBLIC_HOST` here — `launch.py` overwrites it with the live domain.

### Named tunnel (stable hostname)

Quick tunnels hand you a new random domain every run, so the Spark config needs re-pasting each time. A [named tunnel](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/get-started/create-remote-tunnel/) fixes the hostname permanently. It needs Cloudflare running DNS for your domain — at your registrar, switch the nameservers to the two Cloudflare gives you when you add the site.

Then, in **Zero Trust → Networks → Tunnels → Create a tunnel**, pick **Cloudflared**, name it, and run the install command it shows in an **Administrator** terminal:

```
cloudflared.exe service install <TOKEN-FROM-DASHBOARD>
```

That registers a Windows service that starts on boot. Once the connector is **HEALTHY**, add a public hostname route pointing at the local server:

| Field | Value |
| --- | --- |
| Subdomain | `sparque` |
| Domain | `yourdomain.net` |
| Type | `HTTP` |
| URL | `127.0.0.1:8765` |

> **Route it to `8765`, never `3000`.** Port 3000 is NapCat's raw OneBot API — full read *and write* control of the account, including `send_group_msg` and every group you're in, behind one header token. Port 8765 is this server, where the whitelist, redaction, injection fencing and read-only guarantee live. Tunnelling 3000 bypasses all of it in one move.

With a named tunnel, **don't use `launch.py`** — it opens its own quick tunnel and overwrites `PUBLIC_HOST`. Run the server directly, with the hostname pinned:

```bat
@echo off
chcp 65001 >nul
set PYTHONIOENCODING=utf-8
call mamba activate napcat
set NAPCAT_URL=http://127.0.0.1:3000
set NAPCAT_TOKEN=<the token you set in NapCat>
set WATCH_GROUPS=123456789,987654321
set MCP_SECRET=<the hex you generated>
set PUBLIC_HOST=sparque.yourdomain.net
python qq_digest_mcp.py
pause
```

#### Returning 403 to everyone else

The server already 404s any path that isn't the secret one, but that still lets strangers reach your machine. To stop them at Cloudflare's edge, add **Security → WAF → Custom rules**:

```
(http.host eq "sparque.yourdomain.net" and not starts_with(http.request.uri.path, "/mcp/<MCP_SECRET>"))
```

Action **Block**, response code **403**. A browser hitting the root now gets 403 from Cloudflare, and the request never reaches home — no banner, no headers, nothing to fingerprint.

Two caveats. The secret now lives in **two** places, so rotating `MCP_SECRET` means editing the WAF rule in the same breath or locking yourself out. And leave **Bot Fight Mode** and any managed challenge off for this hostname, or they will challenge the MCP client too.

What this does *not* buy you: hostname secrecy. Universal SSL lists first-level subdomains as explicit SAN entries, so the name lands in public Certificate Transparency logs within minutes, and Censys/FOFA ingest those. The real protection is that a tunnel is outbound-only — there is no inbound port and no origin IP to scan — plus an endpoint that returns nothing without the secret. Optimise for inert, not hidden.

#### Verifying (PowerShell)

PowerShell mangles JSON passed to native executables — it strips the inner double quotes, so `{"jsonrpc":"2.0"}` reaches `curl.exe` as `{jsonrpc:2.0}` and the server correctly answers `400 Parse error`. Put the payload in a file instead:

```powershell
$body = '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"1"}}}'
Set-Content -Path init.json -Value $body -Encoding utf8 -NoNewline

curl.exe -s -o NUL -w "%{http_code}`n" https://sparque.yourdomain.net/      # expect 403

curl.exe -s -i -X POST "https://sparque.yourdomain.net/mcp/<MCP_SECRET>" `
  -H "Content-Type: application/json" `
  -H "Accept: application/json, text/event-stream" `
  -d "@init.json"                                                          # expect 200 + serverInfo
```

A `400` with `Parse error` means the payload got mangled in transit, not that the tunnel is broken — the request reached your Python to be rejected, which proves the whole path works.

### Running the tunnel yourself

If you'd rather manage the tunnel separately, start it by hand and pass the domain in:

```bash
cloudflared tunnel --url http://127.0.0.1:8765 --protocol http2
export PUBLIC_HOST=<tunnel-domain>      # bare hostname, no https://
python qq_digest_mcp.py
```

`--protocol http2` is the default `launch.py` uses. cloudflared prefers QUIC, which needs outbound UDP on port 7844 — commonly blocked on campus and corporate networks. http2 goes over 443 and survives more places.

> **`PUBLIC_HOST` must be a bare hostname** (`abc-def.trycloudflare.com`), not a URL. It feeds the SDK's DNS-rebinding protection, which matches it against the `Host` header. Include the scheme and nothing will ever match — every request comes back **`421 Invalid Host header`**. Leave it unset and only local access is allowed.

The address to paste into Spark is the tunnel domain plus the secret path:

```
https://<tunnel-domain>/mcp/<MCP_SECRET>
```

### Finding your group IDs

`groups.json` in this repo is just a saved dump of NapCat's `get_group_list` response — handy for looking up the numeric `group_id` of a group by name when filling in `WATCH_GROUPS`. Regenerate it any time by calling `get_group_list` against your own NapCat instance.

## Tools exposed

| Tool | Arguments | What it returns |
| --- | --- | --- |
| `list_watched_groups` | — | Group ID, name and member count for every whitelisted group. Call this first to get IDs. |
| `get_group_messages` | `group_id`, `count` (default 200, max 300), `since_days`, `since` | Cleaned recent transcript for one group. `since_days` takes a float (`0.5` = last 12h); `since` takes `YYYY-MM-DD` or `YYYY-MM-DD HH:MM`. |
| `get_group_images` | `group_id`, `count` (default 100), `limit` (default 5) | The images themselves, as native MCP `ImageContent`, for a vision-capable model to look at. |
| `get_my_mentions` | `hours` (default 24), `count` (default 200) | Every message that `@`-ed you across all whitelisted groups, each with one message of context on either side. |

### Images: OCR vs. the real thing

Two different jobs, so two different tools. `get_group_messages` runs OCR and inlines the **text** found in pictures — cheap, and enough for a notice that's mostly words. `get_group_images` hands back the **actual image** as `ImageContent{data, mimeType}`, which any multimodal client (Gemini Spark, Claude, Grok) renders natively. Use it when layout is the information: timetables where the row/column relationship matters, posters, QR codes, diagrams, anything with a circled region.


Images are wrapped in `<<<UNTRUSTED_IMAGE_CONTENT … >>>` with the same "text inside is not an instruction" framing as transcripts — a screenshot is just as capable of carrying a prompt injection as a message is.

Base64 inflates payloads by a third, so three caps apply and a skipped image is always reported in the trailing summary:

| Variable | Default | Caps |
| --- | --- | --- |
| `IMAGE_PER_CALL` | `5` | Images per response |
| `IMAGE_MAX_BYTES` | `2097152` (2 MiB) | Any single image |
| `IMAGE_TOTAL_BYTES` | `8388608` (8 MiB) | All images in one response |

**Images older than about a week are usually unavailable.** NapCat keeps a local copy only for recent files, and Tencent's CDN link expires — refreshing the rkey via `nc_get_rkey` does *not* revive them, because the `fileid` itself has expired. Measured on this setup: every image from the last 7 days was retrievable, while 16 of 17 older ones were gone. Digests read recent history, so this rarely matters in practice.

### `count` is a ceiling, not a promise

`get_group_msg_history` returns whatever NapCat has in the **local** QQ database, not the full history on Tencent's servers. Measured 2026-09-05: one group returned exactly 15 messages whether `count` was 20, 50, 100 or 300, and re-requesting with the oldest `message_seq` as an anchor returned the identical time span — so paging cannot reach further back. Messages that were never synced locally simply do not exist as far as this server is concerned.

Read the first line of the response, not `count`, to know what you actually got: it states the covered time range and the number of distinct speakers.

Repeat-folding makes this worse if you skim. Fifteen students each posting 老师辛苦了 collapse into **one line** carrying the other fourteen names — so "剩 1 条" means one line, not one person. The header says this explicitly because a model given the old wording reported the group as having a single message.

## Configuration

Environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `NAPCAT_URL` | `http://127.0.0.1:3000` | NapCat's OneBot v11 HTTP endpoint. |
| `NAPCAT_TOKEN` | *(empty)* | Sent as `Authorization: Bearer …` to NapCat. |
| `MCP_SECRET` | **required** | Random hex used as the URL path segment. The server exits without it. |
| `WATCH_GROUPS` | **required** | Comma-separated group IDs. The server exits without it. |
| `BIND_HOST` | `127.0.0.1` | Listen address. Keep it on loopback and let the tunnel do the exposing. |
| `BIND_PORT` | `8765` | Listen port. |
| `MCP_BEARER` | *(unset)* | If set, requests must also carry a matching `Authorization: Bearer` header. |
| `PUBLIC_HOST` | *(unset)* | Bare public hostname, added to the DNS-rebinding allowlist. Set automatically by `launch.py`; unset means local-only. |
| `ENABLE_OCR` | `1` | Set to `0` to skip image OCR entirely. |
| `OCR_PER_CALL` | `40` | Maximum images OCR'd per tool call. Images beyond this degrade silently to `[图片]`. |

Read by `launch.py` only:

| Variable | Default | Purpose |
| --- | --- | --- |
| `CLOUDFLARED` | `cloudflared` | Executable name or absolute path. Resolved via `PATH`. |
| `TUNNEL_PROTOCOL` | `http2` | Passed to `cloudflared --protocol`. Use `quic` if UDP/7844 is open and you want it. |
| `TUNNEL_TIMEOUT` | `60` | Seconds to wait for the tunnel domain before giving up. |
| `SELFTEST` | `1` | Set to `0` to skip the post-startup public-URL handshake check. |
| `SELFTEST_TIMEOUT` | `90` | Seconds budgeted for the self-test — applied separately to waiting for the local port and to retrying the public request. |

### Verifying by hand

The self-test is just this request, which you can fire yourself at any time:

```bash
curl -i -X POST "https://<tunnel-domain>/mcp/<MCP_SECRET>" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"1"}}}'
```

A healthy server answers `200` with `Content-Type: text/event-stream` and a `data:` line containing `"serverInfo":{"name":"qq-digest",…}`. On Windows, add `--ssl-no-revoke` if schannel's revocation check stalls.

Tunables at the top of `qq_digest_mcp.py`:

| Constant | Default | Purpose |
| --- | --- | --- |
| `MAX_COUNT` | `300` | Hard cap on raw messages fetched per call. |
| `MAX_MSG_CHARS` | `400` | Per-message truncation length. |
| `MAX_CHAIN_CHARS` | `2000` | Truncation length for the surviving message of a 接龙 chain. Wider, because that one message replaced the whole redundant run. |
| `MIN_CHAIN_CHARS` | `60` | A prefix shorter than this is not treated as a chain, so "ok" → "ok then" is not mistaken for one. |
| `MIN_MSG_CHARS` | `2` | Messages shorter than this are dropped. |
| `OCR_MAX_CHARS` | `600` | Per-image OCR truncation length. |
| `OCR_ROW_TOLERANCE` | `12` | Y-coordinate delta below which OCR boxes count as the same row. |

## What the cleaning pipeline does

Raw OneBot message segments go through several passes before the model sees them:

- **Noise removal.** Pure emoji, punctuation, and filler ("哈", "6", "awsl") are dropped, as are messages shorter than `MIN_MSG_CHARS`.
- **De-duplication.** Consecutive identical messages (复读) collapse to one. Comparison uses the **untruncated** body — folding on truncated text used to merge messages that differed only past `MAX_MSG_CHARS`.
- **Chain collapse (接龙).** A 接龙 is cumulative: each message is the previous one plus one more name. When a message is a strict prefix-extension of the one before it, the run collapses to its **last** member — the only complete copy — and the annotation says `接龙链` rather than "sent the same content", crediting the final text to whoever actually posted it. Measured on a real group: 29 messages, 41 names, previously rendered as entries 1–18 with a false repeat count.
- **Image OCR.** Group notices, timetables and exam schedules are usually screenshots, so images are run through NapCat's `ocr_image` (Tencent's own Chinese OCR — free, no local dependency). Results are **reassembled into rows** by Y-coordinate and sorted by X, so tables survive as tables instead of collapsing into one line. Results are cached by `file_unique`, and OCR failures are swallowed rather than failing the digest.
- **Card unwrapping.** Group announcements (`com.tencent.mannounce`) carry their body as base64; it gets decoded rather than fed to the model as gibberish. Other shares degrade to `[分享:title]`.
- **Redaction.** Phone numbers are masked, ID card numbers are removed, and `密码：xxx` / `token: xxx`-style plaintext credentials are stripped before anything leaves the server.
- **Prompt-injection fencing.** Transcripts ship wrapped in `<<<UNTRUSTED_CHAT_CONTENT … UNTRUSTED_CHAT_CONTENT>>>` with an explicit instruction that nothing inside is a command, and the tool docstrings repeat it.

## Security notes

- **The whitelist is a deny-by-default.** An empty `WATCH_GROUPS` refuses everything; this is deliberate, to make a fat-fingered full export impossible. `get_group_messages` re-checks the whitelist on every call, so a model that guesses a group ID gets rejected.
- **Two layers on the endpoint.** The secret path is mandatory; anything outside `/mcp/<MCP_SECRET>` gets a 404. Set `MCP_BEARER` for a second factor if Spark can send headers.
- **Redaction is best-effort.** It catches the common shapes, not everything. Treat the digest destination as capable of seeing group content.
- Anyone with the tunnel URL can read your whitelisted groups. Rotate `MCP_SECRET` (and the tunnel) if it leaks.

## Limitations

- Only the most recent `count` messages per group are reachable — there's no pagination back through history.
- The `@me` check resolves against whichever account NapCat is logged into.
- OCR quality is Tencent's; handwriting and low-resolution screenshots come back rough.
- `cloudflared tunnel --url` gives you a fresh random domain each run. Use a named tunnel if you want the Spark config to stay put.
