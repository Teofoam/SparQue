# QQSpark

一个**只读**的 MCP Server，把 QQ 群聊记录喂给大模型，专为 [Gemini Spark](https://gemini.google.com/) 的定时任务而写——让模型每天定点把群里的消息拉一遍，给你写份简报。

*[English](./README.md)*

```
QQ（小号）
  └─ NapCat ── OneBot v11 HTTP ──▶ qq_digest_mcp.py ── streamable HTTP ──▶ cloudflared ──▶ Gemini Spark
                                   （白名单、去噪、
                                     OCR、脱敏、加围栏）
```

## 设计原则

1. **只读。** 不暴露任何发送 / 群管 / 撤回类工具。最坏情况是简报质量差，不会替你说话。
2. **无状态。** 按需向 NapCat 拉历史，不跑常驻 WS、不落库。
3. **预处理在服务端做。** 白名单、去噪、去复读、截断都在这里完成，喂给模型的是已经瘦过身的文本，省 token 也提高摘要质量。

## 前置

- **NapCat** 已登录 QQ（建议用小号），并在 WebUI「网络配置」里开了一个 **HTTP 服务器**（默认端口 `3000`），且设好 token。
- **Python 3.10+**，以及 **1.x 版的 MCP SDK**——见下面的版本锁定说明。

### 环境搭建

conda 生态的标准姿势：mamba 管环境，环境内部用 pip 装包。这三个都是纯 Python 包，pip 装没有任何风险。

```bash
mamba create -n napcat python=3.13
mamba activate napcat
pip install "mcp[cli]<2" httpx uvicorn
```

装完确认一下：

```bash
python -c "import mcp, httpx, uvicorn; print('ok')"
```

> ### ⚠️ 必须锁 `mcp<2`
>
> 这份代码是按 **1.x** 的 API 写的：`from mcp.server.fastmcp import FastMCP`、`mcp.settings.streamable_http_path`、`mcp.streamable_http_app()`。
>
> 现在不指定版本直接 `pip install "mcp[cli]"` 会装到 **2.x**。2.x 把整个分发重排了——types 拆成独立的 `mcp-types` 包，`httpx` 换成了 `httpx2`——而且 `mcp.server.fastmcp` 这个入口是**直接删掉、不是废弃**，所以一启动 import 就炸。1.x 仍在维护，在这份代码迁移完成之前，保持 `<2` 这个上限。
>
> 已经装成 2.x 了也不用重建环境，`pip install "mcp[cli]<2"` 就能回去。降级后环境里残留的 `httpx2` 和 `mcp-types` 无害，不用管。

已验证可用的版本：

| 包 | 版本 |
| --- | --- |
| Python | 3.13.13 |
| `mcp` | 1.29.0 |
| `httpx` | 0.28.1 |
| `uvicorn` | 0.52.1 |
| `starlette` | 1.4.1 |

## 启动

`launch.py` 是一键路径：它把 cloudflared 隧道拉起来，从日志里抠出随机域名，塞进 `PUBLIC_HOST`，再带着它启动服务——要填进 Spark 的地址会直接打在屏幕上。

```bash
export NAPCAT_URL=http://127.0.0.1:3000
export NAPCAT_TOKEN=你在NapCat里设的token
export MCP_SECRET=$(python -c "import secrets;print(secrets.token_hex(32))")
export WATCH_GROUPS=123456789,987654321      # 必填，不填的话什么都读不到
python launch.py
```

输出长这样：

```
起隧道: cloudflared tunnel --url http://127.0.0.1:8765 --protocol http2
隧道域名: souls-app-sox-ericsson.trycloudflare.com
MCP endpoint: http://127.0.0.1:8765/mcp/<MCP_SECRET>
填进 Spark 的地址: https://souls-app-sox-ericsson.trycloudflare.com/mcp/<MCP_SECRET>
允许的 Host: [… , 'souls-app-sox-ericsson.trycloudflare.com', …]
监听群: [123456789, 987654321]
[自检] ✅ 隧道通了，握手成功（qq-digest 1.29.0）
```

最后那行是**自检**：本地端口起来之后，`launch.py` 会朝自己的公网地址真发一个 MCP `initialize`，确认能收到 `serverInfo`。本地日志只能证明进程活着，证明不了隧道通不通、`Host` 头过不过得了白名单——这一发能，而且失败时会直接说是哪种失败：

| 现象 | 含义 |
| --- | --- |
| `421 Invalid Host header` | `PUBLIC_HOST` 跟 `Host` 头对不上，多半是多带了 `https://` 或末尾斜杠。 |
| `404` | 密钥路径不对，检查 `MCP_SECRET`。 |
| `SSL UNEXPECTED_EOF` / `502` / `530`，并在重试 | Cloudflare 边缘还没认这个新域名。会一直重试到 `SELFTEST_TIMEOUT`；临时隧道经常要 30 秒以上，所以默认给到 90。 |

不想要就 `SELFTEST=0`。自检超时**不等于**启动失败——服务照常在跑，那条消息里会附一条 `curl`，等边缘生效之后自己再验一次就行。

### 进程清理

`launch.py`手底下有两个子进程：`cloudflared` 和服务端。哪个变成孤儿都挺烦——服务端孤儿会一直占着 8765（下次启动直接被端口检查拦下），`cloudflared` 孤儿则会继续挂着一条你以为已经关掉的隧道。

正常退出和 Ctrl+C 由 `finally` 兜。但被强杀或者直接崩掉时 `finally` 是不会执行的，所以 Windows 上还额外把子进程挂进了一个带 `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` 的 **Job Object**：父进程不管怎么死，句柄一关，内核就把 job 里的进程全部收掉，完全不需要我们的代码配合。已经拿 `taskkill /F` 单杀父进程验证过：两个子进程一起没，端口当场释放。

非 Windows 上这段是空操作；job 建不起来的话会打条警告，退回到 `finally` 清理。

Ctrl+C 会把隧道和服务一起收掉。**每次启动域名都会变**，所以 Spark 里的地址每次都得跟着改；想固定下来就用[具名隧道](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/get-started/create-remote-tunnel/)。

`launch.py` 会在**开隧道之前**先查 `MCP_SECRET`、`WATCH_GROUPS` 和监听端口——变量没填、或者上一个实例还开着，都会当场退出，不会白开一条隧道。

### Windows 下

`run.bat` 把上面这一套包起来了。它**已被 gitignore**，因为里面存着你真实的 NapCat token——照下面的模板自己建一个：

```bat
@echo off
chcp 65001 >nul
set PYTHONIOENCODING=utf-8
call mamba activate napcat
set NAPCAT_URL=http://127.0.0.1:3000
set NAPCAT_TOKEN=你在NapCat里设的token
set WATCH_GROUPS=123456789,987654321
set MCP_SECRET=你生成的那串hex
python launch.py
pause
```

三个容易踩的点：

- **`call` 不能省。** `mamba activate` 本身就是个批处理脚本，不加 `call` 的话，bat 执行到这一行就把控制权交出去、直接退出了，后面的根本不跑。
- **环境名要跟你实际建的那个对上。** 不写这行 activate，双击 `run.bat` 用的是 `PATH` 里第一个 `python`——通常是系统默认的那个解释器，然后 MCP 就 import 失败。
- **`PYTHONIOENCODING=utf-8`。** `chcp 65001` 只管控制台；一旦输出被重定向（`run.bat > log.txt`，或者被计划任务拉起），Python 会退回系统 ANSI 代码页，启动横幅里的中文直接 `UnicodeEncodeError` 崩掉。加这一行就一劳永逸。

另外**不要在这里再 `set PUBLIC_HOST`**——`launch.py` 会用当前隧道的域名把它覆盖掉。

### 具名隧道（固定域名）

临时隧道每跑一次换一个随机域名，Spark 里的地址就得重贴一次。[具名隧道](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/get-started/create-remote-tunnel/)能把域名固定下来。前提是 DNS 得交给 Cloudflare 托管——在域名注册商那边，把 nameserver 改成 Cloudflare 添加站点时给的那两个。

然后在 **Zero Trust → Networks → Tunnels → Create a tunnel** 里选 **Cloudflared**、起个名字，把它给的安装命令用**管理员**终端跑一遍：

```
cloudflared.exe service install <面板里给的TOKEN>
```

这会注册一个开机自启的 Windows 服务。等连接器变成 **HEALTHY**，再加一条 public hostname 路由指向本地服务：

| 字段 | 值 |
| --- | --- |
| Subdomain | `sparque` |
| Domain | `yourdomain.net` |
| Type | `HTTP` |
| URL | `127.0.0.1:8765` |

> **只能指向 `8765`，绝对不要指 `3000`。** 3000 是 NapCat 的原始 OneBot 接口——对账号有完整的读**写**权限，`send_group_msg`、你在的每一个群都在里面，前面只挡着一个 header token。8765 才是本服务，白名单、脱敏、注入围栏、只读保证全都住在这一层。把 3000 挂出去，等于一步把这些全绕过。

用了具名隧道就**别再用 `launch.py`**——它会自己开一条临时隧道，还会把 `PUBLIC_HOST` 覆盖掉。直接跑服务、把域名钉死：

```bat
@echo off
chcp 65001 >nul
set PYTHONIOENCODING=utf-8
call mamba activate napcat
set NAPCAT_URL=http://127.0.0.1:3000
set NAPCAT_TOKEN=你在NapCat里设的token
set WATCH_GROUPS=123456789,987654321
set MCP_SECRET=你生成的那串hex
set PUBLIC_HOST=sparque.yourdomain.net
python qq_digest_mcp.py
pause
```

#### 让其他人都吃 403

服务端本来就会把非密钥路径 404 掉，但那样陌生人的请求还是能打到你机器上。想让他们在 Cloudflare 边缘就被拦下，加一条 **Security → WAF → Custom rules**：

```
(http.host eq "<<<YOUR-HOSTNAME>>>" and not starts_with(http.request.uri.path, "/mcp/<<<YOUR-MCP-SECRET>>>"))
```

动作选 **Block**，响应码 **403**。这样浏览器打根路径直接被 Cloudflare 403，请求根本到不了家里——没有 banner、没有响应头，没有任何可以指纹识别的东西。

> ### ⚠️ 两个占位符都要替换掉，然后必须实测规则真的生效
>
> 这条规则是**静默失效**的。`<<<YOUR-HOSTNAME>>>` 不替换，表达式在语法上照样成立：保存不报错，面板里显示 **Active**，但它谁都匹配不上——没有任何请求的 `Host` 是这个值。没有报错、没有告警，什么异常都看不到。一条从不触发的 WAF 规则，长得和正常工作的一模一样。
>
> 所以规则保存了不算完，**看到一个请求被拦下来**才算完。做法见下面的[怎么验](#怎么验powershell)——真正的判据是**服务端控制台没有新日志**，光看状态码不够。

两个注意点。密钥现在存在**两个**地方了，以后轮换 `MCP_SECRET` 必须同时改 WAF 规则，否则就是把自己锁在外面。另外这个域名上的 **Bot Fight Mode** 和各类 managed challenge 都要关掉，不然它们会连 MCP 客户端一起挑战。

这套**换不来**的东西是：域名保密。Universal SSL 会把一级子域名以明确的 SAN 条目写进证书，所以域名几分钟内就会出现在公开的证书透明度（CT）日志里，Censys、FOFA 都在吃这个源。真正保护你的是隧道本身是出站连接——没有入站端口、没有源站 IP 可扫——外加一个不给密钥就什么都不吐的端点。要的是"打不开"，不是"找不到"。

#### 怎么验（PowerShell）

PowerShell 往原生 exe 传参会吞掉里层的双引号，`{"jsonrpc":"2.0"}` 到 `curl.exe` 手里就变成了 `{jsonrpc:2.0}`，服务端于是正确地回 `400 Parse error`。把 body 写进文件再传：

```powershell
$body = '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"1"}}}'
Set-Content -Path init.json -Value $body -Encoding utf8 -NoNewline

# 1. 必须在边缘就被拦掉的路径——应该是 403
curl.exe -s -o NUL -w "%{http_code}`n" "https://<<<YOUR-HOSTNAME>>>/foo"

# 2. 密钥路径——应该是 200。把这个也拦了的规则，等于把 Spark 一起锁在外面。
curl.exe -s -i -X POST "https://<<<YOUR-HOSTNAME>>>/mcp/<<<YOUR-MCP-SECRET>>>" `
  -H "Content-Type: application/json" `
  -H "Accept: application/json, text/event-stream" `
  -d "@init.json"
```

**跑第 1 条的时候把服务端控制台放在看得见的地方。** 状态码本身说明不了是谁回的，控制台可以。

| 第 1 条的结果 | 控制台 | 含义 |
| --- | --- | --- |
| `403` | **没有新日志** | 正常。请求在边缘就死了，根本没到你这儿。 |
| `404` | 冒出 `GET /foo … 404 Not Found` | **规则没生效。** 占位符没换，或者域名对不上。 |
| `403` | 冒出新日志 | 那不是 WAF——是你自己这边回的 403。 |

想确认这个 403 到底是谁给的，看响应体：Cloudflare 会回大约 4.5 KB 的 HTML，里面有 `Sorry, you have been blocked`；而本服务只会回 21 字节的 `{"error":"not found"}`。

第 2 条如果是 `403`，那是反过来错了——规则里的密钥和启动脚本里的对不上，Spark 也一起被拦了。

看到 `400 Parse error` 不代表隧道坏了，恰恰相反：请求已经打到你的 Python 才被拒的，这说明整条链路是通的。

### 想自己管隧道

不用 `launch.py` 也行，自己起隧道、把域名传进去：

```bash
cloudflared tunnel --url http://127.0.0.1:8765 --protocol http2
export PUBLIC_HOST=<隧道域名>      # 裸域名，不要带 https://
python qq_digest_mcp.py
```

`--protocol http2` 是 `launch.py` 的默认值。cloudflared 优先走 QUIC，那需要出站 UDP 7844，校园网和公司网经常封；http2 走 443，活下来的概率高得多。

> **`PUBLIC_HOST` 必须是裸域名**（`abc-def.trycloudflare.com`），不是完整 URL。它会进 SDK 的 DNS 重绑定防护白名单，那里比对的是 `Host` 头；带上 `https://` 就永远匹配不上，所有请求都会变成 **`421 Invalid Host header`**。不填则只允许本机访问。

填进 Spark 的地址 = 隧道域名 + 密钥路径：

```
https://<隧道域名>/mcp/<MCP_SECRET>
```

### 怎么找群号

仓库里的 `groups.json` 就是 NapCat `get_group_list` 的一份返回存档，填 `WATCH_GROUPS` 时可以拿它按群名反查数字群号。想更新的话，对着自己的 NapCat 再调一次 `get_group_list` 覆盖掉就行。

## 提供的工具

| 工具 | 参数 | 返回 |
| --- | --- | --- |
| `list_watched_groups` | — | 白名单内每个群的群号、群名、人数。做简报前先调它拿群号。 |
| `get_group_messages` | `group_id`、`count`（默认 200，上限 300）、`since_days`、`since` | 单个群清洗后的聊天记录。`since_days` 收小数（`0.5` = 最近 12 小时）；`since` 收 `YYYY-MM-DD` 或 `YYYY-MM-DD HH:MM`。 |
| `get_group_images` | `group_id`、`count`（默认 100）、`limit`（默认 5） | 图片原图，以 MCP 原生 `ImageContent` 返回，交给多模态模型自己看。 |
| `get_my_mentions` | `hours`（默认 24）、`count`（默认 200） | 跨所有白名单群，找出 @ 过我的消息，每条附带前后各一条上下文。 |

### 图片：OCR 和原图是两件事

两种活，所以两个工具。`get_group_messages` 走 OCR，把图里的**文字**抠出来塞进正文——便宜，对付以文字为主的通知够用。`get_group_images` 返回的是**图片本身**（`ImageContent{data, mimeType}`），任何多模态客户端（Gemini Spark、Claude、Grok）都能直接看。什么时候用它：版式本身就是信息的时候——课表这种行列对应关系要紧的、海报、二维码、示意图、圈了某一块的截图。


图片同样用 `<<<UNTRUSTED_IMAGE_CONTENT … >>>` 框起来，跟聊天记录一个待遇：截图里塞提示注入和消息里塞是一样容易的。

base64 会让体积涨三分之一，所以卡了三道；被跳过的图会在结尾的汇总里说明：

| 变量 | 默认值 | 限制 |
| --- | --- | --- |
| `IMAGE_PER_CALL` | `5` | 单次返回几张 |
| `IMAGE_MAX_BYTES` | `2097152`（2 MiB） | 单张体积 |
| `IMAGE_TOTAL_BYTES` | `8388608`（8 MiB） | 单次合计体积 |

**超过一周左右的图基本就取不回来了。** NapCat 只对近期文件留本地副本，腾讯的 CDN 链接也会过期——而且用 `nc_get_rkey` 换个新 rkey 也救不回来，因为过期的是 `fileid` 本身。在这台机器上实测：最近 7 天的图全都能取到，更早的 17 张里有 16 张已经没了。简报读的都是近期消息，所以实际用起来很少碰到。

### `count` 是上限，不是承诺

`get_group_msg_history` 给的是 NapCat **本地** QQ 数据库里缓存的那一段，不是腾讯服务器上的全部历史。2026-09-05 实测：某个群无论 `count` 给 20、50、100 还是 300，一律只回 15 条；拿最老那条的 `message_seq` 当锚点再请求，回来的时间跨度一模一样——翻页翻不到更早的东西。本地没同步过的消息，对这个服务来说就是不存在。

想知道实际拿到了多少，看返回文本的第一行，别拿 `count` 反推：那一行会写明覆盖时段和发言人数。

折叠会让这件事更容易看走眼。十五个同学各发一句"老师辛苦了"会被压成**一行**，另外十四个人的名字挂在行尾——所以"剩 1 条"是一行，不是一个人。表头现在把这句话明说出来，因为旧措辞让模型直接答成"这个群只有一条消息"。

## 配置

环境变量：

| 变量 | 默认值 | 作用 |
| --- | --- | --- |
| `NAPCAT_URL` | `http://127.0.0.1:3000` | NapCat 的 OneBot v11 HTTP 地址。 |
| `NAPCAT_TOKEN` | *（空）* | 以 `Authorization: Bearer …` 发给 NapCat。 |
| `MCP_SECRET` | **必填** | 随机 hex，当作 URL 里的密钥路径。不填直接退出。 |
| `WATCH_GROUPS` | **必填** | 逗号分隔的群号。不填直接退出。 |
| `BIND_HOST` | `127.0.0.1` | 监听地址。留在回环上，对外暴露交给隧道。 |
| `BIND_PORT` | `8765` | 监听端口。 |
| `MCP_BEARER` | *（未设）* | 设了之后，请求还必须带上匹配的 `Authorization: Bearer` 头。 |
| `PUBLIC_HOST` | *（未设）* | 公网裸域名，会加进 DNS 重绑定白名单。由 `launch.py` 自动填；不填则只允许本机访问。 |
| `ENABLE_OCR` | `1` | 设成 `0` 就完全跳过图片 OCR。 |
| `OCR_PER_CALL` | `40` | 单次请求最多识别几张图。超出的图会静默退化成 `[图片]`。 |

只有 `launch.py` 会读的：

| 变量 | 默认值 | 作用 |
| --- | --- | --- |
| `CLOUDFLARED` | `cloudflared` | 可执行文件名或绝对路径，走 `PATH` 查找。 |
| `TUNNEL_PROTOCOL` | `http2` | 传给 `cloudflared --protocol`。UDP 7844 通的话也可以换成 `quic`。 |
| `TUNNEL_TIMEOUT` | `60` | 等隧道域名的秒数，超时就放弃。 |
| `SELFTEST` | `1` | 设 `0` 跳过启动后的公网握手自检。 |
| `SELFTEST_TIMEOUT` | `90` | 自检的超时秒数——等本地端口、重试公网请求各按这个值算。 |

### 想自己手动验一下

自检干的就是下面这一件事，你什么时候想验都可以自己发一发：

```bash
curl -i -X POST "https://<隧道域名>/mcp/<MCP_SECRET>" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"1"}}}'
```

正常的话是 `200`、`Content-Type: text/event-stream`，`data:` 那行里带着 `"serverInfo":{"name":"qq-digest",…}`。Windows 上如果 schannel 的吊销检查卡住，加个 `--ssl-no-revoke`。

`qq_digest_mcp.py` 顶部的可调常量：

| 常量 | 默认值 | 作用 |
| --- | --- | --- |
| `MAX_COUNT` | `300` | 单次最多拉多少条原始消息。 |
| `MAX_MSG_CHARS` | `400` | 单条消息截断长度。 |
| `MAX_CHAIN_CHARS` | `2000` | 接龙链最终版的截断长度。比普通消息宽，因为这一条替掉了整串冗余副本。 |
| `MIN_CHAIN_CHARS` | `60` | 前缀短于此不算接龙，避免「好」→「好的」被误判成链。 |
| `MIN_MSG_CHARS` | `2` | 短于此的正文直接丢。 |
| `OCR_MAX_CHARS` | `600` | 单张图 OCR 文本截断长度。 |
| `OCR_ROW_TOLERANCE` | `12` | 纵坐标差小于此值视为同一行。 |

## 清洗流程做了什么

原始的 OneBot 消息段在进模型之前要过好几道：

- **去噪。** 纯表情、纯标点、纯语气词（「哈」「6」「QAQ」）一律丢掉，短于 `MIN_MSG_CHARS` 的也丢。
- **去复读。** 连续重复的消息只留一条。比对用的是**未截断**的正文——先截后比会把只在 `MAX_MSG_CHARS` 之后才不同的消息误判成相同。
- **接龙折叠。** 接龙是累积型消息：每条都是上一条再加一个名字。后一条是前一条的严格前缀扩展时，整串折成**最后一条**——唯一完整的那份——并标注为「接龙链」而不是「发送相同内容」，正文归到真正发出它的人名下。实测某群：29 条消息、41 个名字，旧逻辑只呈现第 1～18 项，还附带一个错误的复读人数。
- **图片 OCR。** 群里的通知、课表、考试安排大多是图片，所以图片会走 NapCat 的 `ocr_image`（腾讯自家中文 OCR，免费、无本地依赖）。识别结果会按纵坐标**还原成行**、再按横坐标排序，表格才不会串成一行。结果按 `file_unique` 缓存，OCR 失败会被吞掉，不影响整条简报。
- **卡片解包。** 群公告（`com.tencent.mannounce`）的正文是 base64，会先解开再给模型，而不是丢一串乱码过去。其它分享退化成 `[分享:标题]`。
- **脱敏。** 手机号打码、身份证号屏蔽，`密码：xxx` / `token: xxx` 这类明文凭据在出服务端之前就被抹掉。
- **提示注入围栏。** 聊天记录用 `<<<UNTRUSTED_CHAT_CONTENT … UNTRUSTED_CHAT_CONTENT>>>` 包起来，并明确声明里面的任何句子都不是指令；工具的 docstring 里也再说一遍。

## 安全须知

- **白名单是默认拒绝的。** `WATCH_GROUPS` 为空就什么都不给读——这是故意的，防手滑全量导出。`get_group_messages` 每次调用都会重新校验白名单，模型瞎猜群号也会被挡回去。
- **端点有两层。** 密钥路径是强制的，`/mcp/<MCP_SECRET>` 以外的一律 404。如果 Spark 那边能带自定义 header，再设个 `MCP_BEARER` 当第二道。
- **脱敏只是尽力而为。** 它覆盖常见形态，不是全部。默认简报的接收方是能看到群内容的。
- 拿到隧道 URL 的人就能读你的白名单群。泄露了就换 `MCP_SECRET`（顺带换隧道）。

## 已知限制

- 每个群只能拿到最近 `count` 条，没有往前翻页的机制。
- 「@我」判定跟着 NapCat 当前登录的账号走。
- OCR 质量取决于腾讯；手写和低分辨率截图识别得比较糙。
- `cloudflared tunnel --url` 每次跑都会给一个新的随机域名。想让 Spark 里的配置固定下来，用具名隧道。
