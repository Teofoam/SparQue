"""
launch.py — 起隧道 + 起服务，一步到位

cloudflared 的临时隧道每次跑都换一个随机域名，手抄一遍再拼 URL 太蠢。
这个脚本先把隧道拉起来，从它的日志里抓出域名，塞进 PUBLIC_HOST，
再带着这个环境变量启动 qq_digest_mcp.py——服务端自己会把完整地址打出来。

注意 PUBLIC_HOST 要的是**裸域名**（xxx.trycloudflare.com），不带 https://。
服务端拿它去填 SDK 的 allowed_hosts，那里比对的是 Host 头，
带上协议头就永远匹配不上，请求会被挡成 421 Invalid Host header。

用法
  python launch.py

服务起来之后会自检一次：从公网地址发一个 MCP initialize，确认隧道真的通、
Host 头过得了白名单、握手能成。本地日志再好看也证明不了这几件事。

环境变量（都有默认值，一般不用管）
  CLOUDFLARED       cloudflared 可执行文件，默认从 PATH 找
  TUNNEL_PROTOCOL   默认 http2。QUIC 走 UDP/7844，校园网和公司网经常封，
                    http2 走 443 更容易活下来。
  TUNNEL_TIMEOUT    等隧道域名的秒数，默认 60
  SELFTEST          设 0 关掉自检
  SELFTEST_TIMEOUT  自检的超时，默认 90 秒（等本地服务、等隧道生效各一份）
"""

from __future__ import annotations

import json
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
from urllib.parse import urlsplit

import httpx

HERE = os.path.dirname(os.path.abspath(__file__))


def say(msg: str, err: bool = False) -> None:
    """
    一律 flush。输出重定向到文件时 print 是块缓冲的，
    而子进程（服务端）是自己往同一个句柄写——不 flush 的话日志顺序会乱成一团，
    甚至看起来像"隧道没起来"，其实只是那行还压在缓冲区里。
    """
    print(msg, file=sys.stderr if err else sys.stdout, flush=True)


BIND_HOST = os.environ.get("BIND_HOST", "127.0.0.1")
BIND_PORT = os.environ.get("BIND_PORT", "8765")
LOCAL_URL = f"http://{BIND_HOST}:{BIND_PORT}"

CLOUDFLARED = os.environ.get("CLOUDFLARED", "cloudflared")
TUNNEL_PROTOCOL = os.environ.get("TUNNEL_PROTOCOL", "http2")
TUNNEL_TIMEOUT = float(os.environ.get("TUNNEL_TIMEOUT", "60"))

SELFTEST = os.environ.get("SELFTEST", "1") == "1"
# 90 秒是量出来的，不是拍的：临时隧道刚建好时边缘节点还没认这个域名，
# 这段时间 TLS 握手直接被掐（SSL UNEXPECTED_EOF）。实测 30 秒经常不够。
SELFTEST_TIMEOUT = float(os.environ.get("SELFTEST_TIMEOUT", "90"))

_URL_RE = re.compile(r"https://[-a-z0-9]+\.trycloudflare\.com")


# ---------------------------------------------------------------- 子进程托管
#
# finally 里的 terminate() 只在正常退出和 Ctrl+C 时管用。要是本进程被 /F 强杀、
# 或者直接崩了，cloudflared 和服务端就变成孤儿——端口还占着，隧道还连着，
# 下次启动只会看到"端口被占用"，得手动去任务管理器捞。
#
# Windows 上的解法是 Job Object：把子进程都塞进一个带 KILL_ON_JOB_CLOSE 的 job。
# 句柄随进程消亡而关闭，内核顺手把 job 里所有进程一起收掉——这条路径不经过
# 我们的代码，所以杀得多难看都跑得掉。

_JOB = None


def _create_job():
    """建一个"句柄一关就把成员全杀掉"的 job。失败就返回 None，不影响主流程。"""
    if sys.platform != "win32":
        return None

    import ctypes
    from ctypes import wintypes

    class IO_COUNTERS(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_ulonglong),
            ("WriteOperationCount", ctypes.c_ulonglong),
            ("OtherOperationCount", ctypes.c_ulonglong),
            ("ReadTransferCount", ctypes.c_ulonglong),
            ("WriteTransferCount", ctypes.c_ulonglong),
            ("OtherTransferCount", ctypes.c_ulonglong),
        ]

    class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", wintypes.LARGE_INTEGER),
            ("PerJobUserTimeLimit", wintypes.LARGE_INTEGER),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
            ("IoInfo", IO_COUNTERS),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    JobObjectExtendedLimitInformation = 9
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000

    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    k32.CreateJobObjectW.restype = wintypes.HANDLE

    job = k32.CreateJobObjectW(None, None)
    if not job:
        return None

    info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
    info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    ok = k32.SetInformationJobObject(
        job,
        JobObjectExtendedLimitInformation,
        ctypes.byref(info),
        ctypes.sizeof(info),
    )
    if not ok:
        k32.CloseHandle(job)
        return None

    return job


def adopt(proc: subprocess.Popen) -> None:
    """把子进程挂进 job。挂不上不算致命——退化成原来的 finally 清理。"""
    if _JOB is None:
        return

    import ctypes
    from ctypes import wintypes

    PROCESS_SET_QUOTA = 0x0100
    PROCESS_TERMINATE = 0x0001

    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    k32.OpenProcess.restype = wintypes.HANDLE

    handle = k32.OpenProcess(PROCESS_SET_QUOTA | PROCESS_TERMINATE, False, proc.pid)
    if not handle:
        return
    try:
        if not k32.AssignProcessToJobObject(_JOB, handle):
            say(f"[警告] PID {proc.pid} 没挂进 job，强杀本进程时它可能变成孤儿。", err=True)
    finally:
        k32.CloseHandle(handle)


def _pump(stream, found: threading.Event, holder: list[str]) -> None:
    """
    盯着 cloudflared 的日志找域名。
    它把所有日志都写 stderr（包括那个 ASCII 方框里的 URL），所以只读 stderr 就够。
    抓到之后继续读，避免管道写满把 cloudflared 卡死——但只把报错转出来，不刷屏。
    """
    for line in stream:
        line = line.rstrip()
        if not found.is_set():
            m = _URL_RE.search(line)
            if m:
                holder.append(m.group())
                found.set()
                continue
        # 按 cloudflared 的日志级别过滤。之前用 "failed" 做关键字，结果把
        # QUIC 探测失败那几行 INF 也捞上来了——用 http2 的时候 QUIC 失败是预期的，
        # 报出来只会吓人。
        if " ERR " in line or " FTL " in line:
            say(f"[cloudflared] {line}", err=True)


def start_tunnel() -> tuple[subprocess.Popen, str]:
    exe = shutil.which(CLOUDFLARED)
    if not exe:
        sys.exit(
            f"找不到 cloudflared（试的是 {CLOUDFLARED!r}）。"
            "装一个，或者用 CLOUDFLARED 环境变量指到它的绝对路径。"
        )

    cmd = [exe, "tunnel", "--url", LOCAL_URL, "--protocol", TUNNEL_PROTOCOL]
    say(f"起隧道: {' '.join(cmd)}")

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )
    adopt(proc)

    found = threading.Event()
    holder: list[str] = []
    threading.Thread(target=_pump, args=(proc.stderr, found, holder), daemon=True).start()

    if not found.wait(TUNNEL_TIMEOUT):
        proc.terminate()
        if proc.poll() is not None:
            sys.exit(f"cloudflared 自己退了（exit {proc.returncode}），上面应该有报错。")
        sys.exit(f"等了 {TUNNEL_TIMEOUT:.0f} 秒没等到隧道域名，网络可能不通。")

    return proc, holder[0]


def _port_taken() -> bool:
    with socket.socket() as s:
        s.settimeout(1.0)
        return s.connect_ex((BIND_HOST, int(BIND_PORT))) == 0


def _parse_sse(body: str) -> dict:
    """streamable HTTP 的回包是 SSE，真正的 JSON 在 data: 那一行。"""
    for line in body.splitlines():
        if line.startswith("data:"):
            try:
                return json.loads(line[5:].strip())
            except json.JSONDecodeError:
                return {}
    return {}


def selftest(public_host: str) -> None:
    """
    从公网打一发 initialize。

    本地那几行日志只能说明进程活着，说明不了隧道通没通、Host 头有没有被拦。
    真从外面走一趟，才知道 Spark 待会儿能不能连上。
    """
    local_deadline = time.monotonic() + SELFTEST_TIMEOUT
    while not _port_taken():
        if time.monotonic() > local_deadline:
            say("[自检] 本地服务一直没起来，跳过自检。", err=True)
            return
        time.sleep(0.5)

    url = f"https://{public_host}/mcp/{os.environ['MCP_SECRET']}"
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "launch.py-selftest", "version": "1"},
        },
    }
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }

    # 隧道刚建好的头几秒，边缘节点还没认这个域名，这时候打过去会是
    # SSL EOF 或者 502/530。不是配置错，纯粹是没就绪，重试几次就好。
    deadline = time.monotonic() + SELFTEST_TIMEOUT
    resp = None
    hiccup = ""
    while time.monotonic() < deadline:
        try:
            resp = httpx.post(url, json=payload, headers=headers, timeout=20.0)
        except httpx.HTTPError as exc:
            hiccup = f"{type(exc).__name__}: {exc}"
            time.sleep(2.0)
            continue
        if resp.status_code in (502, 503, 530):
            hiccup = f"HTTP {resp.status_code}"
            resp = None
            time.sleep(2.0)
            continue
        break

    if resp is None:
        # 实测边缘生效通常 5~10 秒，但偶尔会长到超过 90 秒。所以这里超时
        # 并不代表配错了，服务也还好好跑着——说清楚，别让人以为要重来一遍。
        say(
            f"[自检] {SELFTEST_TIMEOUT:.0f} 秒内没连通隧道，最后一次: {hiccup}\n"
            "        服务本身在跑，隧道多半只是还没生效。过一会儿手动验一下：\n"
            f"        curl -i --ssl-no-revoke -X POST {url} \\\n"
            '          -H "Content-Type: application/json" \\\n'
            '          -H "Accept: application/json, text/event-stream" \\\n'
            '          -d \'{"jsonrpc":"2.0","id":1,"method":"initialize",'
            '"params":{"protocolVersion":"2025-06-18","capabilities":{},'
            '"clientInfo":{"name":"curl","version":"1"}}}\'',
            err=True,
        )
        return

    if resp.status_code == 421:
        say(
            "[自检] 421 Invalid Host header —— PUBLIC_HOST 跟 Host 头对不上。\n"
            f"        当前 PUBLIC_HOST={public_host!r}，确认没带 https:// 或末尾斜杠。",
            err=True,
        )
        return
    if resp.status_code == 404:
        say("[自检] 404 —— 密钥路径不对，检查 MCP_SECRET。", err=True)
        return
    if resp.status_code != 200:
        say(f"[自检] HTTP {resp.status_code}: {resp.text[:200]}", err=True)
        return

    info = _parse_sse(resp.text).get("result", {}).get("serverInfo", {})
    name = info.get("name")
    if name:
        say(f"[自检] ✅ 隧道通了，握手成功（{name} {info.get('version', '')}".rstrip() + "）")
    else:
        say(f"[自检] 200 了但没解出 serverInfo: {resp.text[:200]}", err=True)


def main() -> None:
    # job 要在起任何子进程之前建好，而且句柄得一直拿在手里：
    # 一旦被 GC 掉，句柄关闭，里面的进程当场全灭。
    global _JOB
    _JOB = _create_job()

    # 下面这些都在起隧道之前查。等隧道起来了再报错，那条隧道就白开了，
    # 而且每开一次域名就换一次，Spark 里的地址还得跟着改。
    for var in ("MCP_SECRET", "WATCH_GROUPS"):
        if not os.environ.get(var):
            sys.exit(f"请先设置 {var}（run.bat 里那几行）。")

    if _port_taken():
        sys.exit(
            f"{BIND_HOST}:{BIND_PORT} 已经被占用了——多半是上一个实例还开着。\n"
            "把那个窗口关掉，或者换个 BIND_PORT 再来。"
        )

    tunnel, tunnel_url = start_tunnel()

    # allowed_hosts 比对的是 Host 头，所以这里只要 netloc，把 https:// 去掉
    public_host = urlsplit(tunnel_url).netloc
    say(f"隧道域名: {public_host}")

    env = os.environ.copy()
    env["PUBLIC_HOST"] = public_host

    try:
        server = subprocess.Popen([sys.executable, "qq_digest_mcp.py"], cwd=HERE, env=env)
        adopt(server)
        if SELFTEST:
            threading.Thread(target=selftest, args=(public_host,), daemon=True).start()
        server.wait()
    except KeyboardInterrupt:
        pass          # Ctrl+C 是正常的退出方式，不用打一堆栈
    finally:
        tunnel.terminate()
        try:
            tunnel.wait(timeout=10)
        except subprocess.TimeoutExpired:
            tunnel.kill()
        say("\n隧道已关闭。下次启动会换一个新域名，记得回 Spark 里改地址。")


if __name__ == "__main__":
    main()
