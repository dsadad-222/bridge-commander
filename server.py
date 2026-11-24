"""
Bridge Commander v2 — AI 代理链路指挥中心
独立进程，只读轮询，绝不干扰现有链路
"""
import asyncio, json, os, re, subprocess, sys, time, sqlite3
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

BASE = Path(__file__).parent
DB_PATH = BASE / "data" / "history.db"
ZEN = "http://127.0.0.1:18790"

# ── 模型元信息 ──────────────────────────────────────────────
MODEL_META = {
    "deepseek-v4-flash-free":   {"type": "chat",  "tag": "主力", "desc": "284B MoE，支持工具调用",          "color": "#c4954f"},
    "ling-3.0-flash-free":      {"type": "chat",  "tag": "备用", "desc": "轻量快速，日常对话",                "color": "#7ea87e"},
    "ling-3.0-tiny-free":       {"type": "chat",  "tag": "轻量", "desc": "极速推理，简单场景",                "color": "#6b9eb4"},
    "mimo-v2.5-free":           {"type": "chat",  "tag": "推理", "desc": "慢思考深度推理",                    "color": "#a46fb4"},
    "nemotron-3-ultra-free":    {"type": "chat",  "tag": "综合", "desc": "NVIDIA Nemotron，综合能力强",      "color": "#7eb47e"},
    "north-mini-code-free":     {"type": "code",  "tag": "编程", "desc": "代码专项，补全 / 生成",             "color": "#b46f6f"},
    "laguna-s-2.1-free":        {"type": "chat",  "tag": "通用", "desc": "通用对话，均衡稳定",                "color": "#7e8eb4"},
    "longcat-2.0-free":         {"type": "chat",  "tag": "长文", "desc": "超长上下文，文档 / 分析",           "color": "#b49f6f"},
}

# ── 数据库 ──────────────────────────────────────────────────
def init_db():
    db = sqlite3.connect(str(DB_PATH))
    db.execute("""CREATE TABLE IF NOT EXISTS snapshots (
        ts INTEGER PRIMARY KEY, zen_ok INTEGER, cl_ok INTEGER,
        zen_req_total INTEGER, zen_req_ok INTEGER, zen_req_fail INTEGER,
        zen_fallback INTEGER, zen_model TEXT, zen_uptime TEXT,
        cl_req_total INTEGER, cl_req_ok INTEGER, cl_req_fail INTEGER, cl_uptime TEXT
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY, value TEXT
    )""")
    # 默认设置
    for k, v in [("poll_interval", "5"), ("alert_fallback_threshold", "3"),
                 ("alert_latency_threshold", "10"), ("theme", "dark"),
                 ("daily_baseline_date", ""), ("daily_baseline_total", "0"), ("daily_baseline_fallback", "0")]:
        db.execute("INSERT OR IGNORE INTO settings VALUES (?,?)", (k, v))
    db.commit()
    return db

init_db()

state = {
    "zen": {"ok": False, "uptime": "", "model": "", "req_total": 0, "req_ok": 0, "req_fail": 0, "fallback": 0},
    "cl": {"ok": False, "uptime": "", "req_total": 0, "req_ok": 0, "req_fail": 0},
    "watchdog": {"alive": False, "restarts_zen": 0, "restarts_cl": 0, "heartbeat": ""},
    "models": [],
    "clients": {},
    "today": {"requests": 0, "fallbacks": 0, "peak_qps": 0.0},
    "events": [],
    "logs": [],
    "quotas": {"daily_limit": 100, "used": 0, "remaining": 100, "pct": 0}
}

subscribers: list[asyncio.Queue] = []

async def broadcast():
    for q in subscribers:
        try: q.put_nowait(json.dumps(state, ensure_ascii=False, default=str))
        except: pass

async def fetch_json(client, url, timeout=3):
    try:
        r = await client.get(url, timeout=timeout)
        return r.json() if r.status_code == 200 else None
    except: return None

def read_log_tail(path, lines=100):
    try:
        p = Path(path)
        if not p.exists(): return []
        return p.read_text(encoding="utf-8", errors="replace").strip().split("\n")[-lines:]
    except: return []

def parse_watchdog(log_text: str):
    result = {"alive": False, "restarts_zen": 0, "restarts_cl": 0, "heartbeat": ""}
    if not log_text: return result
    for line in log_text.split("\n"):
        if "Zen RESTARTED" in line or "Zen PORT STOLEN" in line or "Zen RECLAIMED" in line:
            result["restarts_zen"] += 1
        if "Claudian RESTARTED" in line:
            result["restarts_cl"] += 1
        if "Heartbeat:" in line:
            result["heartbeat"] = line[:19]
    try:
        last = log_text.split("\n")[-1]
        if len(last) > 19:
            ts = datetime.strptime(last[:19], "%Y-%m-%d %H:%M:%S")
            if (datetime.now() - ts).total_seconds() < 15:
                result["alive"] = True
    except: pass
    return result

async def check_clients():
    """检测四路客户端配置是否正确"""
    clients = {}
    # Obsidian: Claudian Bridge 配置
    try:
        op = Path("D:/ObsidianVault/.obsidian/plugins/claudian/data.json")
        if op.exists():
            d = json.loads(op.read_text())
            clients["obsidian"] = {"name": "Obsidian · Claudian", "ok": True,
                "target": d.get("baseUrl", d.get("base_url", "?")),
                "model": d.get("model", "?")}
        else:
            clients["obsidian"] = {"name": "Obsidian · Claudian", "ok": False, "target": "未找到配置"}
    except: clients["obsidian"] = {"name": "Obsidian · Claudian", "ok": False, "target": "配置读取失败"}

    # Codex
    try:
        cf = Path("C:/Users/y7000/.codex/config.toml")
        if cf.exists():
            txt = cf.read_text()
            base = re.search(r'base_url\s*=\s*"(.+?)"', txt)
            model = re.search(r'model\s*=\s*"(.+?)"', txt)
            ok = bool(base and "18790" in base.group(1))
            clients["codex"] = {"name": "Codex", "ok": ok,
                "target": base.group(1) if base else "?",
                "model": model.group(1) if model else "?"}
        else:
            clients["codex"] = {"name": "Codex", "ok": False, "target": "未找到 config.toml"}
    except: clients["codex"] = {"name": "Codex", "ok": False, "target": "配置读取失败"}

    # OpenClaw: 检查配置
    try:
        oc_paths = [Path("C:/Users/y7000/.openclaw/openclaw.json"), Path("C:/Users/y7000/openclaw")]
        found = False; model = "?"; base = "?"
        for p in oc_paths:
            if p.exists():
                found = True; break
        if found:
            try:
                cfg = json.loads(Path("C:/Users/y7000/.openclaw/openclaw.json").read_text())
                providers = cfg.get("models", {}).get("providers", {})
                # 取第一个 provider 的信息
                for pname, pcfg in providers.items():
                    for m in pcfg.get("models", []):
                        model = m.get("id", m) if isinstance(m, dict) else m
                    base = pcfg.get("baseURL", pcfg.get("baseUrl", "http://127.0.0.1:18790/v1"))
                    break
            except: pass
            clients["openclaw"] = {"name": "OpenClaw", "ok": True, "target": base, "model": model}
        else:
            clients["openclaw"] = {"name": "OpenClaw", "ok": False, "target": "目录未找到"}
    except: clients["openclaw"] = {"name": "OpenClaw", "ok": False, "target": "检测失败"}

    # Hermes: 真实路径 D:\hermes-env，配置在 D:\hermes-appdata 或 C:\Users\y7000\.hermes
    try:
        hm_paths = [Path("D:/hermes-env"), Path("C:/Users/y7000/.hermes"), Path("D:/hermes-appdata")]
        found = False; model = "?"; base = "?"
        for p in hm_paths:
            if p.exists():
                found = True; break
        if found:
            try:
                cfg = Path("D:/hermes-appdata/config.yaml")
                if cfg.exists():
                    txt = cfg.read_text(encoding="utf-8")
                    m = re.search(r'default:\s*"?(.+?)"?\s*$', txt, re.MULTILINE)
                    b = re.search(r'base_url:\s*"?(.+?)"?\s*$', txt, re.MULTILINE)
                    if m: model = m.group(1)
                    if b: base = b.group(1)
            except: pass
            clients["hermes"] = {"name": "Hermes", "ok": True, "target": base if base != "?" else "http://127.0.0.1:18790/v1", "model": model}
        else:
            clients["hermes"] = {"name": "Hermes", "ok": False, "target": "目录未找到"}
    except: clients["hermes"] = {"name": "Hermes", "ok": False, "target": "检测失败"}

    return clients

async def collect():
    async with httpx.AsyncClient() as client:
        zh = await fetch_json(client, f"{ZEN}/health")
        zs = await fetch_json(client, f"{ZEN}/stats")
        zm = await fetch_json(client, f"{ZEN}/v1/models")
        if zh:
            state["zen"]["ok"] = zh.get("status") == "ok"
            state["zen"]["uptime"] = zh.get("uptime", "")
            state["zen"]["model"] = zh.get("model", zh.get("upstream", ""))
        else:
            state["zen"]["ok"] = False
        if zs:
            state["zen"]["req_total"] = zs.get("total_requests", 0)
            state["zen"]["req_ok"] = zs.get("success_count", 0)
            state["zen"]["req_fail"] = zs.get("error_count", 0)
            state["zen"]["fallback"] = zs.get("fallback_count", zs.get("fallbacks", 0))
        if zm:
            models_list = zm.get("data", zm.get("models", zm))
            if isinstance(models_list, list):
                state["models"] = [m.get("id", str(m)) for m in models_list if isinstance(m, dict)]

        ch = await fetch_json(client, "http://127.0.0.1:15722/health")
        cs = await fetch_json(client, "http://127.0.0.1:15722/stats")
        state["cl"]["ok"] = ch and ch.get("status") == "ok"
        if ch: state["cl"]["uptime"] = ch.get("uptime", "")
        if cs:
            state["cl"]["req_total"] = cs.get("total_requests", 0)
            state["cl"]["req_ok"] = cs.get("successful", 0)
            state["cl"]["req_fail"] = cs.get("failed", 0)

        wlog = "\n".join(read_log_tail("D:/claudian-bridge/watchdog.log", 100))
        state["watchdog"] = parse_watchdog(wlog)

        # 客户端
        state["clients"] = await check_clients()

        # 配额：基于每日基线差值计算，避免用累计值
        today_str = datetime.now().strftime("%Y-%m-%d")
        db = sqlite3.connect(str(DB_PATH))
        # 读取基线
        cur = db.execute("SELECT key, value FROM settings WHERE key IN ('daily_baseline_date','daily_baseline_total','daily_baseline_fallback')")
        settings_rows = {r[0]: r[1] for r in cur.fetchall()}
        baseline_date = settings_rows.get("daily_baseline_date", "")
        baseline_total = int(settings_rows.get("daily_baseline_total", "0"))
        baseline_fallback = int(settings_rows.get("daily_baseline_fallback", "0"))
        # 换天了？重置基线
        if baseline_date != today_str:
            baseline_total = state["zen"]["req_total"]
            baseline_fallback = state["zen"]["fallback"]
            db.execute("UPDATE settings SET value=? WHERE key='daily_baseline_date'", (today_str,))
            db.execute("UPDATE settings SET value=? WHERE key='daily_baseline_total'", (str(baseline_total),))
            db.execute("UPDATE settings SET value=? WHERE key='daily_baseline_fallback'", (str(baseline_fallback),))
            db.commit()
        # 今日用量 = 当前值 - 基线
        today_total = max(0, state["zen"]["req_total"] - baseline_total)
        today_fb = max(0, state["zen"]["fallback"] - baseline_fallback)
        state["today"] = {"requests": today_total, "fallbacks": today_fb, "peak_qps": 0.0}
        db.close()
        used = state["today"]["requests"]
        state["quotas"] = {"daily_limit": 100, "used": used, "remaining": max(0, 100 - used),
                           "pct": min(100, round(used / 100 * 100, 1))}

        # 事件
        state["events"] = []
        if state["watchdog"]["restarts_zen"] > 0:
            state["events"].append({"type": "warn", "text": f"Zen Bridge 今日自动恢复 {state['watchdog']['restarts_zen']} 次"})
        if state["watchdog"]["restarts_cl"] > 0:
            state["events"].append({"type": "warn", "text": f"Claudian Bridge 今日自动恢复 {state['watchdog']['restarts_cl']} 次"})
        if state["zen"]["fallback"] > 0:
            state["events"].append({"type": "warn", "text": f"模型 fallback 触发 {state['zen']['fallback']} 次"})
        # 客户端检查
        for cid, c in state["clients"].items():
            if not c["ok"]:
                state["events"].append({"type": "error", "text": f"{c['name']} 配置异常: {c['target']}"})
        if state["quotas"]["pct"] >= 90:
            state["events"].append({"type": "warn", "text": f"今日配额已用 {state['quotas']['pct']}%，接近上限"})

        # 聚合日志
        logs = []
        for line in read_log_tail("D:/op-zen-bridge/proxy.log", 20):
            if line.strip(): logs.append({"source": "zen", "text": line.strip()})
        for line in read_log_tail("D:/claudian-bridge/proxy_debug.log", 20):
            if line.strip(): logs.append({"source": "claudian", "text": line.strip()})
        state["logs"] = logs[-40:]

        await broadcast()

async def snapshot_loop():
    while True:
        await collect()
        db = sqlite3.connect(str(DB_PATH))
        db.execute("""INSERT INTO snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", (
            int(time.time()),
            int(state["zen"]["ok"]), int(state["cl"]["ok"]),
            state["zen"]["req_total"], state["zen"]["req_ok"], state["zen"]["req_fail"],
            state["zen"]["fallback"], state["zen"]["model"], state["zen"]["uptime"],
            state["cl"]["req_total"], state["cl"]["req_ok"], state["cl"]["req_fail"],
            state["cl"]["uptime"]
        ))
        db.commit()
        db.close()
        await asyncio.sleep(5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(snapshot_loop())
    yield
    task.cancel()

app = FastAPI(title="Bridge Commander v2", lifespan=lifespan)

# 禁用静态文件缓存，避免浏览器加载旧版 HTML/JS
@app.middleware("http")
async def no_cache_static(request: Request, call_next):
    resp = await call_next(request)
    if request.url.path == "/" or request.url.path.startswith("/static/"):
        resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
    return resp

app.mount("/static", StaticFiles(directory=str(BASE / "static")), name="static")

@app.get("/", response_class=HTMLResponse)
async def index():
    return (BASE / "static" / "index.html").read_text(encoding="utf-8")

@app.get("/test_bench", response_class=HTMLResponse)
async def test_bench():
    return Path("C:/Users/y7000/AppData/Roaming/Tencent/Marvis/User/oAN1i2XmkxTiB_NgEI2WeaJzJkTs/workspace/conv_19f13ce98c9_4f3707fd3634/temp/bench_test.html").read_text(encoding="utf-8")

@app.get("/diag", response_class=HTMLResponse)
async def diag():
    return Path("C:/Users/y7000/AppData/Roaming/Tencent/Marvis/User/oAN1i2XmkxTiB_NgEI2WeaJzJkTs/workspace/conv_19f13ce98c9_4f3707fd3634/temp/diag.html").read_text(encoding="utf-8")

@app.get("/api/stream")
async def stream():
    q: asyncio.Queue = asyncio.Queue()
    subscribers.append(q)
    async def gen():
        try:
            q.put_nowait(json.dumps(state, ensure_ascii=False, default=str))
            while True:
                data = await q.get()
                yield f"data: {data}\n\n"
        except asyncio.CancelledError: pass
        finally:
            if q in subscribers: subscribers.remove(q)
    return StreamingResponse(gen(), media_type="text/event-stream")

@app.get("/api/status")
async def api_status(): return state

@app.get("/api/history")
async def api_history(minutes: int = 60):
    since = int(time.time()) - minutes * 60
    db = sqlite3.connect(str(DB_PATH))
    rows = db.execute("SELECT * FROM snapshots WHERE ts >= ? ORDER BY ts", (since,)).fetchall()
    db.close()
    return [{"ts": r[0], "zen_ok": r[1], "cl_ok": r[2], "zen_req": r[3], "cl_req": r[9]} for r in rows]

@app.get("/api/logs/{source}")
async def api_logs(source: str):
    paths = {"zen": "D:/op-zen-bridge/proxy.log", "claudian": "D:/claudian-bridge/proxy_debug.log",
             "watchdog": "D:/claudian-bridge/watchdog.log"}
    path = paths.get(source)
    if not path: return {"lines": []}
    return {"lines": read_log_tail(path, 200)}

@app.post("/api/restart/{service}")
async def api_restart(service: str):
    cmds = {
        "zen": ["pythonw", "D:/op-zen-bridge/proxy.py"],
        "claudian": ["pythonw", "D:/claudian-bridge/proxy.py"],
        "watchdog": ["powershell", "-NoProfile", "-WindowStyle", "Hidden", "-File", "D:/claudian-bridge/watchdog.ps1"]
    }
    if service not in cmds: return {"ok": False, "error": "unknown"}
    subprocess.Popen(cmds[service], creationflags=0x08000000 if sys.platform == "win32" else 0)
    return {"ok": True, "service": service}

@app.get("/api/test/{model_name}")
async def api_test(model_name: str):
    t0 = time.time()
    try:
        async with httpx.AsyncClient() as c:
            r = await c.post(f"{ZEN}/v1/chat/completions",
                json={"model": model_name, "messages": [{"role": "user", "content": "say hi"}], "max_tokens": 5},
                headers={"Authorization": "Bearer sk-free"}, timeout=45)
            elapsed = round(time.time() - t0, 2)
            j = r.json()
            choices = j.get("choices") or []
            content = ""
            if choices:
                msg = choices[0].get("message") or {}
                content = (msg.get("content") or "")[:50]
            return {"ok": r.status_code == 200, "status": r.status_code, "latency": elapsed, "model": model_name,
                    "response": content}
    except Exception as e:
        return {"ok": False, "error": str(e), "model": model_name, "latency": round(time.time() - t0, 2)}

@app.get("/api/test-all")
async def api_test_all():
    """流式测速：逐个返回结果，避免前端超时等待"""
    async def gen():
        results = []
        async def test_one(model):
            r = await api_test(model)
            results.append(r)
            return r
        tasks = [test_one(m) for m in MODEL_META]
        for done in asyncio.as_completed(tasks):
            r = await done
            yield f"data: {json.dumps(r, ensure_ascii=False)}\n\n"
        yield f"data: [DONE]\n\n"
    return StreamingResponse(gen(), media_type="text/event-stream")

@app.get("/api/diagnose")
async def api_diagnose():
    """全链路诊断"""
    d = {"steps": [], "pass": 0, "fail": 0, "total": 0, "healthy": False}
    try:
        async with httpx.AsyncClient() as c:
            # Step 1: Zen health
            try:
                r = await fetch_json(c, f"{ZEN}/health")
                ok = bool(r and r.get("status") == "ok")
                d["steps"].append({"name": "Zen Bridge 健康检查", "ok": ok, "detail": r.get("uptime","") if r else "无响应", "dur":"—"})
            except: d["steps"].append({"name": "Zen Bridge 健康检查", "ok": False, "detail": "连接失败", "dur":"—"})

            # Step 2: Zen models
            try:
                r = await fetch_json(c, f"{ZEN}/v1/models")
                mcount = len(r.get("data",[])) if r else 0
                d["steps"].append({"name": "模型列表", "ok": mcount>0, "detail": f"{mcount} 个模型可用", "dur":"—"})
            except: d["steps"].append({"name": "模型列表", "ok": False, "detail": "获取失败", "dur":"—"})

            # Step 3: DeepSeek 实测
            try:
                t0 = time.time()
                r = await c.post(f"{ZEN}/v1/chat/completions",
                    json={"model":"deepseek-v4-flash-free","messages":[{"role":"user","content":"say hi"}],"max_tokens":5},
                    headers={"Authorization":"Bearer sk-free"}, timeout=45)
                dur = round(time.time()-t0, 2)
                ok = r.status_code==200
                d["steps"].append({"name":"DeepSeek 推理实测","ok":ok,"detail":f"{dur}s" if ok else f"HTTP {r.status_code}","dur":f"{dur}s"})
            except Exception as e:
                d["steps"].append({"name":"DeepSeek 推理实测","ok":False,"detail":f"{type(e).__name__}","dur":"—"})

            # Step 4: Claudian
            try:
                r = await fetch_json(c, "http://127.0.0.1:15722/health")
                ok = bool(r and r.get("status")=="ok")
                d["steps"].append({"name":"Claudian Bridge","ok":ok,"detail":r.get("uptime","") if r else "无响应","dur":"—"})
            except: d["steps"].append({"name":"Claudian Bridge","ok":False,"detail":"连接失败","dur":"—"})

            # Step 5: Watchdog
            try:
                wlog = "\n".join(read_log_tail("D:/claudian-bridge/watchdog.log", 50))
                wd = parse_watchdog(wlog)
                d["steps"].append({"name":"守护进程","ok":wd["alive"],"detail":wd["heartbeat"] or "未检测到","dur":"—"})
            except: d["steps"].append({"name":"守护进程","ok":False,"detail":"读取失败","dur":"—"})

            # Step 6: Clients
            try:
                clients = await check_clients()
                for cid, cc in clients.items():
                    d["steps"].append({"name":cc["name"],"ok":cc["ok"],
                        "detail":f"{cc.get('model','?')} @ {cc.get('target','?')}".strip(" ?@"),
                        "dur":"—"})
            except: d["steps"].append({"name":"客户端检测","ok":False,"detail":"检测异常","dur":"—"})
    except Exception as e:
        d["steps"].append({"name":"诊断引擎","ok":False,"detail":str(e)[:60],"dur":"—"})

    d["total"] = len(d["steps"])
    d["pass"] = sum(1 for s in d["steps"] if s["ok"])
    d["fail"] = d["total"] - d["pass"]
    d["healthy"] = d["fail"] == 0
    return d

@app.get("/api/models")
async def api_models():
    """返回模型列表含元信息"""
    result = []
    for mid in state["models"]:
        meta = MODEL_META.get(mid, {"type": "unknown", "tag": "", "desc": "", "color": "#888"})
        result.append({"id": mid, **meta, "active": mid == state["zen"]["model"]})
    return result

@app.get("/api/settings")
async def get_settings():
    db = sqlite3.connect(str(DB_PATH))
    rows = db.execute("SELECT key, value FROM settings").fetchall()
    db.close()
    return {r[0]: r[1] for r in rows}

@app.post("/api/settings")
async def update_settings(data: dict):
    db = sqlite3.connect(str(DB_PATH))
    for k, v in data.items():
        db.execute("INSERT OR REPLACE INTO settings VALUES (?,?)", (k, str(v)))
    db.commit()
    db.close()
    return {"ok": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=9999, log_level="warning")
