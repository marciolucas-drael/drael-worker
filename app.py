#!/usr/bin/env python3
"""
Drael Worker — execução de missões no Render (free tier).

Fluxo: POST /jobs {title, cmd, timeout} -> {id}
  - executa em background (thread), captura stdout/stderr
  - artefatos = arquivos criados/mudados em results/<id>/artifacts/
  - no fim: upload dos artefatos p/ uguu.se + notificação ntfy
  - heartbeat: enquanto houver job rodando, faz self-ping p/ /healthz a cada
    HEARTBEAT_S (mantém a instância free viva além dos 15 min de inatividade)

GET /healthz, GET /jobs, GET /jobs/<id>, DELETE /jobs (limpa memória)
Env: NTFY_TOPIC (padrão SET_IN_RENDER_ENV), AUTH_TOKEN (opcional),
    RENDER_EXTERNAL_HOSTNAME (injetado pelo Render)
"""
import json, os, shutil, subprocess, threading, time, uuid, urllib.request
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from fastapi.responses import PlainTextResponse

app = FastAPI(title="Drael Worker", version="1.0")

WORKDIR = "/home/render/workdir" if os.path.isdir("/home/render/workdir") else os.path.expanduser("~")
RESULTS = os.path.join(WORKDIR, "results")
os.makedirs(RESULTS, exist_ok=True)
TOPIC = os.environ.get("NTFY_TOPIC", "SET_IN_RENDER_ENV")
AUTH = os.environ.get("AUTH_TOKEN", "")
HOST = os.environ.get("RENDER_EXTERNAL_HOSTNAME", "")
HEARTBEAT_S = int(os.environ.get("HEARTBEAT_S", "90"))
MAX_ART_MB = 8
MAX_ARTS = 5

JOBS: "OrderedDict[str, dict]" = OrderedDict()
LOCK = threading.Lock()
EXECUTOR = ThreadPoolExecutor(max_workers=2)


def ntfy(title, msg, prio="high"):
    try:
        req = urllib.request.Request(
            f"https://ntfy.sh/{TOPIC}", data=msg.encode("utf-8"),
            headers={"Title": title[:180], "Priority": prio, "X-Tag": "check_mark"},
            method="POST")
        urllib.request.urlopen(req, timeout=15).read()
    except Exception as e:
        print(f"[ntfy falhou] {e}", flush=True)


def snapshot(d):
    out = {}
    if os.path.isdir(d):
        for f in os.listdir(d):
            p = os.path.join(d, f)
            if os.path.isfile(p):
                st = os.stat(p)
                out[f] = (st.st_mtime_ns, st.st_size)
    return out


def upload_uguu(path):
    """Envia arquivo p/ uguu.se e retorna a URL pública (ou None)."""
    try:
        import http.client, mimetypes
        boundary = "----drael" + uuid.uuid4().hex[:16]
        fn = os.path.basename(path)
        ctype = mimetypes.guess_type(fn)[0] or "application/octet-stream"
        with open(path, "rb") as fh:
            data = fh.read()
        head = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"files[]\"; "
                f"filename=\"{fn}\"\r\nContent-Type: {ctype}\r\n\r\n").encode()
        tail = f"\r\n--{boundary}--\r\n".encode()
        body = head + data + tail
        conn = http.client.HTTPSConnection("uguu.se", timeout=120)
        conn.request("POST", "/upload", body=body,
                     headers={"Content-Type": f"multipart/form-data; boundary={boundary}",
                              "User-Agent": "Mozilla/5.0 (compatible; DraelWorker/1.0)"})
        r = conn.getresponse()
        if r.status == 200:
            return json.loads(r.read())["url"]
    except Exception as e:
        print(f"[uguu falhou {fn}] {e}", flush=True)
    return None


def run_job(job_id):
    with LOCK:
        j = JOBS[job_id]
        j["status"] = "running"
        j["started_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    rdir = os.path.join(RESULTS, job_id)
    art_dir = os.path.join(rdir, "artifacts")
    os.makedirs(art_dir, exist_ok=True)
    before = snapshot(art_dir)
    t0 = time.time()
    try:
        p = subprocess.run(["/bin/sh", "-c", j["cmd"]], cwd=rdir, capture_output=True,
                           text=True, timeout=j.get("timeout") or 3600)
        out, err, rc = p.stdout, p.stderr, p.returncode
    except subprocess.TimeoutExpired as e:
        out = (e.stdout or b"").decode("utf-8", "replace") if isinstance(e.stdout, bytes) else (e.stdout or "")
        err = (f"\n[TIMEOUT após {j.get('timeout')}s]\n"
               + ((e.stderr or b"").decode("utf-8", "replace") if isinstance(e.stderr, bytes) else (e.stderr or "")))
        rc = 124
    with open(os.path.join(rdir, "out.log"), "w") as f:
        f.write(out or "")
    now = time.time()
    artifacts = []
    for fn in sorted(set(snapshot(art_dir))):
        path = os.path.join(art_dir, fn)
        size = os.path.getsize(path)
        if before.get(fn) == (os.stat(path).st_mtime_ns, size):
            continue
        url = None
        if size <= MAX_ART_MB * 1024 * 1024:
            url = upload_uguu(path)
        artifacts.append({"name": fn, "size": size, "url": url})
        if len(artifacts) >= MAX_ARTS:
            break
    status = "done" if rc == 0 else "failed"
    with LOCK:
        j.update(status=status, rc=rc, finished_at=now, seconds=round(now - t0, 1),
                 output=(out or "")[-4000:], error=(err or "")[-2000:] or None, artifacts=artifacts)
    ntfy(f"[{status}] {j['title']}",
         f"{j['title']}\nrc={rc} em {round(now - t0, 1)}s id={job_id}\n"
         + "".join(f"- {a['name']}: {a['url']}\n" for a in artifacts if a.get("url"))
         + (f"saída: {(out or '').strip()[-300:]}" if rc != 0 else ""),
         prio="high" if status == "done" else "max")


def authed(req):
    if not AUTH:
        return True
    return req.headers.get("x-auth", "") == AUTH or req.headers.get("authorization", "") == f"Bearer {AUTH}"


class JobIn(BaseModel):
    title: str
    cmd: str
    timeout: int = 3600


def public(j):
    keep = ("id", "title", "status", "rc", "started_at", "finished_at", "seconds",
            "artifacts", "error")
    d = {k: j.get(k) for k in keep}
    if j["status"] == "running":
        d["output"] = None
    return d


@app.on_event("startup")
def _heartbeat_loop():
    def loop():
        while True:
            time.sleep(20)
            with LOCK:
                busy = [jid for jid, j in JOBS.items() if j["status"] == "running"]
            if not busy or not HOST:
                continue
            try:
                urllib.request.urlopen(f"https://{HOST}/healthz", timeout=15).read()
                print("[heartbeat] ping ok", flush=True)
            except Exception as e:
                print(f"[heartbeat falhou] {e}", flush=True)
    threading.Thread(target=loop, daemon=True).start()


@app.get("/healthz")
def healthz():
    with LOCK:
        n = sum(1 for j in JOBS.values() if j["status"] == "running")
    return {"ok": True, "busy": n, "jobs": len(JOBS),
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}


@app.post("/jobs")
def create_job(body: JobIn, request: "Request"):
    if AUTH and not (request.headers.get("x-auth") == AUTH or
                     request.headers.get("authorization") == f"Bearer {AUTH}"):
        raise HTTPException(401, "x-auth inválido")
    return _create_job(body)


def _create_job(body: JobIn):
    job_id = uuid.uuid4().hex[:10]
    j = {"id": job_id, "title": body.title[:120], "cmd": body.cmd,
         "timeout": body.timeout, "status": "pending", "artifacts": []}
    with LOCK:
        JOBS[job_id] = j
        for k in list(JOBS)[:60]:
            JOBS.pop(k, None)
    os.makedirs(os.path.join(RESULTS, job_id, "artifacts"), exist_ok=True)
    EXECUTOR.submit(run_job, job_id)
    ntfy(f"[novo] {j['title']}", f"missão enfileirada id={job_id}", prio="default")
    return {"ok": True, "id": job_id}


@app.get("/jobs")
def list_jobs():
    with LOCK:
        items = list(JOBS.values())
    return {"ok": True, "jobs": [public(j) for j in reversed(items)]}


@app.get("/jobs/{job_id}")
def get_job(job_id):
    with LOCK:
        j = JOBS.get(job_id)
    if not j:
        raise HTTPException(404, "job não encontrado (a instância pode ter reiniciado — veja o ntfy)")
    d = public(j)
    if d["status"] in ("done", "failed"):
        d["output"] = j.get("output")
    return {"ok": True, "job": d}
