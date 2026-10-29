#!/usr/bin/env python3
"""
Drael Worker — execução de missões no Render (free tier). v1.1

Fluxo: POST /jobs {title, cmd, timeout} -> {id}
  - executa em background (thread), captura stdout/stderr
  - artefatos = arquivos criados/mudados em results/<id>/artifacts/
  - no fim: upload dos artefatos p/ uguu.se + notificação ntfy
  - WAITING_HUMAN: POST /jobs/<id>/wait {reason,instruction} pausa a missão
    e notifica via ntfy; POST /jobs/<id>/resume devolve à fila
  - lease/adoption: cada execução marca lease_until (epoch); thread de
    adoption (a cada ~30s) renova o lease de threads vivas e adota 'running'
    órfãos com lease expirado (attempts < MAX_ATTEMPTS); nunca há 2 threads
    para o mesmo job_id (guarda ACTIVE)
  - restore: pending volta p/ pending (retry idempotente); running vira
    pending se attempts < MAX_ATTEMPTS, senão interrupted
  - heartbeat: self-ping p/ /healthz a cada HEARTBEAT_S enquanto houver job
    em pending|running|waiting_human (mantém instância free viva)

GET /healthz, GET /jobs, GET /jobs/<id>, DELETE /jobs (limpa memória)
Env: NTFY_TOPIC (padrão SET_IN_RENDER_ENV), AUTH_TOKEN (opcional),
    RENDER_EXTERNAL_HOSTNAME, HEARTBEAT_S (padrão 90), LEASE_S (padrão 240),
    MAX_ATTEMPTS (padrão 3)
"""
import json, os, shutil, subprocess, threading, time, uuid, urllib.request
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from fastapi.responses import PlainTextResponse

VERSION = "1.1"
app = FastAPI(title="Drael Worker", version=VERSION)

WORKDIR = "/home/render/workdir" if os.path.isdir("/home/render/workdir") else os.path.expanduser("~")
RESULTS = os.path.join(WORKDIR, "results")
os.makedirs(RESULTS, exist_ok=True)
TOPIC = os.environ.get("NTFY_TOPIC", "SET_IN_RENDER_ENV")
AUTH = os.environ.get("AUTH_TOKEN", "")
HOST = os.environ.get("RENDER_EXTERNAL_HOSTNAME", "")
HEARTBEAT_S = int(os.environ.get("HEARTBEAT_S", "90"))
LEASE_S = int(os.environ.get("LEASE_S", "240"))
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "3"))
MAX_ART_MB = 8
MAX_ARTS = 5

JOBS: "OrderedDict[str, dict]" = OrderedDict()
LOCK = threading.Lock()
ACTIVE: set = set()   # job_ids com thread de execução viva (guarda anti-duplicidade)
EXECUTOR = ThreadPoolExecutor(max_workers=2)


def _persist():
    """Grava estado dos jobs em disco (sobrevive a restart do container)."""
    try:
        state = os.path.join(WORKDIR, "jobs_state.json")
        tmp = state + ".tmp"
        with open(tmp, "w") as f:
            json.dump(list(JOBS.values()), f, default=str)
        os.replace(tmp, state)
    except Exception as e:
        print(f"[persist falhou] {e}", flush=True)


def _restore():
    """Carrega jobs persistidos ao subir.

    pending -> pending (retry idempotente, reexecuta)
    running -> pending se attempts < MAX_ATTEMPTS (adoption no boot),
               senão interrupted
    waiting_human / done / failed -> mantidos como estão
    """
    state = os.path.join(WORKDIR, "jobs_state.json")
    if not os.path.exists(state):
        return
    try:
        with open(state) as f:
            items = json.load(f)
    except Exception:
        return
    with LOCK:
        for v in items[-60:]:
            v = dict(v)
            st = v.get("status")
            if st == "running":
                # sem thread viva após restart: decide por tentativas
                if v.get("attempts", 0) < MAX_ATTEMPTS:
                    v["status"] = "pending"
                else:
                    v["status"] = "interrupted"
                    v.setdefault("error", "instância reiniciou durante a execução (tentativas esgotadas)")
            # 'pending' já está como retry idempotente; demais status não mudam
            JOBS[v["id"]] = v
    # reexecuta pendentes idempotentes depois de restaurar tudo
    with LOCK:
        pids = [k for k, v in JOBS.items() if v["status"] == "pending"]
    for k in pids:
        EXECUTOR.submit(run_job, k)
    print(f"[restore] {len(JOBS)} jobs carregados do disco ({len(pids)} reexecutados)", flush=True)


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
                              "User-Agent": "Mozilla/5.0 (compatible; DraelWorker/1.1)"})
        r = conn.getresponse()
        if r.status == 200:
            d = json.loads(r.read())
            files = d.get("files") or []
            if files and files[0].get("url"):
                return files[0]["url"]
            return d.get("url")
    except Exception as e:
        print(f"[uguu falhou {fn}] {e}", flush=True)
    return None


def start_lease(job_id):
    """Tenta iniciar execução de um job pendente: marca running + lease válido.

    Retorna True se esta chamada assumiu a execução (guarda ACTIVE garante que
    nunca há 2 threads p/ o mesmo job_id). False se já está ativo ou se o
    status mudou antes (ex.: /wait chamado na fila).
    """
    with LOCK:
        j = JOBS.get(job_id)
        if not j or job_id in ACTIVE or j.get("status") != "pending":
            return False
        ACTIVE.add(job_id)
        j.update(status="running", attempts=j.get("attempts", 0) + 1,
                 started_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                 lease_until=time.time() + LEASE_S)
        _persist()
        return True


def adoption_pass():
    """Uma rodada do loop de adoption.

    - running + thread viva (ACTIVE): renova lease_until
    - running + órfão + lease expirado + attempts < MAX_ATTEMPTS: adota (requeue)
    - running + órfão + lease expirado + tentativas esgotadas: interrupted
    """
    now = time.time()
    acted = []
    with LOCK:
        for jid, j in list(JOBS.items()):
            if j.get("status") != "running":
                continue
            if jid in ACTIVE:
                if (j.get("lease_until") or 0) <= now:
                    j["lease_until"] = now + LEASE_S   # renovação p/ thread viva
                    _persist()
                continue
            if now <= (j.get("lease_until") or 0):
                continue                                # lease ainda válido
            if j.get("attempts", 0) < MAX_ATTEMPTS:
                j["status"] = "pending"
                acted.append((jid, "adopted", j))
            else:
                j["status"] = "interrupted"
                j["error"] = "lease expirou e tentativas esgotadas"
                acted.append((jid, "interrupted", j))
        if acted:
            _persist()
    for jid, what, j in acted:
        print(f"[adoption] {jid} {what} (tentativas={j.get('attempts', 0)}/{MAX_ATTEMPTS})", flush=True)
        ntfy(f"[{what}] {j['title']}",
             f"job {jid} {what} (tentativas {j.get('attempts', 0)}/{MAX_ATTEMPTS})",
             prio="default")
        if what == "adopted":
            EXECUTOR.submit(run_job, jid)


def run_job(job_id):
    try:
        if not start_lease(job_id):
            return
        rdir = os.path.join(RESULTS, job_id)
        art_dir = os.path.join(rdir, "artifacts")
        os.makedirs(art_dir, exist_ok=True)
        before = snapshot(art_dir)
        t0 = time.time()
        with LOCK:
            j = JOBS[job_id]
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
            j = JOBS[job_id]
            # finalize só se ainda está rodando ou foi pausado durante a execução
            if j.get("status") not in ("running", "waiting_human"):
                return
            j.update(status=status, rc=rc, finished_at=now, seconds=round(now - t0, 1),
                     output=(out or "")[-4000:], error=(err or "")[-2000:] or None,
                     artifacts=artifacts, lease_until=None)
            _persist()
        ntfy(f"[{status}] {j['title']}",
             f"{j['title']}\nrc={rc} em {round(now - t0, 1)}s id={job_id}\n"
             + "".join(f"- {a['name']}: {a['url']}\n" for a in artifacts if a.get("url"))
             + (f"saída: {(out or '').strip()[-300:]}" if rc != 0 else ""),
             prio="high" if status == "done" else "max")
    finally:
        with LOCK:
            ACTIVE.discard(job_id)


def authed(req):
    if not AUTH:
        return True
    return req.headers.get("x-auth", "") == AUTH or req.headers.get("authorization", "") == f"Bearer {AUTH}"


# restauração no boot, depois que todas as funções de execução existem
_restore()


class JobIn(BaseModel):
    title: str
    cmd: str
    timeout: int = 3600


class WaitIn(BaseModel):
    reason: str = ""
    instruction: str = ""


def public(j):
    keep = ("id", "title", "status", "rc", "started_at", "finished_at", "seconds",
            "artifacts", "error", "reason", "instruction", "attempts")
    d = {k: j.get(k) for k in keep}
    if d["status"] == "running":
        d["output"] = None
    return d


@app.on_event("startup")
def _bg_threads():
    def hb_loop():
        """Keep-alive inteligente: ping enquanto houver job pend|run|waiting."""
        while True:
            time.sleep(HEARTBEAT_S)
            with LOCK:
                live = [jid for jid, j in JOBS.items()
                        if j["status"] in ("pending", "running", "waiting_human")]
            if not live or not HOST:
                continue
            try:
                urllib.request.urlopen(f"https://{HOST}/healthz", timeout=15).read()
                print(f"[heartbeat] ping ok ({len(live)} jobs ativos)", flush=True)
            except Exception as e:
                print(f"[heartbeat falhou] {e}", flush=True)

    def ad_loop():
        while True:
            time.sleep(30)
            try:
                adoption_pass()
            except Exception as e:
                print(f"[adoption falhou] {e}", flush=True)

    threading.Thread(target=hb_loop, daemon=True).start()
    threading.Thread(target=ad_loop, daemon=True).start()


@app.get("/healthz")
def healthz():
    with LOCK:
        busy = sum(1 for j in JOBS.values() if j["status"] == "running")
        total = len(JOBS)
    return {"ok": True, "version": VERSION, "busy": busy, "jobs": total,
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}


@app.post("/jobs")
def create_job(body: JobIn, request: "Request"):
    if AUTH and not authed(request):
        raise HTTPException(401, "x-auth inválido")
    return _create_job(body)


def _create_job(body: JobIn):
    job_id = uuid.uuid4().hex[:10]
    j = {"id": job_id, "title": body.title[:120], "cmd": body.cmd,
         "timeout": body.timeout, "status": "pending", "artifacts": [],
         "reason": None, "instruction": None, "attempts": 0, "lease_until": None}
    rdir = os.path.join(RESULTS, job_id)
    os.makedirs(os.path.join(rdir, "artifacts"), exist_ok=True)
    with LOCK:
        JOBS[job_id] = j
        finished = [k for k, v in JOBS.items() if v.get("status") in ("done", "failed")]
        for k in finished[50:]:   # nunca evictar pending/running/waiting_human
            JOBS.pop(k, None)
        _persist()
    EXECUTOR.submit(run_job, job_id)
    ntfy(f"[novo] {j['title']}", f"missão enfileirada id={job_id}", prio="default")
    return {"ok": True, "id": job_id}


@app.post("/jobs/{job_id}/wait")
def wait_job(job_id, body: WaitIn, request: "Request"):
    """WAITING_HUMAN: pausa a missão e notifica via ntfy com a instrução."""
    if AUTH and not authed(request):
        raise HTTPException(401, "x-auth inválido")
    with LOCK:
        j = JOBS.get(job_id)
        if not j:
            raise HTTPException(404, "job não encontrado")
        if j["status"] not in ("pending", "running"):
            raise HTTPException(409, f"não dá p/ pausar job em {j['status']}")
        j.update(status="waiting_human", reason=body.reason[:300],
                 instruction=body.instruction[:500])
        _persist()
        title = j["title"]
    ntfy(f"[WAITING_HUMAN] {title}",
         f"missão pausada aguardando humano\nid={job_id}\nmotivo: {body.reason}\n"
         f"instrução: {body.instruction}\nresuma com: POST /jobs/{job_id}/resume",
         prio="max")
    return {"ok": True, "id": job_id, "status": "waiting_human"}


@app.post("/jobs/{job_id}/resume")
def resume_job(job_id, request: "Request"):
    """Resumir job WAITING_HUMAN: volta a 'pending' e reentra na fila."""
    if AUTH and not authed(request):
        raise HTTPException(401, "x-auth inválido")
    with LOCK:
        j = JOBS.get(job_id)
        if not j:
            raise HTTPException(404, "job não encontrado")
        if j["status"] != "waiting_human":
            raise HTTPException(409, f"resumir vale só p/ waiting_human (agora: {j['status']})")
        j.update(status="pending", reason=None, instruction=None)
        _persist()
        title = j["title"]
    EXECUTOR.submit(run_job, job_id)
    ntfy(f"[resumed] {title}", f"job {job_id} voltou à fila", prio="default")
    return {"ok": True, "id": job_id, "status": "pending"}


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
