#!/usr/bin/env python3
"""
EXP-04 — braço Orquestrador Híbrido (topologia inteligente, N=9 serial).

1) Pré-pull de python:3.11-slim nos 3 workers (cache inicial equalizado)
2) Cold Start OBRIGATÓRIO: limpa tasks + task_results (zera warm_count)
3) Remove estresse residual do EXP-02 (não misturar noisy neighbor)
4) Submete lote intercalado Leve / Fog / ML × 3 via POST /submit_task
5) Após cada planejamento, pin da imagem para python:3.11-slim (lab)
6) Aguarda status terminal de cada tarefa antes da próxima
7) Exporta JSON + matriz preferred × allocated em results/metrics_export/exp4/hibrido/

Uso:
  python scripts/run_exp4_hybrid.py
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sqlite3
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    print("Instale requests: pip install requests", file=sys.stderr)
    raise SystemExit(1)

ROOT = Path(__file__).resolve().parents[1]
EVAL = ROOT / "scripts" / "eval_tasks"
DEFAULT_DB = ROOT / "data" / "nodes.db"
TERMINAL = frozenset({"concluida", "falha", "cancelada", "erro_agente", "falha_alocacao"})
LAYER_LABEL = "tcc.utfpr.edu.br/compute-layer"
_LAYER_ORDINAL = {"edge": 0, "fog": 1, "cloud": 2}
BASELINE_HIT_RATE = 0.2222
STRESS_POD = "exp2-noisy-neighbor"
PIN_IMAGE = "python:3.11-slim"
KIND_WORKERS = (
    "tcc-cluster-worker",
    "tcc-cluster-worker2",
    "tcc-cluster-worker3",
)

PROFILES = {
    "edge": (EVAL / "task_edge_light.py", "edge"),
    "fog": (EVAL / "task_fog_cpu.py", "fog"),
    "cloud": (EVAL / "task_cloud_ml.py", "cloud"),
}
SEQUENCE = ["edge", "fog", "cloud"] * 3

LAYER_RE = re.compile(
    r"\[TCC-METRICS\]\[LAYER\]\s+preferred=(\S+)\s+chosen=(\S+)\s+"
    r"hostname=(\S+)\s+score_layer=([\d.]+)\s+score_total=([\d.]+)"
)


def _connect(db: Path) -> sqlite3.Connection:
    c = sqlite3.connect(str(db), timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA busy_timeout=8000")
    return c


def kubectl(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["kubectl", *args], check=check, text=True, capture_output=True)


def prepull_runtime_image(image: str = PIN_IMAGE) -> dict:
    """Equaliza o cache inicial de python:3.11-slim nos três workers Kind."""
    results: dict[str, dict] = {}
    for host in KIND_WORKERS:
        r = subprocess.run(
            ["docker", "exec", host, "crictl", "pull", image],
            capture_output=True,
            text=True,
            check=False,
            timeout=300,
        )
        ok = r.returncode == 0
        err = (r.stderr or r.stdout or "").strip()[-300:]
        results[host] = {"ok": ok, "detail": err}
        print(f"  pre-pull {image} @ {host}: {'ok' if ok else 'FAIL'} {err}", flush=True)
        if not ok:
            raise RuntimeError(f"pré-pull falhou em {host}: {err}")
    return results


def cold_start_reset(db: Path, keep_uploads: bool = False) -> dict:
    """Limpa tasks/task_results — zera warm_count (25% imagem) de experimentos anteriores."""
    if not db.is_file():
        raise FileNotFoundError(f"DB não encontrado: {db}")

    backup = db.with_name(f"{db.stem}.bak_{int(time.time())}{db.suffix}")
    shutil.copy2(db, backup)
    print(f"Cold start backup: {backup}", flush=True)

    conn = sqlite3.connect(str(db), timeout=30)
    try:
        conn.execute("PRAGMA busy_timeout=8000")
        cur = conn.cursor()
        n_tasks = cur.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        n_res = cur.execute("SELECT COUNT(*) FROM task_results").fetchone()[0]
        cur.execute("DELETE FROM task_results")
        cur.execute("DELETE FROM tasks")
        try:
            cur.execute(
                "UPDATE nodes SET cpu_reserved_threads=0, memory_reserved_gb=0, disk_reserved_gb=0"
            )
        except sqlite3.Error:
            pass
        conn.commit()
        print(f"Cold start: removidos {n_tasks} tasks, {n_res} results", flush=True)
    finally:
        conn.close()

    removed_uploads = 0
    if not keep_uploads:
        up = ROOT / "uploaded_tasks"
        if up.is_dir():
            for p in up.iterdir():
                if p.is_file() and not p.name.startswith("."):
                    p.unlink(missing_ok=True)
                    removed_uploads += 1
            print(f"Cold start: uploads removidos={removed_uploads}", flush=True)

    return {"n_tasks": n_tasks, "n_results": n_res, "uploads_removed": removed_uploads, "backup": str(backup)}


def neutralize_tensorflow_catalog(db: Path) -> dict:
    """Lab EXP-04: remove tensorflow/tensorflow:latest do catálogo e mapeia libs TF para slim."""
    conn = sqlite3.connect(str(db), timeout=30)
    try:
        conn.execute("PRAGMA busy_timeout=8000")
        row = conn.execute(
            "SELECT libraries FROM docker_images_catalog WHERE image_name=?",
            (PIN_IMAGE,),
        ).fetchone()
        slim_libs: list[str] = []
        if row and row[0]:
            raw = row[0]
            try:
                parsed = json.loads(raw) if isinstance(raw, str) else raw
                if isinstance(parsed, list):
                    slim_libs = [str(x) for x in parsed]
            except (json.JSONDecodeError, TypeError):
                slim_libs = []
        for extra in ("tensorflow", "keras", "sklearn", "scikit-learn"):
            if extra not in slim_libs:
                slim_libs.append(extra)
        conn.execute(
            "UPDATE docker_images_catalog SET libraries=? WHERE image_name=?",
            (json.dumps(slim_libs), PIN_IMAGE),
        )
        cur = conn.execute(
            "DELETE FROM docker_images_catalog WHERE image_name=?",
            ("tensorflow/tensorflow:latest",),
        )
        conn.commit()
        return {"deleted_tf_rows": cur.rowcount, "slim_libraries": slim_libs}
    finally:
        conn.close()


def cleanup_exp2_stress() -> None:
    """EXP-04 é serial/topologia — não pode herdar o noisy neighbor do EXP-02."""
    for ns in ("tcc-baseline", "default"):
        kubectl(
            ["delete", "pod", STRESS_POD, "-n", ns, "--ignore-not-found", "--wait=true"],
            check=False,
        )


def snapshot_node_layers() -> list[dict]:
    r = kubectl(["get", "nodes", "-o", "json"], check=False)
    out: list[dict] = []
    if r.returncode != 0:
        return out
    try:
        items = (json.loads(r.stdout).get("items")) or []
    except json.JSONDecodeError:
        return out
    for n in items:
        meta = n.get("metadata") or {}
        name = meta.get("name")
        labels = meta.get("labels") or {}
        out.append(
            {
                "hostname": name,
                "compute_layer": labels.get(LAYER_LABEL),
                "is_control_plane": "node-role.kubernetes.io/control-plane" in labels,
            }
        )
    return out


def snapshot_db_nodes(db: Path) -> list[dict]:
    conn = _connect(db)
    try:
        rows = conn.execute(
            "SELECT node_id, hostname, compute_layer, is_active, provider_type, "
            "cpu_used_percent, memory_used_percent FROM nodes "
            "WHERE provider_type='kubernetes'"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def submit(api: str, script: Path, user_id: str) -> str:
    url = f"{api.rstrip('/')}/submit_task"
    with script.open("rb") as f:
        r = requests.post(
            url,
            data={"userId": user_id, "taskType": "auto"},
            files={"file": (script.name, f, "text/x-python")},
            timeout=60,
        )
    r.raise_for_status()
    body = r.json()
    tid = body.get("task_id") or body.get("taskId")
    if not tid:
        raise RuntimeError(f"submit sem task_id: {body}")
    return str(tid)


def fetch_task(db: Path, task_id: str) -> dict | None:
    conn = _connect(db)
    try:
        row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        res = conn.execute(
            "SELECT status_final, duration_seconds, cpu_peak_percent, ram_peak_mb, "
            "prediction_delta_ram, detected_libraries FROM task_results WHERE task_id=?",
            (task_id,),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    out = dict(row)
    if res:
        out["result"] = dict(res)
    return out


def _spec_image_from_row(row: dict) -> str | None:
    raw = row.get("ai_raw_response") or ""
    try:
        data = json.loads(raw) if str(raw).strip() else {}
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    spec = data.get("k8s_job_spec") if isinstance(data.get("k8s_job_spec"), dict) else {}
    img = spec.get("image")
    return str(img).strip() if img else None


def _job_name_for_task(task_id: str) -> str:
    compact = task_id.replace("-", "")[:40]
    s = re.sub(r"[^a-z0-9-]+", "-", f"task-job-{compact}".lower()).strip("-")
    return s[:63].rstrip("-") or "job"


def _effective_image(row: dict) -> str:
    return (_spec_image_from_row(row) or row.get("docker_image") or "").strip()


def wait_queued(db: Path, task_id: str, timeout_s: float = 600.0, poll_s: float = 0.5) -> dict:
    """Espera o planejamento terminar. Se a fila já despachou com slim, segue sem pin."""
    deadline = time.time() + timeout_s
    last: dict = {}
    while time.time() < deadline:
        row = fetch_task(db, task_id)
        if row:
            last = row
            status = row.get("status")
            img = _effective_image(row)
            if status == "na_fila":
                return row
            if img == PIN_IMAGE and status in {"executando", *TERMINAL}:
                return row
            # Fila despachou antes do pin (planejamento->despacho < poll): devolve para
            # pin_runtime_image, que apaga o Job e reenfileira com a imagem padronizada.
            if status == "executando" and img != PIN_IMAGE:
                return row
            if status in TERMINAL and img != PIN_IMAGE:
                raise RuntimeError(
                    f"{task_id} terminou com imagem {img!r} antes do pin"
                )
        time.sleep(poll_s)
    raise TimeoutError(
        f"Timeout aguardando na_fila de {task_id} (último status={last.get('status')})"
    )


def pin_runtime_image(db: Path, task_id: str, image: str = PIN_IMAGE) -> dict:
    """Força python:3.11-slim pós-planejamento (lab), para equalizar runtime com a baseline."""
    conn = _connect(db)
    try:
        row = conn.execute(
            "SELECT status, docker_image, ai_raw_response, allocated_node_id "
            "FROM tasks WHERE task_id=?",
            (task_id,),
        ).fetchone()
        if not row:
            raise RuntimeError(f"task {task_id} não encontrada para pin de imagem")
        status = row["status"]
        raw = row["ai_raw_response"] or "{}"
        try:
            data = json.loads(raw) if str(raw).strip() else {}
        except json.JSONDecodeError:
            data = {}
        if not isinstance(data, dict):
            data = {}
        spec = data.get("k8s_job_spec") if isinstance(data.get("k8s_job_spec"), dict) else {}
        spec = dict(spec)
        before_spec = spec.get("image")
        current = str(before_spec or row["docker_image"] or "").strip()
        if current == image and str(row["docker_image"] or "").strip() == image:
            return {
                "pinned_image": image,
                "skipped": True,
                "docker_image_before": row["docker_image"],
                "k8s_spec_image_before": before_spec,
                "status_before": status,
            }

        if row["allocated_node_id"] or status == "executando":
            job_name = _job_name_for_task(task_id)
            kubectl(
                ["delete", "job", job_name, "--ignore-not-found", "--wait=true"],
                check=False,
            )
            kubectl(
                ["delete", "pods", "-l", f"task-id={task_id}", "--ignore-not-found", "--wait=false"],
                check=False,
            )

        spec["image"] = image
        data["k8s_job_spec"] = spec
        data["exp4_lab_image_pin"] = {
            "image": image,
            "docker_image_before": row["docker_image"],
            "k8s_spec_image_before": before_spec,
            "pinned_at": datetime.now(timezone.utc).isoformat(),
            "recovered_dispatch": bool(row["allocated_node_id"] or status == "executando"),
            "note": "Imagem única pós-planejamento para equalizar cache/runtime com a baseline",
        }

        trace = data.get("multi_agent_trace")
        if isinstance(trace, dict):
            summ = trace.get("strategist_summary")
            if isinstance(summ, dict):
                sspec = summ.get("k8s_job_spec") if isinstance(summ.get("k8s_job_spec"), dict) else {}
                sspec = dict(sspec)
                sspec["image"] = image
                summ["k8s_job_spec"] = sspec
                trace["strategist_summary"] = summ
                data["multi_agent_trace"] = trace

        conn.execute(
            "UPDATE tasks SET docker_image=?, ai_raw_response=?, status='na_fila', "
            "allocated_node_id=NULL WHERE task_id=?",
            (image, json.dumps(data, ensure_ascii=False), task_id),
        )
        conn.commit()
        return {
            "pinned_image": image,
            "skipped": False,
            "docker_image_before": row["docker_image"],
            "k8s_spec_image_before": before_spec,
            "status_before": status,
            "recovered_dispatch": bool(row["allocated_node_id"] or status == "executando"),
        }
    finally:
        conn.close()


def kubectl_job_phase(task_id: str) -> dict:
    """Lê o Job no cluster — evita martelar o SQLite no bind-mount Windows/Docker."""
    name = _job_name_for_task(task_id)
    r = kubectl(["get", "job", name, "-o", "json"], check=False)
    if r.returncode != 0 or not (r.stdout or "").strip():
        return {"exists": False, "complete": False, "failed": False, "name": name}
    try:
        data = json.loads(r.stdout)
    except json.JSONDecodeError:
        return {"exists": False, "complete": False, "failed": False, "name": name}
    st = data.get("status") if isinstance(data.get("status"), dict) else {}
    conds = {
        str(c.get("type")): str(c.get("status"))
        for c in (st.get("conditions") or [])
        if isinstance(c, dict)
    }
    return {
        "exists": True,
        "complete": conds.get("Complete") == "True" or int(st.get("succeeded") or 0) >= 1,
        "failed": conds.get("Failed") == "True",
        "name": name,
    }


def wait_done(db: Path, task_id: str, timeout_s: float, poll_s: float = 8.0) -> dict:
    """Espera término no SQLite; se o watcher falhar com disk I/O, aceita o Job Complete."""
    deadline = time.time() + timeout_s
    last: dict = {}
    last_db = 0.0
    while time.time() < deadline:
        job = kubectl_job_phase(task_id)
        now = time.time()
        should_db = (now - last_db) >= 15.0 or job.get("complete") or job.get("failed")
        if should_db:
            row = fetch_task(db, task_id)
            last_db = now
            if row:
                last = row
                status = row.get("status")
                result_st = (row.get("result") or {}).get("status_final")
                print(
                    f"  [{datetime.now().strftime('%H:%M:%S')}] {task_id[:8]}… "
                    f"status={status} k8s={'Complete' if job.get('complete') else ('Failed' if job.get('failed') else ('Job' if job.get('exists') else '—'))} "
                    f"pref={row.get('preferred_compute_layer')} "
                    f"alloc={row.get('allocated_compute_layer')} "
                    f"node={row.get('allocated_node_id') or '—'}",
                    flush=True,
                )
                if status in TERMINAL:
                    return row
                if result_st in TERMINAL:
                    row["status"] = result_st
                    row["status_recovered_from_result"] = True
                    return row
        if job.get("complete") or job.get("failed"):
            inferred = "concluida" if job.get("complete") else "falha"
            recovered = dict(last) if last else {"task_id": task_id}
            recovered["status"] = inferred
            recovered["status_recovered_from_k8s"] = True
            print(
                f"  recover k8s Job {job.get('name')} -> {inferred} "
                "(SQLite não fechou o status; placement já gravado)",
                flush=True,
            )
            return recovered
        time.sleep(poll_s)
    raise TimeoutError(f"Timeout aguardando {task_id} (último status={last.get('status')})")


def docker_logs_since(since_s: float) -> str:
    """Recorta logs recentes do broker (segundos — ISO no Docker Desktop/Windows é frágil)."""
    blob = ""
    window = max(int(since_s) + 5, 15)
    try:
        r = subprocess.run(
            ["docker", "logs", "--since", f"{window}s", "context-server"],
            capture_output=True,
            check=False,
            timeout=30,
        )
        blob = (r.stdout or b"").decode("utf-8", errors="replace") + (
            r.stderr or b""
        ).decode("utf-8", errors="replace")
    except Exception:
        return ""
    return blob


def parse_layer_logs(blob: str) -> list[dict]:
    hits: list[dict] = []
    for line in blob.splitlines():
        m = LAYER_RE.search(line)
        if not m:
            continue
        hits.append(
            {
                "preferred": m.group(1),
                "chosen": m.group(2),
                "hostname": m.group(3),
                "score_layer": float(m.group(4)),
                "score_total": float(m.group(5)),
                "raw": line.rstrip(),
            }
        )
    return hits


def scrape_schedinspector_lines(blob: str) -> list[str]:
    out: list[str] = []
    for line in blob.splitlines():
        if "[TCC-METRICS][SCHEDINSPECTOR]" in line or "[TCC-METRICS][LAYER]" in line:
            out.append(line.rstrip())
    return out[-20:]


def extract_json_blob(ai_raw: str | None) -> dict:
    if not ai_raw:
        return {}
    try:
        data = json.loads(ai_raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    return data if isinstance(data, dict) else {}


def extract_analyst(data: dict) -> dict:
    trace = data.get("multi_agent_trace") or {}
    analyst = trace.get("analyst_output") or data.get("analyst_output") or {}
    return analyst if isinstance(analyst, dict) else {}


def extract_ranker_winner(data: dict) -> dict:
    trace = data.get("multi_agent_trace") or {}
    nr = trace.get("node_ranker") or data.get("node_ranker") or {}
    if not isinstance(nr, dict):
        return {}
    top = nr.get("top_nodes") or []
    winner = top[0] if top and isinstance(top[0], dict) else {}
    req = nr.get("task_requirements") if isinstance(nr.get("task_requirements"), dict) else {}
    return {
        "preferred_layer": req.get("preferred_layer"),
        "compute_layer": winner.get("compute_layer"),
        "hostname": winner.get("k8s_hostname") or winner.get("hostname"),
        "score_layer": winner.get("score_layer"),
        "score_total": winner.get("score_total"),
        "score_image_affinity": winner.get("score_image_affinity"),
        "score_resources": winner.get("score_resources"),
        "score_disk": winner.get("score_disk"),
        "warm_task_count": winner.get("warm_task_count"),
        "top_nodes": [
            {
                "hostname": n.get("k8s_hostname") or n.get("hostname"),
                "compute_layer": n.get("compute_layer"),
                "score_total": n.get("score_total"),
                "score_layer": n.get("score_layer"),
                "score_image_affinity": n.get("score_image_affinity"),
                "score_resources": n.get("score_resources"),
                "warm_task_count": n.get("warm_task_count"),
            }
            for n in top
            if isinstance(n, dict)
        ],
    }


def hostname_from_allocated(node_id: str | None) -> str | None:
    if not node_id:
        return None
    host = str(node_id)
    if host.startswith("k8s:"):
        host = host[4:]
    return host or None


def layer_distance(a: str | None, b: str | None) -> int | None:
    if a not in _LAYER_ORDINAL or b not in _LAYER_ORDINAL:
        return None
    return abs(_LAYER_ORDINAL[a] - _LAYER_ORDINAL[b])


def confusion_matrix(rows: list[dict], pref_key: str, alloc_key: str) -> dict[str, dict[str, int]]:
    layers = ["edge", "fog", "cloud"]
    matrix = {pref: {alloc: 0 for alloc in layers} for pref in layers}
    for row in rows:
        pref = row.get(pref_key)
        alloc = row.get(alloc_key)
        if pref in matrix and alloc in matrix[pref]:
            matrix[pref][alloc] += 1
    return matrix


def print_matrix(cm: dict[str, dict[str, int]], title: str) -> None:
    print(f"  {title}")
    print(f"  {'':10} {'edge':>6} {'fog':>6} {'cloud':>6}")
    for pref in ("edge", "fog", "cloud"):
        row = cm[pref]
        print(f"  {pref:10} {row['edge']:6d} {row['fog']:6d} {row['cloud']:6d}")


def ascii_safe(s: str) -> str:
    return s.encode("ascii", "replace").decode("ascii")


def main() -> int:
    p = argparse.ArgumentParser(description="EXP-04 híbrido: topologia inteligente N=9 serial")
    p.add_argument("--api", default="http://127.0.0.1:5000")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--user", default="lsouza")
    p.add_argument("--timeout", type=float, default=600.0)
    p.add_argument("--gap", type=float, default=8.0, help="Pausa entre runs (s)")
    p.add_argument("--keep-uploads", action="store_true")
    args = p.parse_args()
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from _runlog import start_run_log
    start_run_log("exp4", "hybrid")

    for profile, (script, _) in PROFILES.items():
        if not script.is_file():
            print(f"Script não encontrado ({profile}): {script}", file=sys.stderr)
            return 1
    if not args.db.is_file():
        print(f"DB não encontrado: {args.db}", file=sys.stderr)
        return 1

    print("=" * 64)
    print("EXP-04 — Orquestrador Híbrido (topologia inteligente)")
    print(f"API={args.api}  N={len(SEQUENCE)}  ordem={SEQUENCE}")
    print("=" * 64)

    print("\n[0/5] Preflight: limpar estresse EXP-02 + snapshot de labels...", flush=True)
    cleanup_exp2_stress()
    node_layers = snapshot_node_layers()
    db_nodes = snapshot_db_nodes(args.db)
    print("  kubectl nodes -L compute-layer:", flush=True)
    for n in node_layers:
        role = "control-plane" if n.get("is_control_plane") else "worker"
        print(f"    {n.get('hostname')}: layer={n.get('compute_layer')} ({role})", flush=True)

    print(f"\n[1/5] Pré-pull {PIN_IMAGE} nos 3 workers (cache inicial)...", flush=True)
    prepull = prepull_runtime_image(PIN_IMAGE)

    print("\n[2/5] Cold Start SQLite (tasks + task_results) — OBRIGATÓRIO...", flush=True)
    reset_info = cold_start_reset(args.db, keep_uploads=args.keep_uploads)
    catalog_pin = neutralize_tensorflow_catalog(args.db)
    reset_info["catalog_pin"] = catalog_pin
    print(f"  catálogo TF redirecionado para {PIN_IMAGE}: {catalog_pin}", flush=True)
    time.sleep(3.0)

    print("\n[3/5] Submetendo lote intercalado via /submit_task (serial)...", flush=True)
    series: list[dict] = []
    all_layer_logs: list[dict] = []

    for i, profile in enumerate(SEQUENCE, start=1):
        script, preferred_regra = PROFILES[profile]
        print(
            f"\n--- Run {i}/{len(SEQUENCE)} profile={profile} preferred_regra={preferred_regra} "
            f"script={script.name} ---",
            flush=True,
        )
        t_run0 = time.monotonic()
        t0 = t_run0
        try:
            tid = submit(args.api, script, args.user)
        except Exception as e:
            print(f"submit erro/parse: {e}")
            conn = _connect(args.db)
            try:
                row = conn.execute(
                    "SELECT task_id FROM tasks WHERE user_id=? ORDER BY submitted_at DESC LIMIT 1",
                    (args.user,),
                ).fetchone()
                tid = row["task_id"] if row else None
            finally:
                conn.close()
            if not tid:
                print("Não foi possível obter task_id", file=sys.stderr)
                return 1
            print(f"  task_id inferido: {tid}")
        else:
            print(f"  submetida: {tid}", flush=True)

        try:
            wait_queued(args.db, tid, args.timeout)
            queued = fetch_task(args.db, tid) or {}
            if _effective_image(queued) == PIN_IMAGE:
                pin = {
                    "pinned_image": PIN_IMAGE,
                    "skipped": True,
                    "docker_image_before": queued.get("docker_image"),
                    "status_before": queued.get("status"),
                }
                print(
                    f"  skip pin (já {PIN_IMAGE}) status={queued.get('status')}",
                    flush=True,
                )
            else:
                pin = pin_runtime_image(args.db, tid, PIN_IMAGE)
                skipped = "skip" if pin.get("skipped") else "pin"
                recovered = " recovered" if pin.get("recovered_dispatch") else ""
                print(
                    f"  {skipped} imagem: {pin.get('docker_image_before')} -> {pin.get('pinned_image')}{recovered}",
                    flush=True,
                )
        except (TimeoutError, RuntimeError) as e:
            print(f"  ERRO no pin de imagem: {e}", file=sys.stderr)
            return 1

        try:
            row = wait_done(args.db, tid, args.timeout)
        except TimeoutError as e:
            print(f"  ERRO: {e}", file=sys.stderr)
            series.append(
                {
                    "run": i,
                    "profile": profile,
                    "script": script.name,
                    "task_id": tid,
                    "preferred_regra": preferred_regra,
                    "status": "Timeout",
                    "duration_wall_s": round(time.monotonic() - t0, 3),
                    "at": datetime.now(timezone.utc).isoformat(),
                }
            )
            break

        wall = time.monotonic() - t0
        data = extract_json_blob(row.get("ai_raw_response"))
        analyst = extract_analyst(data)
        ranker = extract_ranker_winner(data)
        blob = docker_logs_since(time.monotonic() - t_run0)
        layer_hits = parse_layer_logs(blob)
        sched_lines = scrape_schedinspector_lines(blob)
        # O re-ranking da fila é o último [LAYER] da janela desta run.
        layer_dispatch = layer_hits[-1] if layer_hits else None
        if layer_dispatch:
            all_layer_logs.append({"run": i, "task_id": tid, **layer_dispatch})

        preferred_ia = row.get("preferred_compute_layer") or ranker.get("preferred_layer")
        allocated = row.get("allocated_compute_layer")
        host = hostname_from_allocated(row.get("allocated_node_id"))
        dist_ia = layer_distance(preferred_ia, allocated)
        dist_regra = layer_distance(preferred_regra, allocated)
        score_layer = None
        if layer_dispatch:
            score_layer = layer_dispatch.get("score_layer")
        elif ranker.get("score_layer") is not None:
            score_layer = ranker.get("score_layer")

        result = row.get("result") or {}
        entry = {
            "run": i,
            "profile": profile,
            "script": script.name,
            "task_id": tid,
            "status": row.get("status"),
            "docker_image": row.get("docker_image"),
            "image_pin": pin,
            "preferred_regra": preferred_regra,
            "preferred_ia": preferred_ia,
            "preferred_compute_layer": preferred_ia,
            "allocated_compute_layer": allocated,
            "allocated_node_id": row.get("allocated_node_id"),
            "hostname": host,
            "hit_exact": bool(preferred_ia and allocated and preferred_ia == allocated),
            "hit_exact_vs_regra": bool(allocated == preferred_regra),
            "hit_adjacent": dist_ia is not None and dist_ia <= 1,
            "layer_distance": dist_ia,
            "layer_distance_vs_regra": dist_regra,
            "score_layer": score_layer,
            "score_total": (layer_dispatch or {}).get("score_total") or ranker.get("score_total"),
            "score_image_affinity": ranker.get("score_image_affinity"),
            "warm_task_count": ranker.get("warm_task_count"),
            "ranker_winner": {k: v for k, v in ranker.items() if k != "top_nodes"},
            "ranker_top_nodes": ranker.get("top_nodes"),
            "analyst_task_type": analyst.get("task_type"),
            "analyst_complexity_score": analyst.get("complexity_score"),
            "layer_log": layer_dispatch,
            "layer_log_all_in_window": layer_hits,
            "schedinspector_lines": [ascii_safe(ln) for ln in sched_lines[-8:]],
            "result": result,
            "ram_peak_mb": result.get("ram_peak_mb"),
            "cpu_peak_percent": result.get("cpu_peak_percent"),
            "duration_wall_s": round(wall, 3),
            "at": datetime.now(timezone.utc).isoformat(),
        }
        series.append(entry)
        print(
            f"  RESULTADO: status={entry['status']} pref_ia={preferred_ia} "
            f"alloc={allocated} node={host} hit_exact={entry['hit_exact']} "
            f"score_layer={score_layer} warm={entry['warm_task_count']} "
            f"img={entry['docker_image']} wall_s={wall:.1f}",
            flush=True,
        )
        if i < len(SEQUENCE):
            time.sleep(args.gap)

    n = len(series)
    hits = sum(1 for s in series if s.get("hit_exact"))
    hits_regra = sum(1 for s in series if s.get("hit_exact_vs_regra"))
    hits_adj = sum(1 for s in series if s.get("hit_adjacent"))
    hit_rate = hits / n if n else 0.0
    hit_rate_regra = hits_regra / n if n else 0.0
    hit_rate_adj = hits_adj / n if n else 0.0
    cm_ia = confusion_matrix(series, "preferred_ia", "allocated_compute_layer")
    cm_regra = confusion_matrix(series, "preferred_regra", "allocated_compute_layer")

    hit_by_pref: dict[str, dict] = {}
    for pref in ("edge", "fog", "cloud"):
        rows = [s for s in series if s.get("preferred_ia") == pref]
        h = sum(1 for s in rows if s.get("hit_exact"))
        hit_by_pref[pref] = {
            "n": len(rows),
            "hits": h,
            "hit_rate": (h / len(rows) if rows else 0.0),
        }

    ml_on_cloud = sum(
        1
        for s in series
        if s.get("profile") == "cloud" and s.get("allocated_compute_layer") == "cloud"
    )
    light_on_edge = sum(
        1
        for s in series
        if s.get("profile") == "edge" and s.get("allocated_compute_layer") == "edge"
    )
    light_on_cloud = sum(
        1
        for s in series
        if s.get("profile") == "edge" and s.get("allocated_compute_layer") == "cloud"
    )
    ml_on_edge = sum(
        1
        for s in series
        if s.get("profile") == "cloud" and s.get("allocated_compute_layer") == "edge"
    )
    beat_baseline = hit_rate > BASELINE_HIT_RATE

    report = {
        "experiment": "exp4",
        "arm": "hybrid",
        "n": n,
        "sequence": SEQUENCE,
        "cold_start": reset_info,
        "runtime_image": PIN_IMAGE,
        "prepull": prepull,
        "note_image_control": (
            "Imagem única python:3.11-slim nos 9 jobs após pin pós-planejamento; "
            "pré-pull nos 3 workers. Fator residual: w_i (warm) após a 1ª execução."
        ),
        "cluster_node_layers": node_layers,
        "db_nodes_at_start": db_nodes,
        "hits": hits,
        "hit_rate": round(hit_rate, 4),
        "hits_vs_regra": hits_regra,
        "hit_rate_vs_regra": round(hit_rate_regra, 4),
        "hits_adjacent": hits_adj,
        "hit_rate_adjacent": round(hit_rate_adj, 4),
        "baseline_hit_rate": BASELINE_HIT_RATE,
        "beat_baseline": beat_baseline,
        "hit_by_preferred_ia": hit_by_pref,
        "confusion_matrix_preferred_ia_vs_allocated": cm_ia,
        "confusion_matrix_preferred_regra_vs_allocated": cm_regra,
        "allocated_layer_distribution": dict(
            Counter(s.get("allocated_compute_layer") or "unknown" for s in series)
        ),
        "profile_counts": dict(Counter(s.get("profile") for s in series)),
        "ml_on_cloud": ml_on_cloud,
        "light_on_edge": light_on_edge,
        "light_on_cloud_opposite": light_on_cloud,
        "ml_on_edge_opposite": ml_on_edge,
        "layer_logs": all_layer_logs,
        "runs": series,
        "at": datetime.now(timezone.utc).isoformat(),
    }

    out = (
        ROOT
        / "results"
        / "metrics_export"
        / "exp4"
        / "hibrido"
        / f"exp4_hybrid_{int(time.time())}.json"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n" + "=" * 64)
    print("RESUMO EXP-04 HIBRIDO")
    print(f"  Hit rate exato (IA vs allocated): {hits}/{n} = {hit_rate:.1%}")
    print(f"  Hit rate vs regra offline:        {hits_regra}/{n} = {hit_rate_regra:.1%}")
    print(f"  Hit adjacente (d<=1):             {hits_adj}/{n} = {hit_rate_adj:.1%}")
    print(f"  Baseline:                         2/9 = {BASELINE_HIT_RATE:.1%}")
    print(f"  Superou baseline 22,2%?           {'SIM' if beat_baseline else 'NAO'}")
    print_matrix(cm_ia, "Matriz preferred_ia \\ allocated:")
    print("  Por classe (preferred_ia):")
    for pref, info in hit_by_pref.items():
        print(f"    {pref}: {info['hits']}/{info['n']} ({info['hit_rate']:.0%})")
    print(f"  ML->cloud: {ml_on_cloud}/3   leve->edge: {light_on_edge}/3")
    print(f"  opostas: leve->cloud={light_on_cloud}  ML->edge={ml_on_edge}")
    if all_layer_logs:
        print("  --- logs [TCC-METRICS][LAYER] ---")
        for lg in all_layer_logs:
            print(
                f"    run{lg['run']}: preferred={lg.get('preferred')} chosen={lg.get('chosen')} "
                f"hostname={lg.get('hostname')} score_layer={lg.get('score_layer')} "
                f"score_total={lg.get('score_total')}"
            )
    print(f"\nJSON: {out}")

    ok = n == len(SEQUENCE) and all(s.get("status") == "concluida" for s in series)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
