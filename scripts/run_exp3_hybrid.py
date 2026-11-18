#!/usr/bin/env python3
"""
EXP-03 — braço Orquestrador Híbrido (anti-deadlock / clamp 80%).

1) Cold start SQLite
2) Submete sonda leve (task_exp3_hello.py)
3) Após o planejamento (status=na_fila), injeta oversize em
   required_memory_gb + k8s_job_spec.memory_* no ai_raw_response
   (o Librarian caparia em 8 GB — a prova é da FILA, não do LLM)
4) Aguarda despacho / status terminal
5) Exporta JSON em results/metrics_export/exp3/hibrido/

Uso:
  python scripts/run_exp3_hybrid.py
  python scripts/run_exp3_hybrid.py --memory-gi 9.45
"""

from __future__ import annotations

import argparse
import json
import math
import shutil
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    print("Instale requests: pip install requests", file=sys.stderr)
    raise SystemExit(1)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCRIPT = ROOT / "scripts" / "eval_tasks" / "task_exp3_hello.py"
DEFAULT_DB = ROOT / "data" / "nodes.db"
TERMINAL = frozenset({"concluida", "falha", "cancelada", "erro_agente", "falha_alocacao"})
BASELINE_OVERSIZE_GI = 9.45  # padronizado com run_exp3_baseline.STANDARD_OVERSIZE_GI


class _Tee:
    """Espelha stdout/stderr num arquivo de log (evidencia para artigo/slides)."""

    def __init__(self, stream, fh):
        self._s, self._f = stream, fh

    def write(self, data):
        self._s.write(data)
        try:
            self._f.write(data)
            self._f.flush()
        except Exception:
            pass
        return len(data)

    def flush(self):
        self._s.flush()
        try:
            self._f.flush()
        except Exception:
            pass

    def __getattr__(self, name):
        return getattr(self._s, name)


def start_run_log(arm: str) -> Path:
    log_dir = ROOT / "results" / "metrics_export" / "exp3" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"exp3_{arm}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    fh = path.open("w", encoding="utf-8", errors="replace")
    sys.stdout = _Tee(sys.stdout, fh)
    sys.stderr = _Tee(sys.stderr, fh)
    return path


def _connect(db: Path) -> sqlite3.Connection:
    c = sqlite3.connect(str(db))
    c.row_factory = sqlite3.Row
    return c


def kubectl(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["kubectl", *args], check=check, text=True, capture_output=True)


def cold_start_reset(db: Path, keep_uploads: bool = False) -> None:
    if not db.is_file():
        raise FileNotFoundError(f"DB não encontrado: {db}")
    backup = db.with_name(f"{db.stem}.bak_{int(time.time())}{db.suffix}")
    shutil.copy2(db, backup)
    print(f"Cold start backup: {backup}", flush=True)

    conn = sqlite3.connect(str(db))
    try:
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
        # Nó fantasma não existe no Kind — não pode inflar R_max do clamp.
        try:
            cur.execute(
                "UPDATE nodes SET is_active=0 WHERE node_id='cloud-highspec-worker'"
            )
        except sqlite3.Error:
            pass
        conn.commit()
        print(f"Cold start: removidos {n_tasks} tasks, {n_res} results", flush=True)
    finally:
        conn.close()

    if not keep_uploads:
        up = ROOT / "uploaded_tasks"
        if up.is_dir():
            removed = 0
            for p in up.iterdir():
                if p.is_file() and not p.name.startswith("."):
                    p.unlink(missing_ok=True)
                    removed += 1
            print(f"Cold start: uploads removidos={removed}", flush=True)


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
        return dict(row) if row else None
    finally:
        conn.close()


def freeze_on_queue(db: Path, task_id: str) -> dict:
    """
    Assim que a tarefa chega em na_fila, tira-a da fila (pendente) antes do
    process_queue despachar o pedido 'pequeno' da IA.
    """
    conn = _connect(db)
    try:
        conn.execute(
            "UPDATE tasks SET status='pendente' WHERE task_id=? AND status='na_fila' "
            "AND (allocated_node_id IS NULL OR allocated_node_id='')",
            (task_id,),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def wait_status(
    db: Path,
    task_id: str,
    wanted: set[str],
    timeout_s: float,
    poll_s: float = 0.15,
    *,
    freeze_when_na_fila: bool = False,
) -> dict:
    deadline = time.time() + timeout_s
    last: dict = {}
    while time.time() < deadline:
        row = fetch_task(db, task_id)
        if row:
            last = row
            st = row.get("status")
            print(
                f"  [{datetime.now().strftime('%H:%M:%S')}] {task_id[:8]}... status={st}",
                flush=True,
            )
            if freeze_when_na_fila and st == "na_fila":
                frozen = freeze_on_queue(db, task_id)
                if frozen.get("status") == "pendente":
                    print("  -> congelada em pendente (anti-corrida com a fila)", flush=True)
                    return frozen
                # Se ja alocou, devolve o estado atual
                return frozen or row
            if st in wanted:
                return row
            if st in TERMINAL and st not in wanted:
                return row
        time.sleep(poll_s)
    raise TimeoutError(
        f"Timeout aguardando {wanted} para {task_id} (ultimo={last.get('status')})"
    )


def measure_r_max_gi(db: Path) -> tuple[float, list[dict]]:
    """R_max ≈ maior memory disponível entre workers K8s ativos (sem control-plane)."""
    conn = _connect(db)
    try:
        rows = conn.execute(
            "SELECT node_id, hostname, memory_total_gb, memory_available_gb, "
            "cpu_used_percent, is_active, provider_type, compute_layer "
            "FROM nodes WHERE provider_type='kubernetes' AND is_active=1"
        ).fetchall()
    finally:
        conn.close()

    workers: list[dict] = []
    for r in rows:
        d = dict(r)
        host = str(d.get("hostname") or d.get("node_id") or "")
        if "control-plane" in host.lower():
            continue
        total = float(d.get("memory_total_gb") or 0)
        avail = d.get("memory_available_gb")
        eff = float(avail) if avail is not None else total
        d["effective_ram_gi"] = round(eff, 3)
        workers.append(d)

    if not workers:
        raise RuntimeError("Nenhum worker Kubernetes ativo para medir R_max")
    r_max = max(w["effective_ram_gi"] for w in workers)
    return r_max, workers


def choose_inject_gi(desired: float, r_max: float) -> tuple[float, str]:
    """
    Escolhe R injetado na janela (R_max, 1.25*R_max] para o clamp 80% despachar.
    Preferência: desired (ex. 10) quando matematicamente válido.
    """
    hi = r_max / 0.80
    if desired > r_max and desired <= hi + 1e-9:
        return float(desired), "desired_in_window"
    if desired <= r_max:
        # Força oversize mínimo
        calibrated = min(hi, max(r_max + 0.25, round((r_max + hi) / 2.0, 2)))
        return calibrated, "desired_not_oversize"
    # desired > 1.25*R_max → calibra para ainda oversize mas dentro da janela
    calibrated = min(desired, math.floor(hi * 100) / 100.0)
    if calibrated <= r_max:
        calibrated = round(r_max + 0.1, 3)
    return float(calibrated), "calibrated_to_80pct_window"


def inject_oversize(db: Path, task_id: str, memory_gi: float, cpu_cores: float = 1.0) -> dict:
    """Patch SQLite: required_* + k8s_job_spec memory/cpu = oversize (pré-fila)."""
    mem_qty = f"{int(memory_gi)}Gi" if float(memory_gi).is_integer() else f"{int(memory_gi * 1024)}Mi"
    cpu_qty = f"{cpu_cores:g}"

    conn = _connect(db)
    try:
        row = conn.execute(
            "SELECT status, ai_raw_response, required_memory_gb, required_cpu_threads, "
            "allocated_node_id FROM tasks WHERE task_id=?",
            (task_id,),
        ).fetchone()
        if not row:
            raise RuntimeError(f"task {task_id} não encontrada para injeção")
        status = row["status"]
        if row["allocated_node_id"]:
            raise RuntimeError(
                f"tarefa já alocada em {row['allocated_node_id']} — corrida com a fila; "
                "reinicie o experimento"
            )
        if status not in {"na_fila", "pendente"}:
            raise RuntimeError(f"injecao requer status=na_fila/pendente (atual={status})")

        # Garante fora da fila durante o patch
        conn.execute(
            "UPDATE tasks SET status='pendente' WHERE task_id=?",
            (task_id,),
        )

        raw = row["ai_raw_response"] or "{}"
        try:
            data = json.loads(raw) if str(raw).strip() else {}
        except json.JSONDecodeError:
            data = {}
        if not isinstance(data, dict):
            data = {}

        spec = data.get("k8s_job_spec") if isinstance(data.get("k8s_job_spec"), dict) else {}
        spec = dict(spec)
        spec["memory_request"] = mem_qty
        spec["memory_limit"] = mem_qty
        spec["cpu_request"] = cpu_qty
        spec["cpu_limit"] = cpu_qty
        # Remove pinagem residual que poderia mascarar o teste de capacidade.
        if isinstance(spec.get("nodeSelector"), dict):
            spec.pop("nodeSelector", None)
        data["k8s_job_spec"] = spec
        data["exp3_lab_injection"] = {
            "memory_gi": memory_gi,
            "memory_qty": mem_qty,
            "injected_at": datetime.now(timezone.utc).isoformat(),
            "note": "Oversize injetado pós-planejamento para exercitar clamp da fila",
        }

        # Também no strategist_summary se existir
        trace = data.get("multi_agent_trace")
        if isinstance(trace, dict):
            summ = trace.get("strategist_summary")
            if isinstance(summ, dict):
                sspec = summ.get("k8s_job_spec") if isinstance(summ.get("k8s_job_spec"), dict) else {}
                sspec = dict(sspec)
                sspec["memory_request"] = mem_qty
                sspec["memory_limit"] = mem_qty
                sspec["cpu_request"] = cpu_qty
                sspec["cpu_limit"] = cpu_qty
                sspec.pop("nodeSelector", None)
                summ["k8s_job_spec"] = sspec
                trace["strategist_summary"] = summ
                data["multi_agent_trace"] = trace

        new_raw = json.dumps(data, ensure_ascii=False)
        conn.execute(
            "UPDATE tasks SET required_memory_gb=?, required_cpu_threads=?, "
            "ai_raw_response=?, status='na_fila', allocated_node_id=NULL WHERE task_id=?",
            (float(memory_gi), float(cpu_cores), new_raw, task_id),
        )
        conn.commit()
        return {
            "injected_memory_gi": memory_gi,
            "injected_memory_qty": mem_qty,
            "injected_cpu": cpu_cores,
            "status_before": status,
            "required_memory_gb_before": row["required_memory_gb"],
        }
    finally:
        conn.close()


def wait_terminal(db: Path, task_id: str, timeout_s: float, poll_s: float = 3.0) -> dict:
    deadline = time.time() + timeout_s
    last: dict = {}
    while time.time() < deadline:
        row = fetch_task(db, task_id)
        if row:
            last = row
            st = row.get("status")
            print(
                f"  [{datetime.now().strftime('%H:%M:%S')}] {task_id[:8]}... "
                f"status={st} node={row.get('allocated_node_id') or '-'} "
                f"req_ram={row.get('required_memory_gb')}",
                flush=True,
            )
            if st in TERMINAL:
                return row
        time.sleep(poll_s)
    raise TimeoutError(f"Timeout terminal {task_id} (último={last.get('status')})")


def extract_effective_spec(ai_raw: str | None) -> dict:
    if not ai_raw:
        return {}
    try:
        data = json.loads(ai_raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    if not isinstance(data, dict):
        return {}
    spec = data.get("k8s_job_spec")
    if isinstance(spec, dict):
        return spec
    return {}


def extract_dispatch_meta(ai_raw: str | None) -> dict:
    if not ai_raw:
        return {}
    try:
        data = json.loads(ai_raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    if not isinstance(data, dict):
        return {}
    meta = data.get("dispatch_meta")
    return meta if isinstance(meta, dict) else {}


def scrape_clamp_logs(task_id: str, since_s: float = 600.0) -> list[str]:
    """Recorta logs do context-server relacionados ao clamp / fila desta tarefa."""
    blob = ""
    try:
        r = subprocess.run(
            ["docker", "logs", "--since", f"{int(since_s)}s", "context-server"],
            capture_output=True,
            check=False,
        )
        blob = (r.stdout or b"").decode("utf-8", errors="replace") + (
            r.stderr or b""
        ).decode("utf-8", errors="replace")
    except Exception:
        try:
            cid = subprocess.run(
                ["docker", "ps", "-aq", "--filter", "name=context-server"],
                capture_output=True,
                text=True,
                check=False,
            ).stdout.strip().splitlines()[0]
            r = subprocess.run(
                ["docker", "logs", "--since", f"{int(since_s)}s", cid],
                capture_output=True,
                check=False,
            )
            blob = (r.stdout or b"").decode("utf-8", errors="replace") + (
                r.stderr or b""
            ).decode("utf-8", errors="replace")
        except Exception:
            return []

    out: list[str] = []
    for line in blob.splitlines():
        if task_id in line or "80%" in line or "ALOCACAO" in line or "Job criado" in line:
            if (
                task_id in line
                or "80%" in line
                or "ALOCACAO" in line
                or "Job criado" in line
            ):
                out.append(line.rstrip())
    filtered = [
        ln
        for ln in out
        if task_id in ln or "80%" in ln or "ALOCACAO" in ln or "Job criado" in ln
    ]
    return filtered[-40:]


def reset_for_oversize_lab(db: Path, task_id: str) -> None:
    """
    Procedimento de laboratorio: remove Job/CM ja criados com o pedido 'pequeno'
    da IA, limpa alocacao/resultado e deixa a tarefa pronta para injecao oversize.
    """
    # Labels usadas pelo executor
    for sel in (f"task_id={task_id}", f"task-id={task_id}"):
        kubectl(
            [
                "delete",
                "jobs,pods,cm",
                "-n",
                "default",
                "-l",
                sel,
                "--ignore-not-found",
                "--wait=false",
            ],
            check=False,
        )
    # Nome deterministico
    compact = task_id.replace("-", "")[:40]
    kubectl(
        ["delete", "job", f"task-job-{compact}", "-n", "default", "--ignore-not-found", "--wait=false"],
        check=False,
    )
    kubectl(
        ["delete", "cm", f"task-cm-{compact}", "-n", "default", "--ignore-not-found", "--wait=false"],
        check=False,
    )
    time.sleep(2.0)

    conn = _connect(db)
    try:
        conn.execute("DELETE FROM task_results WHERE task_id=?", (task_id,))
        conn.execute(
            "UPDATE tasks SET status='pendente', allocated_node_id=NULL, "
            "allocated_compute_layer=NULL, reallocation_attempts=0, "
            "dispatched_at=NULL WHERE task_id=?",
            (task_id,),
        )
        conn.commit()
    finally:
        conn.close()


def main() -> int:
    p = argparse.ArgumentParser(description="EXP-03 hibrido: oversize + clamp 80%")
    p.add_argument("--api", default="http://127.0.0.1:5000")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--script", type=Path, default=DEFAULT_SCRIPT)
    p.add_argument("--user", default="lsouza")
    p.add_argument(
        "--memory-gi",
        type=float,
        default=BASELINE_OVERSIZE_GI,
        help="Oversize desejado (baseline=10). Calibrado a janela 80% se necessario.",
    )
    p.add_argument(
        "--force-exact",
        action="store_true",
        help="Injeta --memory-gi sem calibrar (pode ficar na_fila se R>1.25*R_max)",
    )
    p.add_argument("--timeout-plan", type=float, default=300.0)
    p.add_argument("--timeout-run", type=float, default=600.0)
    p.add_argument(
        "--allow-calibrate",
        action="store_true",
        help="Permite injetar valor diferente de --memory-gi (quebra a paridade com o baseline).",
    )
    p.add_argument("--keep-uploads", action="store_true")
    p.add_argument("--skip-reset", action="store_true")
    args = p.parse_args()
    log_path = start_run_log("hybrid")

    if not args.script.is_file():
        print(f"Script nao encontrado: {args.script}", file=sys.stderr)
        return 1
    if not args.db.is_file():
        print(f"DB nao encontrado: {args.db}", file=sys.stderr)
        return 1

    print("=" * 64)
    print("EXP-03 - Orquestrador Hibrido (anti-deadlock / clamp 80%)")
    print(f"API={args.api}  desired_oversize={args.memory_gi}Gi  script={args.script.name}")
    print("=" * 64)

    kubectl(
        ["delete", "jobs,pods,cm", "-l", "app=tcc-task-runner", "--all-namespaces", "--ignore-not-found"],
        check=False,
    )

    if not args.skip_reset:
        cold_start_reset(args.db, keep_uploads=args.keep_uploads)
    else:
        print("AVISO: --skip-reset", flush=True)

    time.sleep(12.0)
    r_max, workers = measure_r_max_gi(args.db)
    print(f"\nR_max efetivo (workers K8s): {r_max:.3f} GiB", flush=True)
    for w in workers:
        print(
            f"  {w.get('hostname')}: eff={w['effective_ram_gi']} "
            f"total={w.get('memory_total_gb')} layer={w.get('compute_layer')}",
            flush=True,
        )

    if args.force_exact:
        inject_gi, calibrate_reason = float(args.memory_gi), "force_exact"
    else:
        inject_gi, calibrate_reason = choose_inject_gi(float(args.memory_gi), r_max)

    if (
        not args.force_exact
        and not args.allow_calibrate
        and abs(inject_gi - float(args.memory_gi)) > 1e-9
    ):
        print(
            f"ERRO: {args.memory_gi}Gi fora da janela 80% deste cluster; calibrar para "
            f"{inject_gi}Gi quebraria a paridade com o baseline. Reexecute o baseline "
            f"com --memory-gi {inject_gi} ou use --allow-calibrate.",
            file=sys.stderr,
        )
        return 2

    window_hi = r_max / 0.80
    print(
        f"Janela 80%: ({r_max:.3f}, {window_hi:.3f}] GiB | "
        f"injetar={inject_gi}Gi ({calibrate_reason})",
        flush=True,
    )
    if calibrate_reason != "desired_in_window" and not args.force_exact:
        print(
            f"NOTA: {args.memory_gi}Gi (baseline) fica fora da janela 1.25x R_max neste cluster; "
            f"usando {inject_gi}Gi para exercitar o clamp (ainda oversize vs R_max={r_max:.3f}).",
            flush=True,
        )

    print("\n[1/5] Submetendo sonda leve (planejamento IA)...", flush=True)
    tid = submit(args.api, args.script, args.user)
    print(f"  task_id={tid}", flush=True)
    t0 = time.monotonic()

    print("\n[2/5] Aguardando fim do planejamento (IA escreve na_fila/despacha)...", flush=True)
    row = wait_status(
        args.db,
        tid,
        {"na_fila", "executando", "concluida", "pendente", "falha", "erro_agente"},
        args.timeout_plan,
        poll_s=0.5,
    )
    print(
        f"  pos-IA: status={row.get('status')} req_ram={row.get('required_memory_gb')} "
        f"node={row.get('allocated_node_id')}",
        flush=True,
    )

    print("\n[3/5] Reset laboratorio + injecao oversize (pedido que a fila deve clamar)...", flush=True)
    reset_for_oversize_lab(args.db, tid)
    inj = inject_oversize(args.db, tid, inject_gi)
    print(f"  injection={inj}", flush=True)
    check = fetch_task(args.db, tid) or {}
    print(
        f"  confirmado required_memory_gb={check.get('required_memory_gb')} "
        f"status={check.get('status')}",
        flush=True,
    )
    if float(check.get("required_memory_gb") or 0) < float(inject_gi) * 0.99:
        print("ERRO: injecao nao persistiu", file=sys.stderr)
        return 1

    print("\n[4/5] Aguardando despacho / status terminal (pos-clamp)...", flush=True)
    try:
        final = wait_terminal(args.db, tid, args.timeout_run)
    except TimeoutError as e:
        print(f"ERRO: {e}", file=sys.stderr)
        final = fetch_task(args.db, tid) or {}
        logs = scrape_clamp_logs(tid)
        report = {
            "experiment": "exp3",
            "arm": "hybrid",
            "task_id": tid,
            "status": final.get("status"),
            "error": str(e),
            "r_max_gi": r_max,
            "desired_memory_gi": args.memory_gi,
            "injected_memory_gi": inject_gi,
            "calibrate_reason": calibrate_reason,
            "injection": inj,
            "clamp_log_lines": [ln.encode("ascii", "replace").decode("ascii") for ln in logs],
            "at": datetime.now(timezone.utc).isoformat(),
        }
        out = (
            ROOT
            / "results"
            / "metrics_export"
            / "exp3"
            / "hibrido"
            / f"exp3_hybrid_{int(time.time())}.json"
        )
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"JSON (parcial): {out}")
        return 1

    wall = time.monotonic() - t0
    ai_raw = final.get("ai_raw_response")
    spec = extract_effective_spec(ai_raw)
    meta = extract_dispatch_meta(ai_raw)
    logs = scrape_clamp_logs(tid)
    clamp_seen = any(
        "tolerancia de 80%" in ln.lower().replace("â", "a").replace("ã", "a")
        or "tolerância de 80%" in ln
        or "80% do piso" in ln
        or "80%% do piso" in ln
        for ln in logs
    )
    if not clamp_seen:
        clamp_seen = any("ajustada dentro da" in ln.lower() and "80" in ln for ln in logs)

    # Sanitize logs for Windows console
    logs_safe = []
    for ln in logs:
        logs_safe.append(ln.encode("ascii", "replace").decode("ascii"))

    report = {
        "experiment": "exp3",
        "arm": "hybrid",
        "task_id": tid,
        "status": final.get("status"),
        "allocated_node_id": final.get("allocated_node_id"),
        "allocated_compute_layer": final.get("allocated_compute_layer"),
        "docker_image": final.get("docker_image"),
        "r_max_gi": r_max,
        "workers": [
            {
                "hostname": w.get("hostname"),
                "effective_ram_gi": w.get("effective_ram_gi"),
                "memory_total_gb": w.get("memory_total_gb"),
                "compute_layer": w.get("compute_layer"),
            }
            for w in workers
        ],
        "desired_memory_gi": args.memory_gi,
        "injected_memory_gi": inject_gi,
        "calibrate_reason": calibrate_reason,
        "window_hi_gi": round(window_hi, 3),
        "injection": inj,
        "required_memory_gb_final": final.get("required_memory_gb"),
        "effective_k8s_job_spec": spec,
        "memory_limit_effective": spec.get("memory_limit"),
        "memory_request_effective": spec.get("memory_request"),
        "dispatch_meta": meta,
        "best_effort": bool(meta.get("best_effort")),
        "clamp_log_seen": clamp_seen,
        "clamp_log_lines": logs_safe,
        "duration_wall_s": round(wall, 3),
        "job_created": bool(final.get("allocated_node_id"))
        or final.get("status") in {"executando", "concluida", "falha", "erro_agente"},
        "anti_pending": final.get("status") != "na_fila" and bool(final.get("allocated_node_id")),
        "at": datetime.now(timezone.utc).isoformat(),
    }

    print("\n" + "=" * 64)
    print("RESUMO EXP-03 HIBRIDO")
    print(f"  status={report['status']}")
    print(f"  node={report['allocated_node_id']}")
    print(f"  injected={inject_gi}Gi  R_max={r_max:.3f}Gi")
    print(f"  memory_limit efetivo={report['memory_limit_effective']}")
    print(f"  best_effort={report['best_effort']}  clamp_log_seen={clamp_seen}")
    if logs_safe:
        print("  --- logs fila/clamp ---")
        for ln in logs_safe[-12:]:
            print(f"  {ln}")

    out = (
        ROOT
        / "results"
        / "metrics_export"
        / "exp3"
        / "hibrido"
        / f"exp3_hybrid_{int(time.time())}.json"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nJSON: {out}")
    full = log_path.with_name(log_path.stem + "_context_server.log")
    full.write_text("\n".join(logs_safe), encoding="utf-8")
    print(f"Logs fila/clamp: {full}")
    print(f"LOG: {log_path}")

    mem_eff = str(report.get("memory_limit_effective") or "")
    # Efetivo deve ser <= R_max (nao o oversize injetado)
    ok = (
        report["status"] == "concluida"
        and clamp_seen
        and bool(mem_eff)
        and mem_eff != inj.get("injected_memory_qty")
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
