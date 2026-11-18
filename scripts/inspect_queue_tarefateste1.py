#!/usr/bin/env python3
import json
import sqlite3
import time

DB = "/app/data/nodes.db"


def main() -> None:
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    print("=== TABLES ===")
    print([r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'")])

    print("\n=== STATUS COUNTS ===")
    for r in cur.execute("SELECT status, COUNT(*) AS c FROM tasks GROUP BY status"):
        print(dict(r))

    print("\n=== TAREFATESTE1 / RECENT TASKS ===")
    rows = cur.execute(
        """
        SELECT task_id, filename, status, task_type,
               required_cpu_threads, required_memory_gb, required_disk_gb,
               docker_image, allocated_node_id,
               preferred_compute_layer, allocated_compute_layer,
               submitted_at, planning_completed_at, dispatched_at,
               ai_category, ai_detected_libraries, ai_image_source,
               reallocation_attempts
        FROM tasks
        WHERE filename LIKE '%tarefateste1%' OR filename LIKE '%teste1%'
        ORDER BY submitted_at DESC
        LIMIT 20
        """
    ).fetchall()
    if not rows:
        print("(sem match por nome; últimas 10)")
        rows = cur.execute(
            """
            SELECT task_id, filename, status, task_type,
                   required_cpu_threads, required_memory_gb, required_disk_gb,
                   docker_image, allocated_node_id,
                   preferred_compute_layer, allocated_compute_layer,
                   submitted_at, planning_completed_at, dispatched_at,
                   ai_category, ai_detected_libraries, ai_image_source,
                   reallocation_attempts
            FROM tasks
            ORDER BY submitted_at DESC
            LIMIT 10
            """
        ).fetchall()

    now = time.time()
    for r in rows:
        d = dict(r)
        sub = d.get("submitted_at")
        plan = d.get("planning_completed_at")
        if sub is not None and plan is not None:
            d["planning_s"] = round(float(plan) - float(sub), 3)
        if sub is not None:
            d["age_s"] = round(now - float(sub), 1)
        # trim long fields
        for k in ("ai_detected_libraries",):
            if d.get(k) and len(str(d[k])) > 200:
                d[k] = str(d[k])[:200] + "..."
        print(json.dumps(d, ensure_ascii=False, default=str))

    print("\n=== QUEUE (na_fila / analisando_ia / alocada / executando) ===")
    q = cur.execute(
        """
        SELECT task_id, filename, status,
               required_cpu_threads, required_memory_gb, required_disk_gb,
               docker_image, allocated_node_id,
               preferred_compute_layer, submitted_at, planning_completed_at
        FROM tasks
        WHERE status IN ('na_fila','analisando_ia','pendente','alocada','executando','aguardando')
        ORDER BY submitted_at ASC
        """
    ).fetchall()
    for r in q:
        print(json.dumps(dict(r), ensure_ascii=False, default=str))
    print(f"queue_count={len(q)}")

    print("\n=== NODES ===")
    for r in cur.execute("SELECT * FROM nodes"):
        print(json.dumps(dict(r), ensure_ascii=False, default=str))

    print("\n=== AI RAW (últimas tarefateste1, resumo) ===")
    for r in cur.execute(
        """
        SELECT task_id, filename, status, ai_raw_response, ai_category,
               required_cpu_threads, required_memory_gb, docker_image
        FROM tasks
        WHERE filename LIKE '%tarefateste1%' OR filename LIKE '%teste1%'
        ORDER BY submitted_at DESC
        LIMIT 3
        """
    ):
        d = dict(r)
        raw = d.pop("ai_raw_response") or ""
        print("---", d["task_id"], d["status"], "---")
        print("category=", d.get("ai_category"), "cpu=", d.get("required_cpu_threads"),
              "ram=", d.get("required_memory_gb"), "img=", d.get("docker_image"))
        if raw:
            try:
                blob = json.loads(raw)
                trace = blob.get("multi_agent_trace") or blob
                # compact
                if isinstance(trace, dict):
                    for agent in ("analyst", "librarian", "strategist", "Analista", "Librarian", "Strategist"):
                        if agent in trace:
                            print(agent, ":", json.dumps(trace[agent], ensure_ascii=False)[:800])
                else:
                    print(str(trace)[:800])
            except json.JSONDecodeError:
                print(raw[:800])


if __name__ == "__main__":
    main()
