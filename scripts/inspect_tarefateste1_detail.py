#!/usr/bin/env python3
import json
import sqlite3

from kubernetes import client, config as kcfg

DB = "/app/data/nodes.db"
IDS = [
    "f29ee8a2-64d2-4bd5-ae9c-1d6709bbf1c3",
    "69a54c73-008f-4d43-8df0-9539c97c7b66",
    "132ee0f4-7a43-4a6e-a053-80a58b6ef2bb",
]


def main() -> None:
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    for tid in IDS:
        r = conn.execute(
            "SELECT task_id, status, required_cpu_threads, required_memory_gb, "
            "docker_image, ai_category, ai_raw_response FROM tasks WHERE task_id=?",
            (tid,),
        ).fetchone()
        print("=" * 60)
        print(tid, r["status"], "cpu=", r["required_cpu_threads"], "ram=", r["required_memory_gb"], "img=", r["docker_image"], "cat=", r["ai_category"])
        raw = r["ai_raw_response"] or ""
        try:
            blob = json.loads(raw)
            trace = blob.get("multi_agent_trace") or {}
            for k in ("analyst", "librarian", "strategist"):
                if k in trace:
                    print(f"--- {k} ---")
                    print(json.dumps(trace[k], ensure_ascii=False, indent=2)[:1500])
            # walk for metadata_name
            def walk(o):
                if isinstance(o, dict):
                    if "metadata_name" in o:
                        print("metadata_name=", o["metadata_name"])
                    if "k8s_job_spec" in o and isinstance(o["k8s_job_spec"], dict):
                        print("k8s_job_spec.keys=", list(o["k8s_job_spec"].keys()))
                        print("k8s_job_spec=", json.dumps(o["k8s_job_spec"], ensure_ascii=False)[:1000])
                    for v in o.values():
                        walk(v)
                elif isinstance(o, list):
                    for v in o:
                        walk(v)
            walk(blob)
        except Exception as e:
            print("parse_err", e)

    kcfg.load_kube_config()
    core = client.CoreV1Api()
    print("\n=== POD EVENTS / STATUS ===")
    for p in core.list_namespaced_pod("default").items:
        print(p.metadata.name, "phase=", p.status.phase, "node=", p.spec.node_name)
        if p.status.container_statuses:
            for cs in p.status.container_statuses:
                print("  container=", cs.name, "ready=", cs.ready, "state=", cs.state)
        if p.status.conditions:
            for c in p.status.conditions:
                if c.status != "True" or c.type == "PodScheduled":
                    print("  cond=", c.type, c.status, c.reason, (c.message or "")[:200])
    evs = core.list_namespaced_event("default")
    for e in sorted(evs.items, key=lambda x: x.last_timestamp or x.event_time or "", reverse=True)[:15]:
        print("EVT", e.type, e.reason, e.involved_object.name, (e.message or "")[:180])


if __name__ == "__main__":
    main()
