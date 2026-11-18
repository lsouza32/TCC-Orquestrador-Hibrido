#!/usr/bin/env python3
"""
Limpa dados de avaliação no SQLite (mantém nós + catálogo Docker).

Uso:
  python scripts/reset_eval_db.py
  python scripts/reset_eval_db.py --db data/nodes.db
"""

from __future__ import annotations

import argparse
import shutil
import sqlite3
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "nodes.db"


def main() -> int:
    parser = argparse.ArgumentParser(description="Reset tasks/results para avaliação TCC")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--keep-uploads", action="store_true")
    args = parser.parse_args()

    db: Path = args.db
    if not db.is_file():
        print(f"DB não encontrado: {db}")
        return 1

    backup = db.with_name(f"{db.stem}.bak_{int(time.time())}{db.suffix}")
    shutil.copy2(db, backup)
    print(f"Backup: {backup}")

    conn = sqlite3.connect(str(db))
    try:
        cur = conn.cursor()
        n_tasks = cur.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        n_res = cur.execute("SELECT COUNT(*) FROM task_results").fetchone()[0]
        cur.execute("DELETE FROM task_results")
        cur.execute("DELETE FROM tasks")
        # Zera reservas nos nós (legado node_agent)
        try:
            cur.execute(
                "UPDATE nodes SET cpu_reserved_threads=0, memory_reserved_gb=0, disk_reserved_gb=0"
            )
        except sqlite3.Error:
            pass
        conn.commit()
        print(f"Removidos: {n_tasks} tasks, {n_res} results")
        print("Mantidos: nodes + docker_images_catalog")
    finally:
        conn.close()

    if not args.keep_uploads:
        up = ROOT / "uploaded_tasks"
        if up.is_dir():
            removed = 0
            for p in up.iterdir():
                if p.is_file() and not p.name.startswith("."):
                    p.unlink(missing_ok=True)
                    removed += 1
            print(f"Uploads removidos: {removed}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
