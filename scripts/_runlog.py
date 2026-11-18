"""Espelha stdout/stderr em results/metrics_export/<exp>/logs/ (evidencia p/ artigo e slides)."""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class _Tee:
    def __init__(self, stream, fh):
        self._s, self._f = stream, fh

    def write(self, data):
        try:
            self._s.write(data)
        except UnicodeEncodeError:
            self._s.write(data.encode("ascii", "replace").decode("ascii"))
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


def start_run_log(exp: str, arm: str) -> Path:
    d = ROOT / "results" / "metrics_export" / exp / "logs"
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{exp}_{arm}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    fh = path.open("w", encoding="utf-8", errors="replace")
    sys.stdout = _Tee(sys.stdout, fh)
    sys.stderr = _Tee(sys.stderr, fh)
    return path
