#!/usr/bin/env python3
"""EXP-02 — carga CPU-bound ~18 s (leve, idêntica nas 5 tarefas)."""
from __future__ import annotations

import time

HOLD_SECONDS = 18.0


def cpu_bound_hold(seconds: float) -> int:
    t0 = time.monotonic()
    acc = 0
    i = 0
    while time.monotonic() - t0 < seconds:
        acc += (i * i) ^ (i << 1)
        i += 1
    return acc


if __name__ == "__main__":
    print(f"exp2_cpu: hold {HOLD_SECONDS}s...", flush=True)
    t0 = time.monotonic()
    result = cpu_bound_hold(HOLD_SECONDS)
    elapsed = time.monotonic() - t0
    print(f"exp2_cpu_ok result={result} hold_s={elapsed:.2f}", flush=True)
