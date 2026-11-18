#!/usr/bin/env python3
"""EXP-04 — carga CPU intermediária → preferência teórica Fog."""

from __future__ import annotations


def cpu_bound_work(n: int = 4_000_000) -> int:
    """Loop intensivo de CPU (sem deps externas)."""
    acc = 0
    for i in range(n):
        acc += (i * i) ^ (i << 1)
    return acc


if __name__ == "__main__":
    result = cpu_bound_work()
    print("fog_cpu_probe_ok", result, flush=True)
