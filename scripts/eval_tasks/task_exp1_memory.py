#!/usr/bin/env python3
"""
EXP-01 — carga previsível de RAM (~220 MiB) + CPU por 15–20 s.

Com limit K8s de 256Mi, o pico fica acima de 85% (near_ceiling) e força
a margem 1.25x do Orquestrador Híbrido na iteração seguinte.
"""

from __future__ import annotations

import math
import sys
import time

# ~220 MiB: acima do limiar 0.85 * 256 MiB ≈ 217.6 MiB
TARGET_MIB = 220
HOLD_SECONDS = 18.0


def allocate_ram(mib: int) -> bytearray:
    n = mib * 1024 * 1024
    buf = bytearray(n)
    # Toca páginas para garantir residência (não só virtual).
    step = 4096
    for i in range(0, n, step):
        buf[i] = (i // step) % 256
    return buf


def cpu_hold(seconds: float, buf: bytearray) -> float:
    """Loop CPU simples mantendo `buf` vivo; retorna duração real."""
    t0 = time.monotonic()
    acc = 0.0
    i = 0
    while time.monotonic() - t0 < seconds:
        acc += math.sqrt((i % 10_000) + 1.0) + (buf[i % len(buf)] * 0.0)
        i += 1
    return time.monotonic() - t0


def main() -> int:
    print(f"exp1_memory: alocando {TARGET_MIB} MiB...", flush=True)
    buf = allocate_ram(TARGET_MIB)
    print(f"exp1_memory: alocado {len(buf)} bytes; hold CPU {HOLD_SECONDS}s", flush=True)
    elapsed = cpu_hold(HOLD_SECONDS, buf)
    # Referência explícita evita otimização agressiva do GC.
    checksum = buf[0] + buf[len(buf) // 2] + buf[-1]
    print(
        f"exp1_memory_ok bytes={len(buf)} hold_s={elapsed:.2f} checksum={checksum}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
