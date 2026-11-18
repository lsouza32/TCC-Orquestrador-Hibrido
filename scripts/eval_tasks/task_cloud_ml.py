#!/usr/bin/env python3
"""
EXP-04 — carga estilo ML → preferência teórica Cloud.

O `import tensorflow` / `sklearn` fica no topo para o AST do braço híbrido
classificar como ML-heavy. Em runtime (baseline com python:3.11-slim) o import
pode falhar — a carga útil é multiplicação pesada de matrizes em Python puro.
"""

from __future__ import annotations

try:
    import tensorflow as tf  # noqa: F401 — marcador AST / ML-heavy
except ImportError:
    tf = None

try:
    import sklearn  # noqa: F401 — marcador AST / ML-heavy
except ImportError:
    sklearn = None


def matmul(a: list[list[float]], b: list[list[float]]) -> list[list[float]]:
    n, m, p = len(a), len(b[0]), len(b)
    out = [[0.0] * m for _ in range(n)]
    for i in range(n):
        for k in range(p):
            aik = a[i][k]
            for j in range(m):
                out[i][j] += aik * b[k][j]
    return out


def heavy_matrix_work(size: int = 64, rounds: int = 8) -> float:
    """Simula treino/inferência com multiplicações densas repetidas."""
    a = [[float((i * size + j) % 17) for j in range(size)] for i in range(size)]
    b = [[float((i + j * 3) % 13) for j in range(size)] for i in range(size)]
    acc = 0.0
    x = a
    for _ in range(rounds):
        x = matmul(x, b)
        acc += x[0][0] + x[size // 2][size // 2]
    return acc


if __name__ == "__main__":
    print(
        f"cloud_ml: tf={'ok' if tf is not None else 'mock'} "
        f"sklearn={'ok' if sklearn is not None else 'mock'}",
        flush=True,
    )
    result = heavy_matrix_work()
    print("cloud_ml_probe_ok", result, flush=True)
