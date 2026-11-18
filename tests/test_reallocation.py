"""Testes da realocação pós-falha."""

from __future__ import annotations

import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

_ROOT = Path(__file__).resolve().parents[1]


def _ensure_pkg():
    ctx = types.ModuleType("context_server")
    ctx.__path__ = [str(_ROOT / "context_server")]
    sys.modules.setdefault("context_server", ctx)
    for name, path in (
        ("context_server.database", _ROOT / "context_server" / "database"),
        ("context_server.services", _ROOT / "context_server" / "services"),
    ):
        if name not in sys.modules:
            m = types.ModuleType(name)
            m.__path__ = [str(path)]
            sys.modules[name] = m


def _load_module(mod_name: str, rel: str):
    _ensure_pkg()
    # stubs mínimos usados por reallocation
    if "context_server.config" not in sys.modules:
        cfg = types.ModuleType("context_server.config")
        cfg.TASK_REALLOCATION_MAX_ATTEMPTS = 3
        sys.modules["context_server.config"] = cfg
    if "context_server.logging_config" not in sys.modules:
        log_mod = types.ModuleType("context_server.logging_config")
        log_mod.get_logger = lambda name: MagicMock()
        log_mod.log_error_with_context = lambda *a, **k: None
        sys.modules["context_server.logging_config"] = log_mod
    if "context_server.services.k8s_job_executor" not in sys.modules:
        k8s = types.ModuleType("context_server.services.k8s_job_executor")
        k8s.kubernetes_allocation_hostname = (
            lambda aid: (str(aid)[4:] if aid and str(aid).startswith("k8s:") else None)
        )
        sys.modules["context_server.services.k8s_job_executor"] = k8s

    spec = importlib.util.spec_from_file_location(mod_name, _ROOT / rel)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return mod


_realloc = _load_module(
    "context_server.services.reallocation",
    "context_server/services/reallocation.py",
)


class TestReallocationHelpers(unittest.TestCase):
    def test_can_reallocate_within_limit(self):
        row = {"ai_raw_response": "{}", "allocated_node_id": "k8s:worker"}
        ok, why = _realloc.can_reallocate(
            row,
            failed_hostname="worker",
            available_hostnames=["worker", "worker2"],
        )
        self.assertTrue(ok, why)

    def test_can_reallocate_rejects_no_alternatives(self):
        row = {"ai_raw_response": "{}", "allocated_node_id": "k8s:worker"}
        ok, why = _realloc.can_reallocate(
            row,
            failed_hostname="worker",
            available_hostnames=["worker"],
        )
        self.assertFalse(ok)
        self.assertIn("sem_nos", why)

    def test_can_reallocate_rejects_max_attempts(self):
        raw = json.dumps({
            "reallocation": {
                "attempts": 3,
                "excluded_hostnames": ["worker", "worker2"],
                "history": [],
            }
        })
        row = {"ai_raw_response": raw, "allocated_node_id": "k8s:worker2"}
        ok, why = _realloc.can_reallocate(
            row,
            failed_hostname="worker2",
            available_hostnames=["worker", "worker2", "worker3"],
        )
        self.assertFalse(ok)
        self.assertIn("limite", why)

    def test_build_increments_and_excludes(self):
        row = {
            "ai_raw_response": "{}",
            "allocated_node_id": "k8s:worker",
        }
        new_raw = _realloc.build_reallocation_ai_raw(
            row,
            failed_hostname="worker",
            reason="Failed",
            phase="Failed",
        )
        data = json.loads(new_raw)
        block = data["reallocation"]
        self.assertEqual(block["attempts"], 1)
        self.assertIn("worker", block["excluded_hostnames"])
        self.assertEqual(len(block["history"]), 1)

        # segunda falha
        row2 = {"ai_raw_response": new_raw}
        new_raw2 = _realloc.build_reallocation_ai_raw(
            row2,
            failed_hostname="worker2",
            reason="Unschedulable",
            phase="Failed",
        )
        block2 = json.loads(new_raw2)["reallocation"]
        self.assertEqual(block2["attempts"], 2)
        self.assertEqual(set(block2["excluded_hostnames"]), {"worker", "worker2"})

    def test_filter_nodes_excluding(self):
        nodes = [
            {"hostname": "worker", "node_id": "worker"},
            {"hostname": "worker2", "node_id": "worker2"},
        ]
        out = _realloc.filter_nodes_excluding_hostnames(nodes, ["worker"])
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["hostname"], "worker2")

    def test_failed_hostname_from_allocation(self):
        row = {"allocated_node_id": "k8s:tcc-cluster-worker"}
        self.assertEqual(_realloc.failed_hostname_from_task(row), "tcc-cluster-worker")


class TestReallocationConfig(unittest.TestCase):
    def test_max_attempts_from_config(self):
        old = getattr(_realloc.config, "TASK_REALLOCATION_MAX_ATTEMPTS", 3)
        try:
            _realloc.config.TASK_REALLOCATION_MAX_ATTEMPTS = 0
            ok, why = _realloc.can_reallocate({}, failed_hostname="x")
            self.assertFalse(ok)
            self.assertIn("desabilitada", why)
        finally:
            _realloc.config.TASK_REALLOCATION_MAX_ATTEMPTS = old


if __name__ == "__main__":
    unittest.main()
