"""Testes do enforcement determinístico do nodeSelector."""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]


def _load_enforce():
    spec = importlib.util.spec_from_file_location(
        "strategist_guard_test",
        _ROOT / "context_server" / "services" / "strategist_guard.py",
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.enforce_strategist_node_selector


_enforce_fn = _load_enforce()


class TestStrategistEnforcement(unittest.TestCase):
    def test_corrects_invalid_hostname_to_top1(self):
        ranked = [
            {"k8s_hostname": "tcc-cluster-worker", "node_id": "tcc-cluster-worker", "score_total": 90},
            {"k8s_hostname": "worker2", "node_id": "worker2", "score_total": 70},
        ]
        spec = {"nodeSelector": {"kubernetes.io/hostname": "invalid-node"}}
        out, corr = _enforce_fn(spec, ranked)
        self.assertEqual(
            out["nodeSelector"]["kubernetes.io/hostname"],
            "tcc-cluster-worker",
        )
        self.assertIsNotNone(corr)
        self.assertEqual(corr["corrected_hostname"], "tcc-cluster-worker")

    def test_forces_rank1_when_llm_picks_other_top(self):
        ranked = [
            {"k8s_hostname": "tcc-cluster-worker", "node_id": "tcc-cluster-worker", "score_total": 90},
            {"k8s_hostname": "worker2", "node_id": "worker2", "score_total": 70},
        ]
        spec = {"nodeSelector": {"kubernetes.io/hostname": "worker2"}}
        out, corr = _enforce_fn(spec, ranked)
        self.assertEqual(
            out["nodeSelector"]["kubernetes.io/hostname"],
            "tcc-cluster-worker",
        )
        self.assertIsNotNone(corr)

    def test_keeps_rank1_hostname_without_correction_meta(self):
        ranked = [{"k8s_hostname": "worker", "node_id": "worker", "score_total": 80}]
        spec = {"nodeSelector": {"kubernetes.io/hostname": "worker"}}
        out, corr = _enforce_fn(spec, ranked)
        self.assertEqual(out["nodeSelector"]["kubernetes.io/hostname"], "worker")
        self.assertIsNone(corr)

    def test_empty_ranking_uses_fallback(self):
        spec = {"nodeSelector": {"kubernetes.io/hostname": "nowhere"}}
        out, corr = _enforce_fn(spec, [])
        host = out["nodeSelector"]["kubernetes.io/hostname"]
        self.assertIn(host, ("worker", "worker2", "tcc-cluster-worker"))
        self.assertIsNotNone(corr)


if __name__ == "__main__":
    unittest.main()
