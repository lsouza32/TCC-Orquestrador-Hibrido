"""Testes da tipagem Edge/Fog/Cloud."""

from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]


def _load_compute_layer():
    ctx = types.ModuleType("context_server")
    ctx.__path__ = [str(_ROOT / "context_server")]
    sys.modules.setdefault("context_server", ctx)
    svc = types.ModuleType("context_server.services")
    svc.__path__ = [str(_ROOT / "context_server" / "services")]
    sys.modules.setdefault("context_server.services", svc)

    spec = importlib.util.spec_from_file_location(
        "context_server.services.compute_layer",
        _ROOT / "context_server" / "services" / "compute_layer.py",
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["context_server.services.compute_layer"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_compute_layer()


class TestComputeLayer(unittest.TestCase):
    def test_normalize_aliases(self):
        self.assertEqual(_mod.normalize_layer("Edge"), "edge")
        self.assertEqual(_mod.normalize_layer("névoa"), "fog")
        self.assertEqual(_mod.normalize_layer("nuvem"), "cloud")
        self.assertIsNone(_mod.normalize_layer("xyz"))

    def test_infer_capacity(self):
        self.assertEqual(_mod.infer_layer_from_capacity(1, 1.0), "edge")
        self.assertEqual(_mod.infer_layer_from_capacity(4, 4.0), "fog")
        self.assertEqual(_mod.infer_layer_from_capacity(8, 16.0), "cloud")

    def test_k8s_label(self):
        labels = {_mod.K8S_LAYER_LABEL: "fog"}
        self.assertEqual(_mod.infer_layer_from_k8s_labels(labels), "fog")

    def test_preferred_ml_heavy(self):
        self.assertEqual(
            _mod.preferred_layer_for_workload(task_type="ML-heavy", complexity_score=3),
            "cloud",
        )

    def test_preferred_light(self):
        self.assertEqual(
            _mod.preferred_layer_for_workload(
                task_type="IO-bound",
                complexity_score=2,
                required_memory_gb=0.5,
                required_cpu_threads=1.0,
            ),
            "edge",
        )

    def test_affinity_scores(self):
        self.assertEqual(_mod.layer_affinity_score("edge", "edge", 20.0), 20.0)
        self.assertEqual(_mod.layer_affinity_score("fog", "edge", 20.0), 10.0)
        self.assertEqual(_mod.layer_affinity_score("cloud", "edge", 20.0), 0.0)
        self.assertEqual(_mod.layer_affinity_score("cloud", None, 20.0), 20.0)

    def test_from_task_row_parses_analyst(self):
        import json

        row = {
            "ai_category": "geral",
            "required_memory_gb": 0.5,
            "required_cpu_threads": 1.0,
            "ai_raw_response": json.dumps(
                {
                    "multi_agent_trace": {
                        "analyst_output": {
                            "task_type": "ML-heavy",
                            "complexity_score": 9,
                        }
                    }
                }
            ),
        }
        self.assertEqual(_mod.preferred_layer_from_task_row(row), "cloud")


if __name__ == "__main__":
    unittest.main()
