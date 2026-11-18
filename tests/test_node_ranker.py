"""Testes unitários do ranking híbrido de nós (sem Ollama / sem DB / sem Flask)."""

from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]


def _load_node_ranker_module():
    """Carrega node_ranker sem puxar Flask via context_server/__init__.py."""
    ctx = types.ModuleType("context_server")
    ctx.__path__ = [str(_ROOT / "context_server")]
    sys.modules.setdefault("context_server", ctx)
    svc = types.ModuleType("context_server.services")
    svc.__path__ = [str(_ROOT / "context_server" / "services")]
    sys.modules.setdefault("context_server.services", svc)

    qty_spec = importlib.util.spec_from_file_location(
        "context_server.services.k8s_quantity",
        _ROOT / "context_server" / "services" / "k8s_quantity.py",
    )
    assert qty_spec and qty_spec.loader
    qty_mod = importlib.util.module_from_spec(qty_spec)
    sys.modules["context_server.services.k8s_quantity"] = qty_mod
    qty_spec.loader.exec_module(qty_mod)

    cl_spec = importlib.util.spec_from_file_location(
        "context_server.services.compute_layer",
        _ROOT / "context_server" / "services" / "compute_layer.py",
    )
    assert cl_spec and cl_spec.loader
    cl_mod = importlib.util.module_from_spec(cl_spec)
    sys.modules["context_server.services.compute_layer"] = cl_mod
    cl_spec.loader.exec_module(cl_mod)

    # logging_config mínimo para o import do node_ranker
    log_mod = types.ModuleType("context_server.logging_config")

    def get_logger(_name):
        import logging

        return logging.getLogger(_name)

    log_mod.get_logger = get_logger
    sys.modules["context_server.logging_config"] = log_mod

    nr_spec = importlib.util.spec_from_file_location(
        "_node_ranker_test_mod",
        _ROOT / "context_server" / "services" / "node_ranker.py",
    )
    assert nr_spec and nr_spec.loader
    mod = importlib.util.module_from_spec(nr_spec)
    nr_spec.loader.exec_module(mod)
    return mod


_mod = _load_node_ranker_module()
get_top_nodes_for_task = _mod.get_top_nodes_for_task
format_ranking_rationale_pt = _mod.format_ranking_rationale_pt
_min_max_norm = _mod._min_max_norm
_parse_task_requirements = _mod._parse_task_requirements


def _node(
    nid: str,
    *,
    threads: int = 8,
    ram_avail: float = 4.0,
    cpu_pct: float = 100.0,
    disk_avail: float = 20.0,
    provider_type: str = "node_agent",
    compute_layer=None,
    memory_total_gb: float = 16.0,
) -> dict:
    return {
        "node_id": nid,
        "hostname": nid,
        "provider_type": provider_type,
        "compute_layer": compute_layer,
        "cpu_total_threads": threads,
        "memory_total_gb": memory_total_gb,
        "disk_total_gb": 100.0,
        "cpu_available_percent": cpu_pct,
        "memory_available_gb": ram_avail,
        "disk_available_gb": disk_avail,
        "cpu_reserved_threads": 0,
        "memory_reserved_gb": 0,
        "disk_reserved_gb": 0,
    }


class TestNodeRanker(unittest.TestCase):
    def test_hard_filter_drops_insufficient_ram(self):
        nodes = [
            _node("a", ram_avail=1.0),
            _node("b", ram_avail=8.0),
        ]
        req = {
            "required_memory_gb": 4.0,
            "required_cpu_threads": 1.0,
            "image_warm_counts": {},
        }
        top = get_top_nodes_for_task(req, nodes)
        self.assertEqual(len(top), 1)
        self.assertEqual(top[0]["node_id"], "b")

    def test_top_three_order_by_score(self):
        nodes = [
            _node("low", ram_avail=4.0, cpu_pct=50.0, disk_avail=10.0),
            _node("mid", ram_avail=6.0, cpu_pct=60.0, disk_avail=15.0),
            _node("high", ram_avail=12.0, cpu_pct=90.0, disk_avail=40.0),
            _node("x4", ram_avail=5.0, cpu_pct=55.0, disk_avail=12.0),
        ]
        req = {
            "required_memory_gb": 2.0,
            "required_cpu_threads": 1.0,
            "image_warm_counts": {},
            "required_disk_gb": 0.5,
        }
        top = get_top_nodes_for_task(req, nodes)
        self.assertLessEqual(len(top), 3)
        self.assertEqual(top[0]["node_id"], "high")
        scores = [x["score_total"] for x in top]
        self.assertEqual(scores, sorted(scores, reverse=True))

    def test_warm_image_boosts_score(self):
        nodes = [
            _node("cold", ram_avail=8.0, cpu_pct=80.0, disk_avail=30.0),
            _node("warm", ram_avail=8.0, cpu_pct=80.0, disk_avail=30.0),
        ]
        req = {
            "required_memory_gb": 2.0,
            "required_cpu_threads": 1.0,
            "suggested_docker_image": "python:3.11-slim",
            "image_warm_counts": {"warm": 5, "cold": 0},
        }
        top = get_top_nodes_for_task(req, nodes)
        self.assertEqual(top[0]["node_id"], "warm")
        self.assertGreater(top[0]["score_image_affinity"], 0.0)

    def test_single_candidate_max_resource_score(self):
        nodes = [_node("only", ram_avail=8.0, cpu_pct=90.0, disk_avail=40.0)]
        req = {
            "required_memory_gb": 2.0,
            "required_cpu_threads": 1.0,
            "image_warm_counts": {},
        }
        top = get_top_nodes_for_task(req, nodes)
        self.assertEqual(len(top), 1)
        self.assertEqual(top[0]["score_resources"], 40.0)
        self.assertEqual(top[0]["score_disk"], 15.0)
        self.assertEqual(top[0]["score_layer"], 20.0)

    def test_preferred_layer_boosts_matching_node(self):
        nodes = [
            _node("edge-node", ram_avail=8.0, cpu_pct=80.0, disk_avail=30.0, compute_layer="edge"),
            _node("cloud-node", ram_avail=8.0, cpu_pct=80.0, disk_avail=30.0, compute_layer="cloud"),
        ]
        req = {
            "required_memory_gb": 2.0,
            "required_cpu_threads": 1.0,
            "image_warm_counts": {},
            "preferred_layer": "edge",
        }
        top = get_top_nodes_for_task(req, nodes)
        self.assertEqual(top[0]["node_id"], "edge-node")
        self.assertGreater(top[0]["score_layer"], top[1]["score_layer"])

    def test_ml_heavy_prefers_cloud_via_layer_score(self):
        nodes = [
            _node("edge-node", ram_avail=8.0, cpu_pct=80.0, disk_avail=30.0, compute_layer="edge"),
            _node("cloud-node", ram_avail=8.0, cpu_pct=80.0, disk_avail=30.0, compute_layer="cloud"),
        ]
        req = {
            "required_memory_gb": 2.0,
            "required_cpu_threads": 1.0,
            "image_warm_counts": {},
            "preferred_layer": "cloud",
        }
        top = get_top_nodes_for_task(req, nodes)
        self.assertEqual(top[0]["node_id"], "cloud-node")
        self.assertEqual(top[0]["compute_layer"], "cloud")

    def test_parse_k8s_memory_requirement(self):
        ram, cpu, disk = _parse_task_requirements(
            {"required_memory_gb": "512Mi", "required_cpu_threads": "500m", "required_disk_gb": "1Gi"}
        )
        self.assertAlmostEqual(ram, 0.5, places=3)
        self.assertAlmostEqual(cpu, 0.5, places=3)
        self.assertAlmostEqual(disk, 1.0, places=3)

    def test_kubernetes_null_telemetry_uses_safety_factor(self):
        node = {
            "node_id": "tcc-cluster-worker",
            "hostname": "tcc-cluster-worker",
            "provider_type": "kubernetes",
            "cpu_total_threads": 8,
            "memory_total_gb": 16.0,
            "disk_total_gb": 100.0,
            "cpu_available_percent": None,
            "memory_available_gb": None,
            "disk_available_gb": None,
        }
        eff_t, eff_r, eff_d = _mod._effective_resources(node)
        self.assertAlmostEqual(eff_t, 4.0, places=2)
        self.assertAlmostEqual(eff_r, 8.0, places=2)
        self.assertAlmostEqual(eff_d, 50.0, places=2)

    def test_min_max_norm_single_value_is_one(self):
        self.assertEqual(_min_max_norm([42.0], 42.0), 1.0)

    def test_rationale_mentions_fallback_when_empty(self):
        text = format_ranking_rationale_pt([], {"required_memory_gb": 8.0, "required_cpu_threads": 4.0})
        self.assertIn("fallback", text.lower())

    def test_load_deviation_fields_on_ranked_nodes(self):
        nodes = [
            _node("light", ram_avail=14.0, cpu_pct=95.0),
            _node("heavy", ram_avail=2.0, cpu_pct=30.0),
        ]
        req = {
            "required_memory_gb": 1.0,
            "required_cpu_threads": 1.0,
            "image_warm_counts": {},
        }
        top = get_top_nodes_for_task(req, nodes)
        self.assertGreaterEqual(len(top), 1)
        for row in top:
            self.assertIn("load_deviation_cpu", row)
            self.assertIn("load_deviation_mem", row)
            self.assertIn("cluster_load_cpu_avg", row)
        best = top[0]
        self.assertEqual(best["node_id"], "light")
        self.assertGreaterEqual(best["load_deviation_cpu"], 0.0)
        self.assertGreaterEqual(best["load_deviation_mem"], 0.0)

    def test_rationale_includes_load_deviation(self):
        nodes = [_node("a", ram_avail=8.0, cpu_pct=80.0)]
        req = {"required_memory_gb": 2.0, "required_cpu_threads": 1.0, "image_warm_counts": {}}
        top = get_top_nodes_for_task(req, nodes)
        text = format_ranking_rationale_pt(top, req)
        self.assertIn("dvCpu", text)

    def test_filter_kubernetes_nodes_excludes_node_agent(self):
        nodes = [
            _node("PC-lucas", provider_type="node_agent"),
            {
                "node_id": "tcc-cluster-worker",
                "hostname": "tcc-cluster-worker",
                "provider_type": "kubernetes",
                "cpu_total_threads": 4,
                "memory_total_gb": 8.0,
                "disk_total_gb": 50.0,
                "cpu_available_percent": 50.0,
                "memory_available_gb": 4.0,
                "disk_available_gb": 25.0,
            },
        ]
        only_k8s = _mod.filter_kubernetes_nodes(nodes)
        self.assertEqual(len(only_k8s), 1)
        self.assertEqual(only_k8s[0]["node_id"], "tcc-cluster-worker")


if __name__ == "__main__":
    unittest.main()
