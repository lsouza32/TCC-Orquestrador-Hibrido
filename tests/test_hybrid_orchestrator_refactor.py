"""Testes: success_rate, near_ceiling (parse Mi/Gi), margem RL e injeção do vencedor."""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
import tempfile
import types
import unittest
from pathlib import Path

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
    spec = importlib.util.spec_from_file_location(mod_name, _ROOT / rel)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return mod


_qty = _load_module("context_server.services.k8s_quantity", "context_server/services/k8s_quantity.py")
_guard = _load_module("context_server.services.strategist_guard", "context_server/services/strategist_guard.py")


class TestK8sMemoryParse(unittest.TestCase):
    def test_mi_gi_m_to_mb(self):
        self.assertAlmostEqual(_qty.parse_memory_quantity_to_mb("512Mi"), 512.0)
        self.assertAlmostEqual(_qty.parse_memory_quantity_to_mb("1Gi"), 1024.0)
        self.assertAlmostEqual(_qty.parse_memory_quantity_to_mb("256M"), 256.0)
        self.assertIsNone(_qty.parse_memory_quantity_to_mb("lixo"))
        self.assertIsNone(_qty.parse_memory_quantity_to_mb(None))


class TestWinnerInjection(unittest.TestCase):
    def test_forces_rank1_even_if_llm_picks_rank2(self):
        ranked = [
            {"k8s_hostname": "worker-a", "node_id": "worker-a", "score_total": 90},
            {"k8s_hostname": "worker-b", "node_id": "worker-b", "score_total": 70},
        ]
        spec = {"nodeSelector": {"kubernetes.io/hostname": "worker-b"}}
        out, meta = _guard.apply_ranked_winner_node_selector(spec, ranked)
        self.assertEqual(out["nodeSelector"]["kubernetes.io/hostname"], "worker-a")
        self.assertIsNotNone(meta)
        self.assertTrue(meta.get("overrode_llm"))

    def test_corrects_hallucinated_hostname(self):
        ranked = [{"k8s_hostname": "tcc-cluster-worker", "node_id": "tcc-cluster-worker"}]
        spec = {"nodeSelector": {"kubernetes.io/hostname": "invalid-node"}}
        out, meta = _guard.apply_ranked_winner_node_selector(spec, ranked)
        self.assertEqual(out["nodeSelector"]["kubernetes.io/hostname"], "tcc-cluster-worker")
        self.assertIsNotNone(meta)


class TestSafetyMargin(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Carrega ai_service com stubs mínimos (sem Flask/Ollama reais).
        _ensure_pkg()
        cfg = types.ModuleType("context_server.config")
        cfg.DATABASE_PATH = ":memory:"
        cfg.STRATEGIST_FALLBACK_HOSTNAMES = ["worker"]
        sys.modules["context_server.config"] = cfg

        log_mod = types.ModuleType("context_server.logging_config")
        log_mod.get_logger = lambda name=None: types.SimpleNamespace(
            warning=lambda *a, **k: None,
            info=lambda *a, **k: None,
            debug=lambda *a, **k: None,
        )
        sys.modules["context_server.logging_config"] = log_mod

        crud_mod = types.ModuleType("context_server.database.crud")
        sys.modules["context_server.database.crud"] = crud_mod
        db_pkg = sys.modules["context_server.database"]
        db_pkg.crud = crud_mod

        tcc = types.ModuleType("context_server.services.tcc_metrics")
        tcc.log_planejamento = lambda *a, **k: None
        sys.modules["context_server.services.tcc_metrics"] = tcc

        # node_ranker já testado isolado; stub leve
        nr = types.ModuleType("context_server.services.node_ranker")
        nr.format_ranking_rationale_pt = lambda *a, **k: ""
        nr.get_top_nodes_for_task = lambda *a, **k: []
        nr.log_sched_inspector_decision = lambda *a, **k: None
        nr.filter_kubernetes_nodes = lambda nodes: [
            n for n in (nodes or [])
            if isinstance(n, dict) and (n.get("provider_type") or "") == "kubernetes"
        ]
        sys.modules["context_server.services.node_ranker"] = nr

        sg = types.ModuleType("context_server.services.strategist_guard")
        sg.apply_ranked_winner_node_selector = _guard.apply_ranked_winner_node_selector
        sg.enforce_strategist_node_selector = _guard.enforce_strategist_node_selector
        sys.modules["context_server.services.strategist_guard"] = sg

        sys.modules.setdefault("requests", types.ModuleType("requests"))

        cls.ai = _load_module(
            "context_server.services.ai_service_margin_test",
            "context_server/services/ai_service.py",
        )

    def test_margin_applies_1_25_capped_at_2x(self):
        cpu, ram, meta = self.ai.apply_history_safety_margin(
            2.0,
            4.0,
            {"samples": 10, "near_ceiling": True, "near_ceiling_ratio": 0.5, "near_ceiling_count": 5},
        )
        self.assertTrue(meta["applied"])
        self.assertAlmostEqual(cpu, 2.0 * 1.25)
        self.assertAlmostEqual(ram, 4.0 * 1.25)
        self.assertLessEqual(cpu, 2.0 * 2.0)
        self.assertLessEqual(ram, 4.0 * 2.0)

    def test_margin_not_applied_without_ceiling(self):
        cpu, ram, meta = self.ai.apply_history_safety_margin(
            2.0, 4.0, {"samples": 10, "near_ceiling": False, "near_ceiling_ratio": 0.0}
        )
        self.assertFalse(meta["applied"])
        self.assertEqual(cpu, 2.0)
        self.assertEqual(ram, 4.0)


def _init_mini_db(path: str):
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE tasks (
            task_id TEXT PRIMARY KEY,
            docker_image TEXT,
            ai_detected_libraries TEXT,
            ai_raw_response TEXT
        );
        CREATE TABLE task_results (
            task_id TEXT PRIMARY KEY,
            status_final TEXT,
            detected_libraries TEXT,
            cpu_peak_percent REAL,
            ram_peak_mb REAL,
            completed_at REAL
        );
        """
    )
    conn.commit()
    conn.close()


class TestHistoryCrud(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_pkg()
        base = _load_module("context_server.database.base", "context_server/database/base.py")
        # tasks_crud imports get_db_connection from crud — stub circular-safe
        crud_stub = types.ModuleType("context_server.database.crud")
        crud_stub.get_db_connection = base.get_db_connection
        sys.modules["context_server.database.crud"] = crud_stub
        cls.tasks = _load_module(
            "context_server.database.tasks_crud_hist_test",
            "context_server/database/tasks_crud.py",
        )

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.db = self.tmp.name
        _init_mini_db(self.db)

    def _insert(self, task_id, image, libs, status, cpu_peak, ram_peak, mem_limit, cpu_limit="1"):
        raw = json.dumps({
            "k8s_job_spec": {
                "memory_limit": mem_limit,
                "cpu_limit": cpu_limit,
            }
        })
        conn = sqlite3.connect(self.db)
        conn.execute(
            "INSERT INTO tasks VALUES (?,?,?,?)",
            (task_id, image, json.dumps(libs), raw),
        )
        conn.execute(
            "INSERT INTO task_results VALUES (?,?,?,?,?,?)",
            (task_id, status, json.dumps(libs), cpu_peak, ram_peak, 1_700_000_000.0),
        )
        conn.commit()
        conn.close()

    def test_success_rate_by_image(self):
        self._insert("t1", "img-a:latest", ["numpy"], "concluida", 50, 100, "512Mi")
        self._insert("t2", "img-a:latest", ["numpy"], "falha", 50, 100, "512Mi")
        self._insert("t3", "img-b:latest", ["numpy"], "concluida", 50, 100, "512Mi")
        stats = self.tasks.get_docker_image_success_stats_for_libraries(self.db, ["numpy"])
        by_img = {s["docker_image"]: s for s in stats}
        self.assertAlmostEqual(by_img["img-a:latest"]["success_rate"], 0.5)
        self.assertEqual(by_img["img-a:latest"]["total"], 2)
        self.assertAlmostEqual(by_img["img-b:latest"]["success_rate"], 1.0)

    def test_near_ceiling_ram_with_gi_parse(self):
        # 900 MB peak vs 1Gi (=1024 Mi) → 900/1024 ≈ 87.9% > 85%
        self._insert("t1", "python:3.9-slim", ["pandas"], "concluida", 40, 900, "1Gi")
        signals = self.tasks.get_resource_ceiling_signals_for_libraries(self.db, ["pandas"])
        self.assertTrue(signals["near_ceiling"])
        self.assertGreaterEqual(signals["near_ceiling_count"], 1)
        detail = signals["samples_detail"][0]
        self.assertAlmostEqual(detail["memory_limit_mb"], 1024.0)

    def test_near_ceiling_with_512mi(self):
        # 450 MB vs 512Mi → ~87.9%
        self._insert("t1", "python:3.9-slim", ["sklearn"], "concluida", 40, 450, "512Mi")
        signals = self.tasks.get_resource_ceiling_signals_for_libraries(self.db, ["sklearn"])
        self.assertTrue(signals["near_ceiling"])

    def test_no_near_ceiling_when_well_below(self):
        self._insert("t1", "python:3.9-slim", ["requests"], "concluida", 10, 100, "1Gi")
        signals = self.tasks.get_resource_ceiling_signals_for_libraries(self.db, ["requests"])
        self.assertFalse(signals["near_ceiling"])


class TestQueueBestEffort(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_pkg()
        log_mod = types.ModuleType("context_server.logging_config")

        class _L:
            def warning(self, *a, **k): pass
            def info(self, *a, **k): pass
            def debug(self, *a, **k): pass

        log_mod.get_logger = lambda name=None: _L()
        log_mod.log_error_with_context = lambda *a, **k: None
        log_mod.log_success = lambda *a, **k: None
        sys.modules["context_server.logging_config"] = log_mod

        cfg = types.ModuleType("context_server.config")
        cfg.DATABASE_PATH = ":memory:"
        cfg.TASK_REQUIREMENTS = {}
        sys.modules["context_server.config"] = cfg

        _load_module("context_server.services.k8s_quantity", "context_server/services/k8s_quantity.py")
        nr = _load_module("context_server.services.node_ranker", "context_server/services/node_ranker.py")
        _load_module("context_server.services.strategist_guard", "context_server/services/strategist_guard.py")

        # Stubs para deps do queue_processor
        rt = types.ModuleType("context_server.realtime")
        rt.sse_hub = types.SimpleNamespace(publish=lambda *a, **k: None)
        sys.modules["context_server.realtime"] = rt

        crud = types.ModuleType("context_server.database.crud")
        crud.get_completed_task_counts_by_node_for_image = lambda *a, **k: {}
        sys.modules["context_server.database.crud"] = crud
        sys.modules["context_server.database"].crud = crud

        k8s_exec = types.ModuleType("context_server.services.k8s_job_executor")
        k8s_exec.allocation_id_for_kubernetes_task = lambda tid, hostname=None: (
            f"k8s:{hostname}" if hostname else f"k8s:{tid}"
        )
        k8s_exec.create_kubernetes_job = lambda *a, **k: (True, "job", None, {"nodeSelector": {"kubernetes.io/hostname": "small"}})
        k8s_exec.extract_k8s_job_spec = lambda row: {}
        k8s_exec.is_kubernetes_configured = lambda: True
        sys.modules["context_server.services.k8s_job_executor"] = k8s_exec

        cls.qp = _load_module(
            "context_server.queue_processor_test",
            "context_server/queue_processor.py",
        )
        cls.nr = nr

    def test_clamp_and_rerank_when_oversize(self):
        nodes = [
            {
                "node_id": "small",
                "hostname": "small",
                "provider_type": "node_agent",
                "cpu_total_threads": 4,
                "memory_total_gb": 4.0,
                "disk_total_gb": 20.0,
                "cpu_available_percent": 100.0,
                "memory_available_gb": 4.0,
                "disk_available_gb": 20.0,
                "cpu_reserved_threads": 0,
                "memory_reserved_gb": 0,
                "disk_reserved_gb": 0,
            }
        ]
        task_req = {
            "required_cpu_threads": 4.5,
            "required_memory_gb": 4.5,
            "required_disk_gb": 0.0,
            "suggested_docker_image": "python:3.9-slim",
            "image_warm_counts": {},
        }
        # Filtro duro puro falha
        self.assertEqual(self.nr.get_top_nodes_for_task(task_req, nodes), [])
        ranked, final_req, _hints = self.qp._rank_with_optional_best_effort(
            "task-x", task_req, nodes, {}
        )
        self.assertIsNotNone(ranked)
        self.assertEqual(ranked[0]["k8s_hostname"], "small")
        self.assertLessEqual(final_req["required_memory_gb"], 4.0)
        self.assertLessEqual(final_req["required_cpu_threads"], 4.0)


class TestDispatchPersistenceAndWarm(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _ensure_pkg()
        cfg = types.ModuleType("context_server.config")
        cfg.DATABASE_PATH = ":memory:"
        cfg.K8S_NAMESPACE = "default"
        sys.modules["context_server.config"] = cfg

        tel = types.ModuleType("context_server.services.k8s_telemetry")
        tel.prestop_lifecycle_command = lambda: ["true"]
        tel.retention_shell_suffix = lambda: "true"
        sys.modules["context_server.services.k8s_telemetry"] = tel

        _load_module("context_server.services.strategist_guard", "context_server/services/strategist_guard.py")

        base = _load_module("context_server.database.base", "context_server/database/base.py")
        crud_stub = types.ModuleType("context_server.database.crud")
        crud_stub.get_db_connection = base.get_db_connection
        sys.modules["context_server.database.crud"] = crud_stub
        cls.tasks = _load_module(
            "context_server.database.tasks_crud_dispatch_test",
            "context_server/database/tasks_crud.py",
        )
        cls.k8s = _load_module(
            "context_server.services.k8s_job_executor_resolve_test",
            "context_server/services/k8s_job_executor.py",
        )

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.db = self.tmp.name
        conn = sqlite3.connect(self.db)
        conn.executescript(
            """
            CREATE TABLE tasks (
                task_id TEXT PRIMARY KEY,
                docker_image TEXT,
                allocated_node_id TEXT,
                ai_raw_response TEXT,
                required_cpu_threads REAL,
                required_memory_gb REAL
            );
            CREATE TABLE task_results (
                task_id TEXT PRIMARY KEY,
                status_final TEXT,
                completed_at REAL
            );
            """
        )
        conn.commit()
        conn.close()

    def test_preserve_fresh_nodeselector_over_stale_ranking(self):
        task_row = {
            "ai_raw_response": json.dumps({
                "multi_agent_trace": {
                    "node_ranker": {
                        "top_nodes": [
                            {"k8s_hostname": "stale-worker", "node_id": "stale-worker"},
                        ]
                    }
                }
            })
        }
        spec = {"nodeSelector": {"kubernetes.io/hostname": "fresh-worker"}, "cpu_limit": "1"}
        out, meta = self.k8s.resolve_job_node_selector(spec, task_row)
        self.assertEqual(out["nodeSelector"]["kubernetes.io/hostname"], "fresh-worker")
        self.assertIsNone(meta)

    def test_fallback_to_stale_ranking_when_no_selector(self):
        task_row = {
            "ai_raw_response": json.dumps({
                "multi_agent_trace": {
                    "node_ranker": {
                        "top_nodes": [
                            {"k8s_hostname": "planned-worker", "node_id": "planned-worker"},
                        ]
                    }
                }
            })
        }
        out, meta = self.k8s.resolve_job_node_selector({}, task_row)
        self.assertEqual(out["nodeSelector"]["kubernetes.io/hostname"], "planned-worker")
        self.assertIsNotNone(meta)

    def test_allocation_id_uses_hostname(self):
        self.assertEqual(
            self.k8s.allocation_id_for_kubernetes_task("uuid-1", hostname="worker"),
            "k8s:worker",
        )
        self.assertEqual(
            self.k8s.allocation_id_for_kubernetes_task("uuid-1"),
            "k8s:uuid-1",
        )

    def test_warm_counts_normalize_k8s_prefix(self):
        conn = sqlite3.connect(self.db)
        conn.execute(
            "INSERT INTO tasks VALUES (?,?,?,?,?,?)",
            ("t1", "img:latest", "k8s:worker", "{}", None, None),
        )
        conn.execute(
            "INSERT INTO task_results VALUES (?,?,?)",
            ("t1", "concluida", 1.0),
        )
        conn.commit()
        conn.close()
        counts = self.tasks.get_completed_task_counts_by_node_for_image(self.db, "img:latest")
        self.assertGreaterEqual(counts.get("worker", 0), 1)

    def test_persist_effective_spec_updates_ai_raw_and_required(self):
        conn = sqlite3.connect(self.db)
        conn.execute(
            "INSERT INTO tasks VALUES (?,?,?,?,?,?)",
            (
                "t1",
                "img:latest",
                None,
                json.dumps({"k8s_job_spec": {"cpu_limit": "8", "memory_limit": "8Gi"}}),
                8.0,
                8.0,
            ),
        )
        conn.commit()
        conn.close()
        effective = {
            "cpu_limit": "2",
            "memory_limit": "2Gi",
            "nodeSelector": {"kubernetes.io/hostname": "worker"},
        }
        self.tasks.persist_effective_k8s_dispatch(
            self.db,
            "t1",
            effective,
            required_cpu_threads=2.0,
            required_memory_gb=2.0,
            dispatch_meta={"injected_hostname": "worker"},
        )
        conn = sqlite3.connect(self.db)
        row = conn.execute("SELECT * FROM tasks WHERE task_id='t1'").fetchone()
        conn.close()
        data = json.loads(row[3])
        self.assertEqual(data["k8s_job_spec"]["cpu_limit"], "2")
        self.assertEqual(data["k8s_job_spec"]["nodeSelector"]["kubernetes.io/hostname"], "worker")
        self.assertEqual(row[4], 2.0)
        self.assertEqual(row[5], 2.0)


if __name__ == "__main__":
    unittest.main()
