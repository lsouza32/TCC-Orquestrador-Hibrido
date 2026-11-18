"""Testes do módulo de logs e métricas [TCC-METRICS]."""

from __future__ import annotations

import unittest

from context_server.services.tcc_metrics import (
    compute_prediction_delta_ram,
    extract_k8s_resources_from_ai_raw,
    format_libraries_for_log,
)


class TestTccMetrics(unittest.TestCase):
    def test_compute_prediction_delta_ram(self):
        # 2 GiB estimado, pico 512 MiB = 0.5 GiB → delta 1.5
        delta = compute_prediction_delta_ram(2.0, 512.0)
        self.assertAlmostEqual(delta, 1.5, places=3)

    def test_compute_prediction_delta_ram_none(self):
        self.assertIsNone(compute_prediction_delta_ram(None, 512.0))

    def test_extract_k8s_from_raw(self):
        raw = '{"k8s_job_spec": {"cpu_request": "500m", "cpu_limit": "1", "memory_request": "512Mi", "memory_limit": "2Gi"}}'
        res = extract_k8s_resources_from_ai_raw(raw)
        self.assertEqual(res["cpu_request"], "500m")
        self.assertEqual(res["memory_limit"], "2Gi")

    def test_format_libraries_list(self):
        self.assertEqual(format_libraries_for_log(["numpy", "pandas"]), '["numpy", "pandas"]')


if __name__ == "__main__":
    unittest.main()
