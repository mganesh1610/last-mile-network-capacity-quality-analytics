import json
import sqlite3
import unittest
from pathlib import Path

from run_pipeline import OUT_DIR, run_pipeline


class TestLastMileNetworkPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.summary = run_pipeline()

    def test_row_conservation_and_integrity(self):
        self.assertEqual(
            self.summary["promoted_delivered_shipments"] + self.summary["quarantined_orders"],
            self.summary["raw_orders"],
        )
        conn = sqlite3.connect(OUT_DIR / "last_mile_network.db")
        self.assertEqual(conn.execute("PRAGMA integrity_check;").fetchone()[0], "ok")
        self.assertEqual(conn.execute("PRAGMA foreign_key_check;").fetchall(), [])
        conn.close()

    def test_all_dq_gates_pass(self):
        self.assertEqual(self.summary["dq_gates_passed"], self.summary["dq_gates_total"])

    def test_forecast_beats_naive(self):
        self.assertLess(self.summary["walk_forward_mape_pct"], self.summary["naive_weekly_mape_pct"])

    def test_artifacts_exist(self):
        for fname in (
            "last_mile_network.db",
            "tableau_last_mile_lane_kpis.csv",
            "tableau_weekly_capacity_forecast.csv",
            "last_mile_capacity_dq_scorecard.xlsx",
            "validation_summary.json",
        ):
            self.assertTrue((OUT_DIR / fname).exists(), f"Missing {fname}")
        loaded = json.loads((OUT_DIR / "validation_summary.json").read_text(encoding="utf-8"))
        self.assertEqual(loaded["promoted_delivered_shipments"], self.summary["promoted_delivered_shipments"])


if __name__ == "__main__":
    unittest.main()
