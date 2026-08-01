import unittest

import pandas as pd

from src.fixed_yaml_generator import (
    AuditedJHRDataExtractor,
    ExtractionError,
    RowObservation,
)


class ValueTests(unittest.TestCase):
    def test_zero_occupancy_is_preserved_in_annual_mean(self) -> None:
        months = {
            f"{month:02d}": {
                "occupancy_pct": None,
                "adr_jpy": None,
                "revpar_jpy": None,
                "sales_total_mil_jpy": None,
            }
            for month in range(1, 13)
        }
        months["01"]["occupancy_pct"] = 0.0
        months["02"]["occupancy_pct"] = 50.0
        summary = AuditedJHRDataExtractor._annual_summary(months)
        self.assertEqual(summary["occupancy_pct_mean"], 25.0)
        self.assertEqual(summary["occupancy_pct_months"], 2)


class AggregationTests(unittest.TestCase):
    def test_hotel_rows_are_labelled_equal_weight_not_portfolio_weight(self) -> None:
        observations = [
            RowObservation(1, "occupancy_pct", (50.0,) + (None,) * 11),
            RowObservation(2, "occupancy_pct", (100.0,) + (None,) * 11),
        ]
        months, semantics = AuditedJHRDataExtractor._equal_weight_hotel_summary(
            observations, 2020
        )
        self.assertEqual(months["01"]["occupancy_pct"], 75.0)
        self.assertFalse(semantics["portfolio_weighted"])
        self.assertIn("not_comparable", semantics["comparability_status"])

    def test_duplicate_source_aggregate_rows_are_rejected(self) -> None:
        observations = [
            RowObservation(1, "adr_jpy", (10_000.0,) + (None,) * 11),
            RowObservation(2, "adr_jpy", (11_000.0,) + (None,) * 11),
        ]
        with self.assertRaises(ExtractionError):
            AuditedJHRDataExtractor._source_aggregate(observations, 2024)

    def test_sales_are_summed_but_adr_is_meaned(self) -> None:
        observations = [
            RowObservation(1, "sales_total_mil_jpy", (10.0,) + (None,) * 11),
            RowObservation(2, "sales_total_mil_jpy", (20.0,) + (None,) * 11),
            RowObservation(3, "adr_jpy", (8_000.0,) + (None,) * 11),
            RowObservation(4, "adr_jpy", (12_000.0,) + (None,) * 11),
        ]
        months, _ = AuditedJHRDataExtractor._equal_weight_hotel_summary(
            observations, 2020
        )
        self.assertEqual(months["01"]["sales_total_mil_jpy"], 30.0)
        self.assertEqual(months["01"]["adr_jpy"], 10_000.0)


class QualityTests(unittest.TestCase):
    def test_revpar_identity_mismatch_is_flagged(self) -> None:
        months = {
            "01": {
                "occupancy_pct": 50.0,
                "adr_jpy": 10_000.0,
                "revpar_jpy": 9_000.0,
                "sales_total_mil_jpy": None,
            }
        }
        flags = AuditedJHRDataExtractor._quality_flags(months)
        self.assertEqual(flags[0]["flag"], "revpar_identity_mismatch")

    def test_candidate_rows_preserve_zero(self) -> None:
        row = ["客室稼働率", "2024年"] + [0.0] + [None] * 11
        frame = pd.DataFrame([row])
        observations = AuditedJHRDataExtractor()._candidate_rows(frame, 2024)
        self.assertEqual(observations[0].values[0], 0.0)


if __name__ == "__main__":
    unittest.main()
