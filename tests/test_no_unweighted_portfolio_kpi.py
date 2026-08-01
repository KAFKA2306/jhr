import unittest

from src.fixed_yaml_generator import AuditedJHRDataExtractor, RowObservation


class NoUnweightedPortfolioKpiTests(unittest.TestCase):
    def test_individual_rows_are_quarantined_without_aggregation(self):
        observations = [
            RowObservation(10, "occupancy_pct", (50.0,) * 12),
            RowObservation(20, "occupancy_pct", (90.0,) * 12),
            RowObservation(11, "adr_jpy", (10_000.0,) * 12),
            RowObservation(21, "adr_jpy", (30_000.0,) * 12),
        ]
        result = AuditedJHRDataExtractor._quarantine_individual_rows(
            observations, 2023
        )
        self.assertEqual(
            result["publication_status"],
            "quarantined_no_verified_portfolio_weights",
        )
        self.assertIsNone(result["monthly_data"]["01"]["occupancy_pct"])
        self.assertIsNone(result["monthly_data"]["01"]["adr_jpy"])
        self.assertIsNone(result["annual_summary"])

    def test_source_aggregate_rejects_duplicate_candidate_rows(self):
        observations = [
            RowObservation(1, "occupancy_pct", (80.0,) * 12),
            RowObservation(2, "occupancy_pct", (81.0,) * 12),
        ]
        with self.assertRaisesRegex(Exception, "multiple candidate aggregate rows"):
            AuditedJHRDataExtractor._source_aggregate(observations, 2024)


if __name__ == "__main__":
    unittest.main()
