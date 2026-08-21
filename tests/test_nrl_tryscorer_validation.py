import os
import unittest

import pandas as pd

os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "dummy")

from functions import _sanitize_tryscorer_rows


class NrlTryscorerValidationTest(unittest.TestCase):
    def test_rejects_numeric_results_and_recomputes_best_price(self):
        df = pd.DataFrame(
            [
                {
                    "Match": "Canberra Raiders v Brisbane Broncos",
                    "Date": "2026-08-21",
                    "Result": "62006901421837",
                    "Value": 62006901421837,
                    "Pointsbet": 1.83,
                    "Sportsbet": 0.0,
                    "Best Bookie": "Pointsbet",
                    "Best Price": 1.83,
                },
                {
                    "Match": "Canberra Raiders v Brisbane Broncos",
                    "Date": "2026-08-21",
                    "Result": "Xavier Savage",
                    "Value": 1.0,
                    "Pointsbet": 1.83,
                    "Sportsbet": 1.9,
                    "Best Bookie": "Pointsbet",
                    "Best Price": 99.0,
                },
                {
                    "Match": "Canberra Raiders v Brisbane Broncos",
                    "Date": "2026-08-21",
                    "Result": "Kaeo Weekes",
                    "Value": 4,
                    "Pointsbet": 2.1,
                    "Sportsbet": 2.2,
                    "Best Bookie": "Sportsbet",
                    "Best Price": 2.2,
                },
            ]
        )

        sanitized = _sanitize_tryscorer_rows(df, ["Pointsbet", "Sportsbet"])

        self.assertEqual(sanitized["Result"].tolist(), ["Xavier Savage"])
        self.assertEqual(sanitized["Value"].tolist(), [1])
        self.assertEqual(sanitized["Best Bookie"].tolist(), ["Sportsbet"])
        self.assertEqual(sanitized["Best Price"].tolist(), [1.9])


if __name__ == "__main__":
    unittest.main()
