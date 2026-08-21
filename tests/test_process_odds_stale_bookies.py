import os
import unittest
from unittest.mock import patch

os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "dummy")

import functions


class _Response:
    def __init__(self, data=None):
        self.data = data or []


class _Table:
    def __init__(self, supabase, name):
        self.supabase = supabase
        self.name = name

    def select(self, *args, **kwargs):
        self.op = "select"
        return self

    def insert(self, records):
        self.op = "insert"
        self.records = records
        return self

    def upsert(self, records, **kwargs):
        self.op = "upsert"
        self.records = records
        self.kwargs = kwargs
        return self

    def delete(self):
        self.op = "delete"
        return self

    def eq(self, *args, **kwargs):
        return self

    def neq(self, *args, **kwargs):
        return self

    def lt(self, *args, **kwargs):
        return self

    def limit(self, *args, **kwargs):
        return self

    def execute(self):
        if self.op == "select":
            return _Response(self.supabase.table_data.get(self.name, []))
        if self.op == "upsert":
            self.supabase.upserts.setdefault(self.name, []).extend(self.records)
        return _Response([])


class _Supabase:
    def __init__(self):
        self.table_data = {}
        self.upserts = {}

    def table(self, name):
        return _Table(self, name)


class ProcessOddsStaleBookmakerTest(unittest.TestCase):
    def test_inactive_requested_bookie_is_written_as_zero_to_clear_stale_db_price(self):
        fake_supabase = _Supabase()
        fake_supabase.table_data["NRL Odds"] = [
            {
                "Match": "Canberra Raiders v Brisbane Broncos",
                "Date": "2026-08-21",
                "Result": "Canberra Raiders",
                "Sportsbet": 1.55,
                "Pointsbet": 1.6,
            },
            {
                "Match": "Canberra Raiders v Brisbane Broncos",
                "Date": "2026-08-21",
                "Result": "Brisbane Broncos",
                "Sportsbet": 2.4,
                "Pointsbet": 2.35,
            },
        ]

        bookmakers = {
            "Sportsbet": {},
            "Pointsbet": {
                ("Canberra Raiders v Brisbane Broncos", "2026-08-21"): {
                    "Canberra Raiders": 1.61,
                    "Brisbane Broncos": 2.36,
                }
            },
        }

        with patch.object(functions, "supabase", fake_supabase), patch.object(
            functions, "_cleanup_recent_flucs", lambda *args, **kwargs: None
        ):
            df, _ = functions.process_odds(
                bookmakers,
                ["Sportsbet", "Pointsbet"],
                table_name="NRL Odds",
                outcomes=2,
                upsert=True,
            )

        self.assertEqual(set(df["Sportsbet"].tolist()), {0.0})
        self.assertEqual(set(row["Sportsbet"] for row in fake_supabase.upserts["NRL Odds"]), {0.0})
        self.assertEqual(
            sorted(row["Pointsbet"] for row in fake_supabase.upserts["NRL Odds"]),
            [1.61, 2.36],
        )


if __name__ == "__main__":
    unittest.main()
