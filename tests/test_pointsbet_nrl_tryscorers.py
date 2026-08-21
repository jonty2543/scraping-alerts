import unittest

from scrapers.pointsbet_scrapers import parse_pointsbet_nrl_tryscorer_event


class PointsBetNrlTryscorerParsingTest(unittest.TestCase):
    def test_numeric_selection_ids_do_not_become_player_names(self):
        event = {
            "name": "Canberra Raiders v Brisbane Broncos",
            "startsAt": "2026-08-21T08:00:00Z",
            "fixedOddsMarkets": [
                {
                    "eventName": "Anytime Tryscorer",
                    "outcomes": [
                        {
                            "name": "62006901421837",
                            "playerName": "Xavier Savage",
                            "playerId": "62006901421837",
                            "price": 1.83,
                        }
                    ],
                },
                {
                    "eventName": "To Score 2+ Tries",
                    "outcomes": [
                        {
                            "name": "62006901421837",
                            "playerName": "Xavier Savage",
                            "playerId": "62006901421837",
                            "price": 4.0,
                        }
                    ],
                },
                {
                    "eventName": "To Score 3+ Tries",
                    "outcomes": [
                        {
                            "name": "62006901421837",
                            "playerName": "Xavier Savage",
                            "playerId": "62006901421837",
                            "price": 12.0,
                        }
                    ],
                },
            ],
        }

        market_key, prices = parse_pointsbet_nrl_tryscorer_event(event)

        self.assertEqual(market_key, ("Canberra Raiders v Brisbane Broncos", "2026-08-21"))
        self.assertEqual(
            prices,
            {
                "Xavier Savage 1+": 1.83,
                "Xavier Savage 2+": 4.0,
                "Xavier Savage 3+": 12.0,
            },
        )
        self.assertNotIn("62006901421837 1+", prices)


if __name__ == "__main__":
    unittest.main()
