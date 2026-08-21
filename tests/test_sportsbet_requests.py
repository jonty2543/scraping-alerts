import unittest
from unittest.mock import patch

from scrapers.sportsbet_scrapers import SBSportsScraper


class _Response:
    status_code = 200

    def json(self):
        return {"ok": True}


class SportsbetRequestsTest(unittest.TestCase):
    def test_direct_json_fetch_uses_cache_buster_and_no_cache_headers(self):
        calls = []

        def fake_get(url, headers=None, timeout=None):
            calls.append((url, headers or {}, timeout))
            return _Response()

        scraper = SBSportsScraper("https://example.test/events?foo=bar", chosen_date="2026-08-21")
        with patch("scrapers.sportsbet_scrapers.requests.get", fake_get):
            self.assertEqual(scraper._requests_json(scraper.url), {"ok": True})

        url, headers, timeout = calls[0]
        self.assertIn("foo=bar", url)
        self.assertRegex(url, r"[?&]_=\d+")
        self.assertIn("no-cache", headers["Cache-Control"])
        self.assertEqual(headers["Pragma"], "no-cache")
        self.assertEqual(timeout, 20)


if __name__ == "__main__":
    unittest.main()
