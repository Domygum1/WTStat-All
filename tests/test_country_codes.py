import sqlite3
import unittest

from country_codes import COUNTRY_DATA, country_pair, migrate_samples, squad_pairs


class CountryCodesTest(unittest.TestCase):
    def test_all_research_trees_have_two_letter_codes_and_english_names(self):
        self.assertEqual(len(COUNTRY_DATA), 10)
        self.assertEqual(country_pair("美国"), ("US", "United States"))
        self.assertEqual(country_pair("中国"), ("CN", "China"))
        self.assertEqual(country_pair("苏联"), ("SU", "Soviet Union"))
        self.assertEqual(country_pair("英国"), ("GB", "United Kingdom"))
        for key, (code, name, chinese) in COUNTRY_DATA.items():
            with self.subTest(key=key):
                self.assertEqual(len(code), 2)
                self.assertTrue(code.isascii() and code.isupper())
                self.assertEqual(country_pair(key), (code, name))
                self.assertEqual(country_pair(chinese), (code, name))

    def test_legacy_rows_are_backfilled_without_changing_display_values(self):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.row_factory = sqlite3.Row
        connection.execute("CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT)")
        connection.execute("CREATE TABLE samples(sid TEXT,uid TEXT,country TEXT,"
                           "squad_countries TEXT,PRIMARY KEY(sid,uid))")
        connection.execute("INSERT INTO samples VALUES('abc','123','中国','[\"美国\",\"苏联\"]')")
        migrate_samples(connection)
        row = connection.execute("SELECT * FROM samples").fetchone()
        self.assertEqual(row["country"], "中国")
        self.assertEqual((row["country_code"], row["country_name"]), ("CN", "China"))
        self.assertEqual(row["squad_country_codes"], '["US", "SU"]')
        self.assertEqual(row["squad_country_names"], '["United States", "Soviet Union"]')
        self.assertEqual(squad_pairs(None), (None, None))
        migrate_samples(connection)
        self.assertEqual(connection.execute("SELECT COUNT(*) FROM settings "
                                            "WHERE key='country_codes_v1'").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
