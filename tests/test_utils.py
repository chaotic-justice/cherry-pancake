import unittest

import pandas as pd

from app.library.utils import get_store_names


class StoreNamesTest(unittest.TestCase):
    def test_loads_bundled_store_mapping(self):
        mapping = get_store_names()

        self.assertGreater(len(mapping), 400)
        self.assertEqual(mapping["0636"], "C4#0636")
        self.assertEqual(mapping["0766"], "C11#0766")
        self.assertEqual(mapping["1994"], "C991994")

    def test_normalizes_customer_number_padding(self):
        stores = pd.DataFrame(
            [
                ["C4#0636", "COSTCO", "#0636"],
                ["C4#636", "COSTCO", "#0636"],
                ["C11#766", "COSTCO", "#0766"],
                ["C9#0766", "COSTCO", "#0766"],
                ["C991994", "COSTCO", "#1994"],
                ["C7#1994", "COSTCO", "#1994"],
            ]
        )

        mapping = get_store_names(df=stores)

        self.assertEqual(mapping["0636"], "C4#0636")
        self.assertEqual(mapping["0766"], "C11#0766")
        self.assertEqual(mapping["1994"], "C991994")


if __name__ == "__main__":
    unittest.main()
