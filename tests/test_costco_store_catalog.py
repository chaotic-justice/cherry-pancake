import json
import unittest
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from openpyxl import Workbook

from app.library.costco_store_catalog import (
    CostcoStoreCatalog,
    StoreCatalogError,
    StoreCatalogUpdate,
    StoreResolution,
)


def _xlsx_bytes(rows: list[list[str]]) -> bytes:
    workbook = Workbook()
    worksheet = workbook.active
    for row in rows:
        worksheet.append(row)
    output = BytesIO()
    workbook.save(output)
    return output.getvalue()


class CostcoStoreCatalogTest(unittest.TestCase):
    def test_loads_bundled_defaults_and_authoritative_values(self):
        catalog = CostcoStoreCatalog.load()

        self.assertEqual(
            catalog.resolve_invoice("0636441151"),
            StoreResolution("0636", "C4#0636", known=True),
        )
        self.assertEqual(
            catalog.resolve_invoice("0766441151"),
            StoreResolution("0766", "C11#0766", known=True),
        )
        self.assertEqual(
            catalog.resolve_invoice("1994441151"),
            StoreResolution("1994", "C991994", known=True),
        )
        self.assertEqual(
            catalog.resolve_invoice("1997441151"),
            StoreResolution("1997", "C991997", known=True),
        )

    def test_csv_update_supports_two_columns_and_normalizes_duplicates(self):
        catalog = CostcoStoreCatalog.load(
            StoreCatalogUpdate(
                "stores.csv",
                b"C4#0636,#636\nC4#636,#0636\n"
                b"C9#0766,#0766\nC7#1994,#1994\nC2#999,#999\n",
            )
        )

        self.assertEqual(
            catalog.resolve_invoice("0636441151"),
            StoreResolution("0636", "C4#0636", known=True),
        )
        self.assertEqual(
            catalog.resolve_invoice("0999441151"),
            StoreResolution("0999", "C2#0999", known=True),
        )
        self.assertEqual(catalog.resolve_invoice("0766441151").store_name, "C11#0766")
        self.assertEqual(catalog.resolve_invoice("1994441151").store_name, "C991994")

    def test_csv_update_supports_three_column_costco_layout(self):
        catalog = CostcoStoreCatalog.load(
            StoreCatalogUpdate(
                "stores.csv",
                b"C8#0888,COSTCO,#0888\nC3#0888,COSTCO,#0888\n",
            )
        )

        self.assertEqual(
            catalog.resolve_invoice("0888441151"),
            StoreResolution("0888", "C3#0888", known=True),
        )

    def test_xlsx_update_uses_the_same_normalization_rules(self):
        catalog = CostcoStoreCatalog.load(
            StoreCatalogUpdate(
                "stores.xlsx",
                _xlsx_bytes([["C2#999", "COSTCO", "#999"]]),
            )
        )

        self.assertEqual(
            catalog.resolve_invoice("0999441151"),
            StoreResolution("0999", "C2#0999", known=True),
        )

    def test_resolves_both_invoice_layouts_and_legacy_trailing_zero_keys(self):
        catalog = CostcoStoreCatalog.load()

        self.assertEqual(catalog.resolve_invoice("0141441151").store_key, "0141")
        self.assertEqual(catalog.resolve_invoice("01414411510").store_key, "0141")
        self.assertEqual(catalog.resolve_invoice("06360441151").store_key, "0636")

    def test_unknown_invoice_has_an_explicit_result(self):
        catalog = CostcoStoreCatalog.load()

        self.assertEqual(
            catalog.resolve_invoice("9999441151"),
            StoreResolution("0000", "Unknown", known=False),
        )
        self.assertEqual(
            catalog.resolve_invoice("not-an-invoice"),
            StoreResolution("0000", "Unknown", known=False),
        )

    def test_rejects_unsupported_or_malformed_updates(self):
        with self.assertRaisesRegex(StoreCatalogError, "CSV, XLS, or XLSX"):
            CostcoStoreCatalog.load(StoreCatalogUpdate("stores.txt", b"x"))

        with self.assertRaisesRegex(StoreCatalogError, "at least two columns"):
            CostcoStoreCatalog.load(StoreCatalogUpdate("stores.csv", b"only-one\n"))

        with self.assertRaisesRegex(StoreCatalogError, "Could not read"):
            CostcoStoreCatalog.load(StoreCatalogUpdate("stores.xlsx", b"not-excel"))

    def test_corrupt_bundled_catalog_fails_closed(self):
        with TemporaryDirectory() as directory:
            catalog_path = Path(directory) / "costco-stores.json"
            catalog_path.write_text(json.dumps(["not", "an", "object"]))

            with patch(
                "app.library.costco_store_catalog._BUNDLED_CATALOG_PATH",
                catalog_path,
            ):
                with self.assertRaisesRegex(StoreCatalogError, "bundled Costco"):
                    CostcoStoreCatalog.load()


if __name__ == "__main__":
    unittest.main()
