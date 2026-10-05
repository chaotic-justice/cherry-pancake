import unittest
from io import BytesIO
from unittest.mock import patch

import pandas as pd
from fastapi.testclient import TestClient
from openpyxl import load_workbook

from app.main import app


class EndpointTest(unittest.TestCase):
    client = TestClient(app)

    def test_costco_requires_a_pdf(self):
        response = self.client.post(
            "/costco",
            files=[
                ("files", ("report.txt", b"not a PDF", "text/plain")),
                ("store_file", ("stores.csv", b"1,Example", "text/csv")),
            ],
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("must be PDFs", response.text)

    def test_sales_requires_an_excel_workbook(self):
        response = self.client.post(
            "/sales",
            files={"file": ("sales.txt", b"not a workbook", "text/plain")},
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("must be an Excel workbook", response.text)

    def test_sales_rejects_an_unexpected_workbook_layout(self):
        workbook = BytesIO()
        pd.DataFrame([["only", 1, 2, 3, 4]], columns=list("abcde")).to_excel(
            workbook, index=False
        )

        response = self.client.post(
            "/sales",
            files={
                "file": (
                    "sales.xlsx",
                    workbook.getvalue(),
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("summary rows", response.text)

    def test_sales_accepts_the_expected_workbook_layout(self):
        workbook = BytesIO()
        pd.DataFrame(
            [
                ["Salesperson Jane", None, None, None, None],
                ["Period To Date:", 10, None, None, None],
                ["Year To Date:", 20, None, None, None],
                ["Prior Year:", 30, None, None, None],
                ["Period total", 10, None, None, None],
                ["Year total", 20, None, None, None],
                ["Prior total", 30, None, None, None],
                ["End", None, None, None, None],
            ],
            columns=list("abcde"),
        ).to_excel(workbook, index=False)

        response = self.client.post(
            "/sales",
            files={
                "file": (
                    "sales.xlsx",
                    workbook.getvalue(),
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(load_workbook(BytesIO(response.content)).sheetnames, ["Sales Report"])

    def test_costco_keeps_reports_with_the_same_payment_id(self):
        class Page:
            def extract_text(self):
                return "Date: 01/02/2026\nPayment: 123"

            def extract_tables(self):
                return [
                    [
                        list("abcdefg"),
                        ["000636ABCDEF", "1", "Item", "01/02", "", "", "10.00"],
                    ]
                ]

        class Pdf:
            pages = [Page()]

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return None

        with patch("pdfplumber.open", side_effect=lambda *_: Pdf()):
            response = self.client.post(
                "/costco",
                files=[
                    ("files", ("one.pdf", b"one", "application/pdf")),
                    ("files", ("two.pdf", b"two", "application/pdf")),
                ],
            )

        self.assertEqual(response.status_code, 200)
        workbook = load_workbook(BytesIO(response.content))
        self.assertEqual(len(workbook.sheetnames), 2)
        self.assertEqual(workbook[workbook.sheetnames[0]]["G2"].value, "C4#0636")

    def test_costco_assigns_negative_date_range_adjustments_to_cosnext(self):
        class Page:
            def extract_text(self):
                return "Date: 09/24/2026\nPayment: 456"

            def extract_tables(self):
                return [
                    [
                        list("abcdefg"),
                        [
                            "0141441151",
                            "8/3/26 - 8/30/26",
                            "",
                            "09/22/2026",
                            "-5,423.93",
                            "0.00",
                            "-5,423.93",
                        ],
                        [
                            "0141441152",
                            "8/3/26 - 8/30/26",
                            "",
                            "09/22/2026",
                            "100.00",
                            "0.00",
                            "100.00",
                        ],
                        [
                            "0141441153",
                            "12345",
                            "",
                            "09/22/2026",
                            "-25.00",
                            "0.00",
                            "-25.00",
                        ],
                    ]
                ]

        class Pdf:
            pages = [Page()]

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return None

        with patch("pdfplumber.open", side_effect=lambda *_: Pdf()):
            response = self.client.post(
                "/costco",
                files={"files": ("adjustments.pdf", b"pdf", "application/pdf")},
            )

        self.assertEqual(response.status_code, 200)
        workbook = load_workbook(BytesIO(response.content))
        sheet = workbook[workbook.sheetnames[0]]
        self.assertEqual(sheet["F2"].value, "0141")
        self.assertEqual(sheet["G2"].value, "COSNEXT")
        self.assertEqual(sheet["G3"].value, "C7#0141")
        self.assertEqual(sheet["G4"].value, "C7#0141")
        check_numbers = [
            row[1].value
            for row in sheet.iter_rows()
            if row[0].value == "Check Number"
        ]
        self.assertEqual(check_numbers, ["456"])

    def test_templates_load_static_assets(self):
        response = self.client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIn('/static/index.css', response.text)
        self.assertEqual(self.client.get("/static/base.css").status_code, 200)

    def test_costco_page_explains_optional_store_update(self):
        response = self.client.get("/costco")

        self.assertEqual(response.status_code, 200)
        self.assertIn("built-in Costco store list", response.text)
        self.assertNotRegex(
            response.text,
            r'<input[^>]*id="store-input"[^>]*\brequired\b',
        )

    def test_openapi_declares_real_response_types(self):
        paths = app.openapi()["paths"]

        self.assertIn("text/html", paths["/"]["get"]["responses"]["200"]["content"])
        self.assertIn(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            paths["/sales"]["post"]["responses"]["200"]["content"],
        )


if __name__ == "__main__":
    unittest.main()
