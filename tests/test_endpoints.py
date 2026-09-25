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
                return [[list("abcdefg"), ["000001ABCDEF", "1", "Item", "01/02", "", "", "10.00"]]]

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
                    ("store_file", ("stores.csv", b"1,Example", "text/csv")),
                ],
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(load_workbook(BytesIO(response.content)).sheetnames), 2)

    def test_templates_load_static_assets(self):
        response = self.client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIn('/static/index.css', response.text)
        self.assertEqual(self.client.get("/static/base.css").status_code, 200)

    def test_openapi_declares_real_response_types(self):
        paths = app.openapi()["paths"]

        self.assertIn("text/html", paths["/"]["get"]["responses"]["200"]["content"])
        self.assertIn(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            paths["/sales"]["post"]["responses"]["200"]["content"],
        )


if __name__ == "__main__":
    unittest.main()
