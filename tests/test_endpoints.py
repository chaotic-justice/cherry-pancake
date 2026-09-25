import unittest

from fastapi.testclient import TestClient

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

    def test_templates_load_static_assets(self):
        response = self.client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIn('/static/index.css', response.text)
        self.assertEqual(self.client.get("/static/base.css").status_code, 200)


if __name__ == "__main__":
    unittest.main()
