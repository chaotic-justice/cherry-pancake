# Cherry Pancake

Cherry Pancake is a small, single-user bookkeeping toolbox. It turns recurring PDF and Excel exports into reviewed or cleaned Excel workbooks through a FastAPI web interface.

The current public instance is available at [cherry.billgatesironworks.com](https://cherry.billgatesironworks.com).

## Tools

| Route | Workflow | Input | Output |
| --- | --- | --- | --- |
| `/costco` | Costco payment reports | Up to 30 payment PDFs, plus an optional `.csv`, `.xls`, or `.xlsx` store mapping update | One dated Costco Excel workbook |
| `/costco-payroll` | Costco payroll master | Up to 30 dated `.xlsx` payroll workbooks | One dated master workbook with review and source-audit sheets |
| `/sales` | Salesperson performance | One sales workbook with the expected five-column summary layout | `Sales_Analysis.xlsx` with salesperson totals and a validation summary |
| `/raccoon` | Monthly reconciliation | VNB/AP workbooks and optional accounts-receivable workbooks | An in-browser review and one downloadable workbook per month |

Uploads are processed in memory. The application has no database, account system, or persistent upload storage.

## Stack

- Python 3.12+
- FastAPI and Uvicorn
- Jinja templates with shared CSS and vanilla JavaScript
- pandas and openpyxl for spreadsheet processing
- pdfplumber for Costco payment reports
- `uv` for dependency and command management

The frontend is intentionally server-rendered. Raccoon progressively enhances its form with `fetch` so processing results replace the page content without a full reload; the native HTML form remains the fallback.

## Run locally

Install [`uv`](https://docs.astral.sh/uv/), then install the locked dependencies:

```bash
uv sync
```

Start the local development server:

```bash
uv run fastapi dev app/main.py --host 127.0.0.1 --port 8000
```

Open <http://127.0.0.1:8000>. FastAPI's generated API documentation is available at <http://127.0.0.1:8000/docs>.

Templates are loaded when `app.main` is imported. Restart the application after editing Jinja templates. Static CSS and JavaScript are served directly, but a hard refresh or asset-version change may be needed after replacing a previously cached stylesheet.

## Run through the configured Cloudflare tunnel

The `Makefile` starts FastAPI and the existing `cherry-pancake` named tunnel together:

Public tunnel URL: <https://cherry.billgatesironworks.com>

```bash
make dev
```

This command requires `cloudflared` and local credentials for that named tunnel. It is specific to the current deployment and is not a general first-time tunnel setup command.

## Upload limits

The current server-side limits are designed for the existing bookkeeping exports:

- Costco: 30 payment PDFs maximum, 5 MB per PDF; the optional mapping file is also limited to 5 MB.
- Costco payroll: 30 Excel workbooks maximum, 5 MB per workbook.
- Salesperson performance: one Excel workbook, 5 MB maximum.
- Raccoon: 24 VNB/AP workbooks and 24 accounts-receivable workbooks maximum, 5 MB per workbook.

Client-side checks improve feedback, but server-side validation remains authoritative.

## Tests

Run the complete test suite with:

```bash
uv run python -m unittest discover -s tests -v
```

Check the browser JavaScript syntax with:

```bash
node --check app/static/costco.js
node --check app/static/costco_payroll.js
node --check app/static/sales.js
node --check app/static/raccoon.js
```

## Project layout

```text
app/
├── main.py                 # FastAPI routes and response contracts
├── routes/                 # Workflow request handling
├── library/                # PDF, spreadsheet, and reconciliation logic
├── templates/              # Jinja pages
├── static/                 # Shared and page-specific CSS/JavaScript
└── data/                   # Bundled Costco store catalog
tests/                       # Unit and endpoint tests
```
