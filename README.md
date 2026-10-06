# Cherry Pancake

Cherry Pancake is a small, single-user bookkeeping toolbox. It turns recurring PDF and Excel exports into reviewed or cleaned Excel workbooks through a FastAPI web interface.

The current public instance is available at [cherry.billgatesironworks.com](https://cherry.billgatesironworks.com).

## Tools

| Route | Workflow | Input | Output |
| --- | --- | --- | --- |
| `/costco` | Costco payment reports | Up to 30 payment PDFs, plus an optional `.csv`, `.xls`, or `.xlsx` store mapping update | One dated Costco Excel workbook |
| `/costco-payroll` | Costco payroll master | Up to 30 dated `.xlsx` payroll workbooks | An HTML master report and dated Excel audit workbook |
| `/sales` | Salesperson performance | One sales workbook with the expected five-column summary layout | `Sales_Analysis.xlsx` with salesperson totals and a validation summary |
| `/raccoon` | Monthly reconciliation | VNB/AP workbooks and optional accounts-receivable workbooks | An in-browser review and one downloadable workbook per month |

Most uploads are processed only in memory. Costco payroll source workbooks are revisioned in SQLite at `.local-data/cherry-pancake.db`; generated reports are rebuilt from those saved sources and are not stored separately. The application has no account system.

### Costco payroll master

Upload each month's dated `.xlsx` files from the unzipped Costco payroll folder. The service saves immutable workbook revisions in SQLite and rebuilds the report from the latest revision of every filename, so earlier months do not need to be uploaded again. It reads each active worksheet, imports both the final payroll and advance-payment sections, and matches rows by warehouse, salesperson, and show dates. A later final payroll row replaces its matching advance row; `Copy of` files are treated as later same-date revisions. Existing calculated commission values are copied from Excel rather than recalculated.

On success, the browser displays a consolidated HTML report with final, provisional-only, review, and source-row counts; the unresolved rows; and the complete payroll master organized by payroll date. A download button provides `Costco_Payroll_Master_<latest-date>.xlsx`, which contains:

- `Payroll Master`: one selected final or provisional row per show.
- `Needs Review`: incomplete identities and conflicting records that were not safe to resolve automatically.
- `Source Rows`: every imported row and the decision made for it, including superseded rows.

Invalid uploads return a plain HTML error and are not saved. Re-uploading changed content under the same filename records a new revision while the report uses the newest one.

## Stack

- Python 3.12+
- FastAPI and Uvicorn
- Jinja templates with shared CSS and vanilla JavaScript
- pandas and openpyxl for spreadsheet processing
- pdfplumber for Costco payment reports
- Python's built-in SQLite support for payroll history
- Docker Compose for the application runtime and persistent data mount
- `uv` for dependency and command management

The frontend is intentionally server-rendered. Raccoon progressively enhances its form with `fetch` so processing results replace the page content without a full reload; the native HTML form remains the fallback.

## Run with Docker Compose

Build and start the application and its configured Cloudflare tunnel:

```bash
docker compose up --build
```

Open <http://127.0.0.1:8000> locally or <https://cherry.billgatesironworks.com> through the tunnel. Payroll history is stored in the gitignored `.local-data/` directory mounted at `/data` in the container, so `docker compose down`, container replacement, and server restarts preserve it.

The tunnel uses the existing named-tunnel credential at `~/.cloudflared/8287ee62-8ec3-4770-867d-b69a050e88aa.json`; the credential remains outside the repository and image. The Compose defaults match this deployment's macOS user. On a host with different numeric IDs, run `HOST_UID=$(id -u) HOST_GID=$(id -g) docker compose up --build`.

The SQLite file contains the original payroll workbooks. Restrict access to `.local-data/` and include it in backups; stop the Compose service before making a file-level backup.

Stop the application with:

```bash
docker compose down
```

Set `CHERRY_PORT` to change the host port, for example `CHERRY_PORT=8080 docker compose up --build`.

## Run natively

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

## Upload limits

The current server-side limits are designed for the existing bookkeeping exports:

- Costco: 30 payment PDFs maximum, 5 MB per PDF; the optional mapping file is also limited to 5 MB.
- Costco payroll: 30 new Excel workbooks per submission, 5 MB per workbook; saved history is included automatically.
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
Dockerfile                  # Production application image
compose.yaml                # Web service, persistent /data mount, and tunnel
app/
├── main.py                 # FastAPI routes and response contracts
├── routes/                 # Workflow request handling
├── library/                # PDF, spreadsheet, and reconciliation logic
├── templates/              # Jinja pages
├── static/                 # Shared and page-specific CSS/JavaScript
└── data/                   # Bundled Costco store catalog
.local-data/                # Gitignored SQLite data and local outputs
tests/                       # Unit and endpoint tests
```
