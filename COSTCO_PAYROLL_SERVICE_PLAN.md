# Costco Payroll Master Service Plan

## Outcome

Add a `Costco payroll master` workflow to Cherry Pancake. The user downloads and unzips the shared Google Drive folder, uploads the resulting XLSX files, and receives one dated workbook in which provisional advances are superseded by later finalized payroll rows that include sales and commission information.

This meets the email's goal without Google OAuth, a scheduler, or another deployed service. SQLite retains source workbook revisions for rolling monthly updates.

## User flow

1. In Google Drive, download the `2026` folder and unzip it locally.
2. Open `/costco-payroll` in Cherry Pancake.
3. Choose the XLSX files and select `Build payroll master`.
4. If no usable payroll tables exist, receive a plain-language error naming the affected files.
5. Review the combined payroll master and unresolved rows in the browser.
6. Download `Costco_Payroll_Master_<latest-payroll-date>.xlsx` for the full audit workbook.

Valid source workbooks are retained in SQLite; generated reports and downloads are rebuilt on demand.

## Input contract

- Accept up to 30 `.xlsx` files in one multi-file selection.
- Reuse the current in-memory upload pattern.
- Enforce server-side limits before parsing: 5 MB per workbook and 30 workbooks.
- Reject non-XLSX, malformed, encrypted, or unreadable workbooks with a file-specific error.
- Parse the payroll date from a filename such as `10.04.26.xlsx`; reject a file whose name has no valid date.
- Treat a `Copy of` file as the later revision for the same payroll date while retaining replaced rows in the audit sheet.

## Table discovery and normalization

Inspect only each workbook's active worksheet. The real exports keep stale payroll-shaped copies in `paid` and `Sheet3`; the active worksheet contains the current data. Search its first 30 rows for each header row containing the identifying fields, which captures both the `Costco Payroll` and `ADVANCE PAYMENTS` regions.

Normalize these observed columns while preserving their original values:

- Show number
- Warehouse number
- Location and state
- Start and end date
- Salesperson
- Number of days
- Advance payment amount
- Payment date
- Net sales
- Rate
- Commission earned
- Salary
- Reimbursement
- Commission added or commission balance due
- Costco next
- Other payment columns
- Amount paid
- Arrears

Header matching should ignore punctuation, whitespace, and case, and should accept the observed `Amoun Paid` typo. Repeated `*Other` headers become `Other 1`, `Other 2`, and so on. Currency, percentage, and date values are normalized for comparison but written as typed numeric/date cells in the output.

## Record identity and precedence

Use this normalized composite key:

`warehouse number + salesperson + start date`

If one component is absent, fall back to `show number + location + salesperson + start date + end date`. Rows without either complete key are placed in `Needs Review` and are not silently merged.

Classify rows from their source region: `Costco Payroll` is final and `ADVANCE PAYMENTS` is provisional. Copy the workbook's cached formula results; do not recreate commission calculations.

For rows with the same identity:

1. Collapse byte-for-byte-equivalent normalized rows as exact duplicates.
2. Prefer finalized rows over provisional rows.
3. Among finalized rows, keep the row from the newest filename payroll date.
4. If no finalized row exists, keep the newest provisional row.
5. Treat a `Copy of` file as the later same-date revision.
6. If two different rows still tie at the winning date/status/revision, put the group in `Needs Review` instead of guessing.

This makes the email's `9.18.26` advance versus `10.04.26` final example resolve to the `10.04.26` row while retaining the earlier row as superseded lineage.

The service copies source commission values; it does not calculate commission thresholds because those rules were not specified.

## Output workbook

### Payroll Master

One canonical row per record identity, sorted by payroll date, salesperson, then show number. Put these lineage columns first:

- Payroll Date
- Decision Status (`Final`, `Provisional only`)
- Source File
- Source Sheet
- Source Row

Follow them with the normalized payroll columns.

### Needs Review

Rows that cannot be resolved safely because identity is incomplete or same-rank records conflict. Invalid filenames, unreadable workbooks, and unrecognized layouts fail the request with a file-specific error instead of producing a partial master. Include the source file, sheet, and row for review rows.

### Source Rows

Every normalized input row, including rows marked `Kept`, `Superseded`, `Exact duplicate`, or `Needs review`. This is the audit trail for consolidation decisions.

Freeze headers, enable filters, format dates/currency/percentages, and size columns using the existing openpyxl approach. Do not add charts or a dashboard.

## HTTP and UI shape

- `GET /costco-payroll`: render the upload page and rebuild the latest saved report when history exists.
- `POST /costco-payroll`: accept `files`, render the HTML payroll report with an embedded XLSX download on success, and return a user-fixable HTML error with status 400 on invalid input.
- Add one home-page tool row: `Costco payroll master`.
- Reuse the existing base layout, file-picker styles, response contract, and server-rendered form fallback.
- Client JavaScript only mirrors file-count/type/size checks and enables the submit button; server validation remains authoritative.

## Implementation map

Keep the change local:

- `app/library/costco_payroll.py`: XLSX loading, header detection, normalization, precedence, workbook writing.
- `app/routes/costco_payroll.py`: upload-to-library HTTP adapter.
- `app/templates/costco_payroll.html`: one upload form and plain instructions.
- `app/static/costco_payroll.js`: minimal selection validation and submit-state behavior.
- `app/main.py`: register the GET/POST routes and template.
- `app/templates/index.html`: add the tool row.
- `tests/test_costco_payroll.py`: business-rule tests with in-memory XLSX fixtures.
- `tests/test_endpoints.py`: route, form, validation, and download contract checks.
- `README.md` and `PRODUCT.md`: document the fourth workflow after preserving the current uncommitted edits.

Reuse pandas, openpyxl, and FastAPI; add no dependency.

## Delivery sequence

1. **Parser:** load each XLSX active sheet, find its two payroll regions, and normalize fields.
2. **Resolver:** apply identity, exact-duplicate, provisional/final, date-precedence, and ambiguity rules.
3. **Persistence:** save immutable XLSX revisions in SQLite and rebuild from the latest revision of each filename.
4. **Workbook:** generate the three output sheets with lineage and formatting.
5. **Web workflow:** add the route, template, home link, and minimal client checks.
6. **Deployment:** run the app with Docker Compose and mount `.local-data` at `/data`.
7. **Verification:** run the full Python test suite and JavaScript syntax check; manually test a folder export containing the `9.18.26` and `10.04.26` examples.

Each step should leave its smallest runnable test before moving on.

## Acceptance criteria

- The 22 downloaded XLSX files produce one master workbook.
- The later finalized `10.04.26` record wins over its matching `9.18.26` advance record.
- The superseded `9.18.26` row remains visible in `Source Rows` with its decision reason.
- The active worksheet's name does not affect discovery; stale secondary tabs are ignored.
- Same-date `Copy of` revisions win and replaced originals remain traceable in `Source Rows`.
- Conflicting ties and incomplete keys appear in `Needs Review`; the service never silently picks one.
- Invalid or oversized inputs, unreadable workbooks, and a complete lack of recognized payroll tables return useful errors.
- The application stores no uploaded payroll data and introduces no new runtime dependency.
- Existing Costco PDF, Sales, and Raccoon tests still pass.

## Shipped assumptions

1. The active worksheet is the current payroll source; repeated `paid` and `Sheet3` tabs are stale copies in the supplied 2026 files.
2. `Copy of` files are later revisions of the same dated workbook. If naming stops carrying revision order, add explicit version metadata.
3. The service returns XLSX rather than publishing a live Google Sheet, avoiding Drive credentials; SQLite stores the source history locally.
