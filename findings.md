# Findings & Decisions

## Requirements

- Read the linked Gmail message using computer controls.
- Plan a new service that reaches the goal described in that message.
- Do not send, delete, archive, or otherwise modify email.
- Treat `raw-data/2026` as read-only private test data and keep it out of Git.

## Research Findings

- The linked tab is open in Brave and titled `NEW PROJECT- Costco Payroll`.
- The repository already has uncommitted user changes in `PRODUCT.md` and `README.md`; preserve them.
- Email sender: Adele Li, subject `NEW PROJECT- Costco Payroll`, received October 5, 2026.
- Goal: combine all Google Sheets in the supplied Drive folder into a single master view covering Costco payroll and advance payments, organized by payroll date.
- Source sheets overlap. An advance-payment sheet can contain a provisional payment for a salesperson; a later payroll sheet can repeat that payment and add the final commission after the sales target is known.
- Required precedence rule: for an overlapping payroll cycle, use the later/final entry that includes commissions (the email's example says to use the `10.04.26` sheet rather than the provisional `9.18.26` entry).
- The sample columns include show/location identifiers, salesperson, event dates, advance paid, net sales, rate, commission earned, salary, reimbursement, commission balance/due, other payments, amount paid, and arrears.
- Example evidence: the earlier sheet shows advance-oriented rows; the later sheet contains net sales, calculated commission, balance due, final amount paid, and arrears for many of the same shows/salespeople.
- The Drive folder contains 20 dated Google Sheets from `01.12.26` through `10.04.26`, plus two files named as copies. Filename date is therefore usable as an import/payroll-cycle signal, but copy files need explicit exclusion or duplicate detection.
- The repository is an existing Python web app with Costco-specific routes, analysis, upload policy, reconciliation, templates, JavaScript, and tests. The service should extend that flow rather than become a separate stack.
- Sample workbook structure is not a single clean table: `09.18.26` has tabs `Sheet1`, `paid`, and `Sheet3`; `10.04.26` has `Sheet1`, `Sheet4`, `paid`, and `Sheet3`. The importer must identify the payroll table by its headers rather than assume a fixed tab name or active sheet.
- The later workbook is link-shared inside the company domain; direct server-side Drive access would add credential and permission management that the current app intentionally avoids.
- The user confirmed the service input is multiple XLSX files, not the downloaded ZIP itself.
- `raw-data/2026` contains 22 XLSX files: 20 dated payroll files plus `Copy of 01.26.26.xlsx` and `Copy of 02.23.26.xlsx`. Every file is under 225 KB.
- The workbooks are heavily preformatted: worksheets report roughly 760–795 rows and 32–33 columns even though only about 218–396 cells contain values. Parsing must stop on actual row content rather than worksheet dimensions.
- Most worksheets contain two separate payroll-style tables, with header rows near 3 and 18/19. A parser must discover multiple regions within one sheet.
- Repeated `paid` and `Sheet3` tabs contain valid-looking headers and formulas but appear copied across many dated workbooks. Header matching alone would import stale rows repeatedly; source-region selection needs duplicate/content and payroll-date evidence.
- Observed header drift includes `Salesperson`/`Sales Person`, `Whs. #` spacing, `Date`/`Payment Date`/`Date Paid`, `Commission Added`/`Commission Balance Due`, `Advance Payment`/`Amount Due`, `Reimbursement`/`Reimburesments`, and `Amount Paid`/`Amoun Paid`.
- Each current workbook's active worksheet is `Sheet1`. Stale `paid` tabs retain `01.26.26`; repeated `Sheet3` tabs retain `06.25.26`. Limiting imports to the active worksheet cleanly avoids those historical copies.
- The active sheet has two semantic regions: the first is labeled `Costco Payroll` and contains finalized/current payroll rows; the second is labeled `ADVANCE PAYMENTS` and contains provisional rows for shows between payroll dates.
- The `09.18.26` advance rows reappear in the first `10.04.26` region with net sales and commission values, directly confirming that a later `Costco Payroll` row must supersede the earlier `ADVANCE PAYMENTS` row.
- Cached values exist for formula cells in the exported XLSX files, so the service can read `data_only=True` and copy the calculated result without reimplementing commission formulas.
- The source filename is the reliable payroll date. Internal `B1` is occasionally off by a day or malformed (`05.03.26`, `06.15.26`, `Hotel`).
- Show number alone is not unique and is sometimes missing. The planned identity remains show number + warehouse + salesperson + start date, with warehouse + location + salesperson + start/end dates as fallback.
- `10.04.26` has `Sheet1` and `Sheet4` with identical data rows; `Sheet4` differs only by one final total formula. Row-level master output is unaffected by choosing the active `Sheet1`.
- Same-date `Copy of 01.26.26` and `Copy of 02.23.26` files contain materially later net-sales and commission values than their originals. The implementation treats `Copy of` as the later revision and retains originals as superseded source rows.
- The implemented real-data run produces 151 canonical rows, 228 source rows, and 3 review rows. The review rows are two conflicting entries for one `06.16.26` Charleston show and one `09.04.26` sick-time row without enough show identity.

## Technical Decisions

| Decision | Rationale |
|----------|-----------|
| Inspect existing project before proposing new code | Reuse is smaller and safer than duplicating functionality |
| Model imported rows as source records and select a canonical final record | Preserves auditability while preventing provisional/final duplicates in the master view |
| Extend the existing Costco feature | The repo already owns Costco ingestion and analysis; a separate deployable service would duplicate infrastructure |
| Accept selected `.xlsx` files only | The user will download and unzip the Drive folder first, so archive parsing is unnecessary |
| Detect the payroll table by required headers | Workbook tab names vary and include generic names such as `Sheet1`, `Sheet3`, `Sheet4`, and `paid` |
| Use the active worksheet only | Every current workbook activates its current `Sheet1`; repeated secondary tabs are stale copies |

## Issues Encountered

| Issue | Resolution |
|-------|------------|

## Resources

- Email: https://mail.google.com/mail/u/9/#inbox/FMfcgzQhWnqpqdlGZDZjWXsFLRKWTBbV

---

*Update this file after every 2 view/browser operations*
