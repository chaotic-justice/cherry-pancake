# Task Plan: Costco Payroll Service

## Goal

Build and verify the smallest service that consolidates the emailed Costco payroll workbooks into an audited master workbook.

## Current Phase

Phase 8

## Phases

### Phase 1: Requirements & Discovery

- [x] Read the linked email without modifying mailbox state
- [x] Inspect the existing repository for reusable code and constraints
- [x] Capture requirements and evidence in `findings.md`
- **Status:** complete

### Phase 2: Service Design

- [x] Define the smallest viable service and user flow
- [x] Specify data, integrations, security, and failure handling
- [x] Record assumptions and decisions
- **Status:** complete

### Phase 3: Delivery Plan

- [x] Break work into testable milestones and acceptance criteria
- [x] Identify risks, open questions, and launch checks
- [x] Write the final service plan
- **Status:** complete

### Phase 4: Verification & Handoff

- [x] Check the plan against every requirement from the email
- [x] Review generated files and summarize the handoff
- **Status:** complete

### Phase 5: Input Contract Revision

- [x] Remove ZIP input and archive handling from the service plan
- [x] Specify direct multi-file XLSX input
- [x] Reserve a gitignored local-data path for real payroll fixtures
- **Status:** complete

### Phase 6: Real Workbook Discovery

- [x] Inventory `raw-data/2026` without modifying source workbooks
- [x] Identify actual sheets, headers, formulas, duplicates, and row identity
- [x] Update the service rules from real evidence
- **Status:** complete

### Phase 7: Implementation

- [x] Add the XLSX-only payroll master library and route
- [x] Add the smallest reusable upload UI
- [x] Add focused parser, precedence, and endpoint tests
- **Status:** complete

### Phase 8: Verification & Handoff

- [x] Run the full automated test suite and JavaScript syntax checks
- [x] Generate and inspect a master workbook from `raw-data/2026`
- [x] Document results and remaining business-rule assumptions
- **Status:** complete

## Key Questions

1. What outcome and workflow does the sender need?
2. What existing project components can be reused?
3. What is the minimum service boundary that safely supports the workflow?

## Decisions Made

| Decision | Rationale |
|----------|-----------|
| Keep mailbox interaction read-only | The user asked to read and plan, not alter email state |
| Add `/costco-payroll` to the existing app | It reuses the app's in-memory spreadsheet workflow while keeping the existing PDF tool unchanged |
| Accept multiple XLSX files only | The user will unzip the Drive download before upload; ZIP handling is unnecessary |
| Preserve every normalized source row in the output | Superseded advances and duplicate decisions remain auditable |
| Do not recalculate commission | The email does not define the target thresholds; copying the finalized source values avoids inventing payroll rules |
| Import only each workbook's active sheet | Real files keep stale payroll-shaped copies in `paid` and `Sheet3` tabs |
| Treat `Copy of` as a later same-date revision | The real copy-named workbooks contain later sales and commission values; source rows retain replaced originals |

## Errors Encountered

| Error | Attempt | Resolution |
|-------|---------|------------|
| Brave new-tab creation was unavailable through the computer-control bridge | 1 | Reuse the already controlled Gmail tab for read-only sample inspection, then navigate back |
| Initial multi-file plan patch used out-of-order hunks | 1 | Applied smaller file-ordered patches |
| Phase update patch matched outdated wording | 1 | Re-read current planning files and applied smaller exact patches |
| `python` command was unavailable for the web-test helper | 1 | Use the installed `python3` command instead |
| Global `python3` lacked `openpyxl` in the browser test | 1 | Keep Playwright test browser-only and inspect the downloaded workbook separately with `uv run python` |
| Global `python3` also lacked Playwright | 2 | Use the available collaborative browser against a separately started local server |
| T3 preview snapshot failed after the page opened | 3 | Used the requested computer-use browser in a separate local tab; endpoint tests cover upload/download |

## Notes

- Preserve the user's existing changes to `PRODUCT.md` and `README.md`.
- Prefer deletion/reuse/native platform features over new abstractions or dependencies.
