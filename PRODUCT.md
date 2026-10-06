# Product

## Register

Product application.

## Purpose

Cherry Pancake is a dependable toolbox for repetitive small-business bookkeeping work. It converts familiar source documents into finished Excel workbooks without requiring command-line steps, manual file renaming, or spreadsheet scripting.

Success means the user can identify the right workflow, understand which files it needs, submit them confidently, review any exceptions, and download a clearly named result without technical help.

## Primary user

One bookkeeping user working at a desk with recurring monthly exports. The product can optimize for this known workflow rather than introducing accounts, teams, roles, or enterprise administration.

Typical needs:

- Combine Costco payment PDFs and resolve store names.
- Consolidate dated Costco payroll workbooks while retaining exceptions and source lineage.
- Validate and reorganize salesperson performance exports.
- Reconcile monthly VNB/AP activity against accounts receivable and review uncertain matches.

## Product principles

### Familiar over novel

Use recognizable file pickers, explicit accepted formats, plain-language actions, and direct downloads. The interface should feel like a reliable utility rather than a configurable platform.

### Calm and task-focused

Keep the visual system practical, friendly, and compact. Use the warm cat-café paper identity and persimmon accent without letting decoration compete with file workflows or review tables.

### Explain what the user can fix

Validation and processing errors should name the affected file or expected workbook structure whenever possible. Status must not rely on color alone.

### Preserve a safe fallback

JavaScript may improve feedback and avoid page reloads, but core upload workflows should continue to use standard server-rendered forms when enhancement is unavailable.

### Make completion obvious

Downloads should appear as soon as processing finishes, use predictable filenames, and remain easy to associate with the submitted month or workflow.

## Current workflows

### Costco payment reports

- Requires one or more Costco payment PDFs.
- Uses the bundled Costco store catalog by default.
- Accepts an optional CSV or Excel store-mapping update.
- Produces one dated Excel workbook containing the combined results.

### Costco payroll master

- Accepts dated Excel payroll workbooks after the user unzips the Drive download.
- Saves immutable workbook revisions in SQLite and rebuilds from the latest version of each filename.
- Keeps finalized payroll rows over matching advance-payment rows.
- Displays the combined payroll master and review exceptions in the browser.
- Produces a master, a review queue, and a complete source-row audit trail.

### Salesperson performance

- Accepts the expected sales export workbook.
- Validates period-to-date, year-to-date, and prior-year totals.
- Produces a clean salesperson report with a validation summary.

### Raccoon reconciliation

- Accepts monthly VNB/AP workbooks and optional accounts-receivable workbooks.
- Infers reconciliation months from workbook data rather than filename conventions.
- Presents matched, review, warning, and input-issue states in the browser.
- Produces a downloadable reconciled workbook for each month.
- Uses progressive submission so results can appear without a full page reload.

## Experience requirements

- Present all workflows as one small web application with a shared visual and interaction system.
- Put required inputs before optional inputs.
- Show accepted formats and relevant limits beside each upload control.
- Keep primary actions disabled until required files are selected.
- Retain selected context and visible progress while processing when JavaScript is available.
- Work at desktop and narrow mobile widths without hiding essential actions or status.

## Accessibility

Target WCAG 2.1 AA:

- Full keyboard operation and visible focus states.
- Readable text and control contrast.
- Status messaging that combines text with color.
- Semantic headings, labels, regions, and live-status announcements.
- Reduced-motion support.
- Touch targets that remain usable on narrow screens.

## Data and operational constraints

- The application is intended for a trusted single user.
- Uploaded files are processed in memory except Costco payroll workbooks, which are retained in SQLite for rolling reports.
- There is no authentication system or multi-user state.
- Current file-count and file-size boundaries are implementation safeguards, not a general-purpose document-storage policy.
- Processing should fail with a useful response rather than silently producing a partial workbook.

## Non-goals

- A general accounting platform or system of record.
- User accounts, permissions, collaboration, or audit administration.
- Persistent document storage or upload history.
- A client-side SPA or frontend framework migration without a demonstrated workflow need.
- Enterprise dashboards, configurable analytics, or generic report builders.
