import base64
from datetime import date, datetime
from io import BytesIO
import sqlite3

from fastapi import UploadFile
from fastapi.responses import HTMLResponse
from jinja2 import Template

from app.library.costco_payroll import (
    MAX_FILES,
    MAX_FILE_BYTES,
    PayrollFile,
    PayrollInputError,
    PayrollReport,
    build_costco_payroll_master,
)
from app.library.payroll_store import PayrollStore, PayrollStoreStats, StoredWorkbook


def show_costco_payroll(template: Template) -> HTMLResponse:
    store = PayrollStore.from_environment()
    try:
        workbooks = store.current()
        if not workbooks:
            return HTMLResponse(template.render())
        generated = _build(workbooks)
        return _render_report(template, generated, store.stats())
    except (PayrollInputError, sqlite3.Error) as error:
        return HTMLResponse(
            template.render(error=f"Could not load saved payroll: {error}"),
            status_code=500,
        )


def process_costco_payroll(
    template: Template,
    files: list[UploadFile] | None = None,
) -> HTMLResponse:
    store = PayrollStore.from_environment()
    try:
        uploads = _read_uploads(files or [])
        combined = {workbook.name: workbook for workbook in store.current()}
        combined.update({workbook.name: workbook for workbook in uploads})
        generated = _build(tuple(combined.values()))
        store.save(uploads)
    except PayrollInputError as error:
        return HTMLResponse(template.render(error=str(error)), status_code=400)
    except sqlite3.Error as error:
        return HTMLResponse(
            template.render(error=f"Could not save payroll: {error}"),
            status_code=500,
        )
    return _render_report(template, generated, store.stats())


def _read_uploads(files: list[UploadFile]) -> tuple[StoredWorkbook, ...]:
    selected = [file for file in files if file.filename]
    if len(selected) > MAX_FILES:
        raise PayrollInputError(f"Choose no more than {MAX_FILES} payroll files at once.")

    uploads = []
    for file in selected:
        name = file.filename or ""
        if not name.lower().endswith(".xlsx"):
            raise PayrollInputError(f"{name} must be an XLSX workbook.")
        content = file.file.read(MAX_FILE_BYTES + 1)
        if len(content) > MAX_FILE_BYTES:
            raise PayrollInputError(f"{name} must be 5 MB or smaller.")
        uploads.append(StoredWorkbook(name, content))
    return tuple(uploads)


def _build(workbooks: tuple[StoredWorkbook, ...]) -> PayrollReport:
    # ponytail: rebuild from source each time; cache normalized rows if yearly
    # history grows enough for this to become measurably slow.
    return build_costco_payroll_master(
        [PayrollFile(workbook.name, BytesIO(workbook.body)) for workbook in workbooks]
    )


def _render_report(
    template: Template,
    generated: PayrollReport,
    stats: PayrollStoreStats,
) -> HTMLResponse:
    report = {
        "rows": [_report_row(record) for record in generated.master],
        "review_rows": [_report_row(record) for record in generated.review],
        "total": len(generated.master),
        "final": sum(record["Status"] == "Final" for record in generated.master),
        "provisional": sum(
            record["Status"] == "Provisional only" for record in generated.master
        ),
        "review": len(generated.review),
        "source": generated.source_count,
        "stored": stats.current_workbooks,
        "revisions": stats.revisions,
    }
    # ponytail: inline data keeps the service stateless; use expiring storage if
    # generated workbooks become too large for an HTML response.
    download = {
        "filename": generated.download_name,
        "content": base64.b64encode(generated.body).decode("ascii"),
        "media_type": generated.media_type,
    }
    return HTMLResponse(
        template.render(report=report, download=download),
    )


def _report_row(record: dict) -> dict[str, str]:
    location = (_display(record.get("Location"), ""), _display(record.get("State"), ""))
    show_dates = (
        _display_date(record.get("Start Date"), ""),
        _display_date(record.get("End Date"), ""),
    )
    return {
        "payroll_date": _display_date(record.get("Payroll Date")),
        "status": _display(record.get("Status")),
        "salesperson": _display(record.get("Sales Person")),
        "show": _display(record.get("Show #")),
        "warehouse": _display(record.get("Whs. #")),
        "location": ", ".join(filter(None, location)),
        "show_dates": " – ".join(filter(None, show_dates)),
        "advance": _display_money(record.get("Advance Payment Amount")),
        "net_sales": _display_money(record.get("Net Sales")),
        "commission": _display_money(record.get("Commission Earned $")),
        "amount_paid": _display_money(record.get("Amount Paid")),
        "arrears": _display_money(record.get("Arrears")),
        "source": f'{record["Source File"]} · row {record["Source Row"]}',
        "reason": _display(record.get("Review Reason"), ""),
    }


def _display(value: object, empty: str = "—") -> str:
    return empty if value is None or value == "" else str(value)


def _display_date(value: object, empty: str = "—") -> str:
    if isinstance(value, (date, datetime)):
        return value.strftime("%m/%d/%y")
    return _display(value, empty)


def _display_money(value: object) -> str:
    if value is None or value == "":
        return "—"
    return f"${value:,.2f}" if isinstance(value, (int, float)) else str(value)
