from collections.abc import Iterable
import base64
from datetime import timedelta
from io import BytesIO
import re
import pandas as pd
from fastapi import UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse
from openpyxl.styles import PatternFill

from app.library.reconciliation import (
    MonthFrames,
    reconcile_months,
    reconcile_receivables,
)
from app.library.utils import to_camel_case


MAX_FILES = 24
MAX_FILE_SIZE = 5 * 1024 * 1024
EXCEL_SUFFIXES = (".xls", ".xlsx")
AR_SHEETS = {
    "CHECK": {"date": 0, "customer": 1, "checkRef": 2, "depositRef": 4, "invoiceRef": 6, "amount": 7, "depositTotal": 8},
    "JGI CC": {"date": 0, "customer": 1, "checkRef": 2, "depositRef": 4, "invoiceRef": 6, "amount": 7, "depositTotal": 8, "batchRef": 10, "authRef": 11},
    "COSNEXT": {"date": 0, "customer": 1, "checkRef": 2, "depositRef": 4, "invoiceRef": 6, "amount": 7, "depositTotal": 8},
    "INTERNET": {"date": 0, "customer": 1, "checkRef": 2, "depositRef": 4, "invoiceRef": 5, "amount": 6, "depositTotal": 7},
}


def _header_row(excel: pd.ExcelFile, sheet: str, required: set[str]) -> int:
    preview = excel.parse(sheet, header=None, nrows=12)
    for index, row in preview.iterrows():
        labels = {
            re.sub(r"\s+", "", str(value)).lower()
            for value in row
            if pd.notna(value)
        }
        if required <= labels:
            return int(index)
    raise ValueError(f"{sheet} sheet does not have the expected columns")


def _load_month(filename: str, content: bytes) -> tuple[pd.Period, MonthFrames]:
    excel = pd.ExcelFile(BytesIO(content))
    if "VNB" not in excel.sheet_names:
        raise ValueError(f"{filename}: missing VNB sheet")

    ap_sheet = next((name for name in ("AP", "AP1") if name in excel.sheet_names), None)
    if ap_sheet is None:
        raise ValueError(f"{filename}: missing AP sheet")

    vnb_header = _header_row(excel, "VNB", {"postdate", "description", "debit", "credit"})
    ap_header = _header_row(excel, ap_sheet, {"checknumber", "checkdate", "checkamount"})
    vnb = excel.parse("VNB", header=vnb_header)
    ap = excel.parse(
        ap_sheet,
        header=ap_header,
    )
    vnb.columns = [to_camel_case(str(column)) for column in vnb.columns]
    ap.columns = [to_camel_case(str(column)) for column in ap.columns]
    vnb["sourceRow"] = vnb.index + vnb_header + 2
    ap["sourceRow"] = ap.index + ap_header + 2

    vnb_columns = ["sourceRow", "postDate", "check", "description", "debit", "credit"]
    ap_columns = ["sourceRow", "checkNumber", "checkDate", "vendorNumber", "name", "checkAmount"]
    missing = [column for column in vnb_columns if column not in vnb]
    missing += [column for column in ap_columns if column not in ap]
    if missing:
        raise ValueError(f"{filename}: missing columns: {', '.join(missing)}")

    vnb = vnb[vnb_columns].copy()
    ap = ap[ap_columns].copy()
    post_dates = pd.to_datetime(vnb["postDate"], errors="coerce")
    periods = post_dates.dropna().dt.to_period("M")
    if periods.empty:
        raise ValueError(f"{filename}: VNB sheet has no valid posting dates")
    period = periods.mode().iloc[0]

    vnb["postDate"] = post_dates.dt.date
    vnb["check"] = vnb["check"].map(
        lambda value: ""
        if pd.isna(value)
        else str(int(value))
        if isinstance(value, float) and value.is_integer()
        else str(value)
    )
    vnb["description"] = vnb["description"].fillna("").astype(str)
    vnb["debit"] = pd.to_numeric(vnb["debit"], errors="coerce")
    vnb["credit"] = pd.to_numeric(vnb["credit"], errors="coerce")

    amount_text = ap["checkAmount"].astype(str).str.replace(",", "", regex=False)
    negative = amount_text.str.endswith("-")
    ap["checkAmount"] = pd.to_numeric(amount_text.str.rstrip("-"), errors="coerce")
    ap.loc[negative, "checkAmount"] *= -1
    ap = ap.dropna(subset=["checkAmount"])
    ap = ap[ap["checkAmount"] != 0].reset_index(drop=True)
    ap["checkDate"] = pd.to_datetime(ap["checkDate"], errors="coerce").dt.date
    ap["checkNumber"] = ap["checkNumber"].fillna("").astype(str).str.lstrip("0")
    ap["vendorNumber"] = ap["vendorNumber"].fillna("").astype(str)
    ap["name"] = ap["name"].fillna("").astype(str)

    return period, MonthFrames(str(period), ap=ap, vnb=vnb)


def _write_workbook(result: MonthFrames) -> bytes:
    output = BytesIO()
    exact_fill = PatternFill("solid", fgColor="90EE90")
    partial_fill = PatternFill("solid", fgColor="228B22")
    ar_match_fill = PatternFill("solid", fgColor="93C5FD")
    warning_fill = PatternFill("solid", fgColor="FCD34D")
    review_fill = PatternFill("solid", fgColor="F9A8D4")
    headers = {
        "sourceRow": "Source Row",
        "postDate": "Bank Date",
        "check": "Bank Check Number",
        "description": "Bank Description",
        "debit": "Bank Debit",
        "credit": "Bank Credit",
        "checkNumber": "AP Check Number",
        "checkDate": "AP Check Date",
        "vendorNumber": "Vendor Number",
        "name": "Vendor Name",
        "checkAmount": "AP Amount",
        "arStatus": "AR Match Status",
        "arSource": "AR Source",
        "arReference": "AR Reference",
        "arRule": "AR Match Rule",
        "arNotes": "AR Match Details",
        "arDate": "AR Date",
        "arDateLag": "Settlement Lag (Days)",
        "arWarning": "Date Warning",
    }
    ar_columns = [
        "sourceRow",
        "postDate",
        "description",
        "credit",
        "arStatus",
        "arSource",
        "arReference",
        "arRule",
        "arDate",
        "arDateLag",
        "arNotes",
        "arWarning",
    ]
    ar_matches = result.vnb.loc[result.vnb["arStatus"] == "matched", ar_columns].copy()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet, frame in (
            ("Processed VNB", result.vnb),
            ("Processed AP", result.ap),
            ("AR Matches", ar_matches),
        ):
            sheet_headers = headers.copy()
            if sheet == "Processed VNB":
                sheet_headers.update({"status": "AP Match Status", "notes": "AP Match Details"})
            elif sheet == "Processed AP":
                sheet_headers.update(
                    {"status": "Match Status", "matchRule": "Match Rule", "notes": "Match Details"}
                )
            elif sheet == "AR Matches":
                sheet_headers["arNotes"] = "AR Source Location"
            exported = frame.rename(columns=sheet_headers)
            exported.to_excel(writer, sheet_name=sheet, index=False)
            worksheet = writer.sheets[sheet]
            worksheet.freeze_panes = "A2"
            worksheet.auto_filter.ref = worksheet.dimensions
            if "status" in frame:
                status_column = exported.columns.get_loc(sheet_headers["status"]) + 1
                for row_number, status in enumerate(frame["status"], start=2):
                    if status == "matched":
                        worksheet.cell(row_number, status_column).fill = exact_fill
                    elif status in {"partial match", "split match"}:
                        worksheet.cell(row_number, status_column).fill = partial_fill
            if "arStatus" in frame:
                ar_status_column = exported.columns.get_loc(headers["arStatus"]) + 1
                warnings = frame.get("arWarning", pd.Series("", index=frame.index))
                for row_number, (status, warning) in enumerate(
                    zip(frame["arStatus"], warnings), start=2
                ):
                    if warning:
                        worksheet.cell(row_number, ar_status_column).fill = warning_fill
                    elif status == "matched":
                        worksheet.cell(row_number, ar_status_column).fill = ar_match_fill
                    elif status == "needs review":
                        worksheet.cell(row_number, ar_status_column).fill = review_fill
    return output.getvalue()


def _load_ar_workbook(filename: str, content: bytes) -> tuple[pd.DataFrame, list[dict]]:
    excel = pd.ExcelFile(BytesIO(content))
    records = []
    issues = []
    for sheet, positions in AR_SHEETS.items():
        if sheet not in excel.sheet_names:
            continue
        raw = excel.parse(sheet, header=None)
        for index, row in raw.iterrows():
            record = {name: row.iloc[position] if position < len(row) else None for name, position in positions.items()}
            ar_date = pd.to_datetime(record.get("date"), errors="coerce")
            amount = _ar_money(record.get("amount"))
            deposit_total = _ar_money(record.get("depositTotal"))
            if amount is None and deposit_total is None:
                continue
            if pd.isna(ar_date):
                has_reference = any(
                    _ar_text(record.get(field))
                    for field in ("checkRef", "depositRef", "invoiceRef", "batchRef", "authRef")
                )
                if amount is not None and amount > 0 and has_reference:
                    issues.append(
                        {
                            "source": filename,
                            "sheet": sheet,
                            "row": index + 1,
                            "date": "",
                            "message": "Missing or invalid AR date on a referenced payment.",
                        }
                    )
                continue
            record.update(
                {
                    "recordId": f"{filename}:{sheet}:{index + 1}",
                    "sourceFile": filename,
                    "sheet": sheet,
                    "row": index + 1,
                    "date": ar_date.date(),
                    "customer": _ar_text(record.get("customer")),
                    "amount": amount,
                    "depositTotal": deposit_total,
                }
            )
            for field in ("checkRef", "depositRef", "invoiceRef", "batchRef", "authRef"):
                record[field] = _ar_text(record.get(field))
            records.append(record)
    if not records:
        raise ValueError(f"{filename}: none of CHECK, JGI CC, COSNEXT, or INTERNET has usable rows")
    return pd.DataFrame(records), issues


def _ar_text(value) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def _ar_money(value) -> float | None:
    if pd.isna(value):
        return None
    text = str(value).strip().replace("$", "").replace(",", "")
    negative = text.endswith("-")
    try:
        amount = float(text.rstrip("-"))
    except ValueError:
        return None
    return round(-amount if negative else amount, 2)


def _load_receivables(files: Iterable[tuple[str, bytes]]) -> tuple[pd.DataFrame, list[dict]]:
    loaded = [_load_ar_workbook(filename, content) for filename, content in files]
    if not loaded:
        return pd.DataFrame(), []
    frames = [frame for frame, _ in loaded]
    issues = [issue for _, workbook_issues in loaded for issue in workbook_issues]
    records = pd.concat(frames, ignore_index=True)
    seen = {}
    keep = []
    for index, row in records.iterrows():
        identifiers = {
            "CHECK": ("checkRef", "depositRef", "invoiceRef"),
            "JGI CC": ("batchRef", "authRef"),
            "COSNEXT": ("depositRef",),
            "INTERNET": ("depositRef",),
        }[row["sheet"]]
        natural = ("sheet", "date", "customer", "amount", *identifiers)
        key = tuple(None if pd.isna(row[column]) else row[column] for column in natural)
        first_file = seen.setdefault(key, row["sourceFile"])
        if first_file == row["sourceFile"]:
            keep.append(index)
    return records.loc[keep].reset_index(drop=True), issues


def _outlying_ar_dates(records: pd.DataFrame, periods: list[pd.Period]) -> list[dict]:
    if records.empty or not periods:
        return []
    earliest = periods[0].start_time.date() - timedelta(days=31)
    latest = periods[-1].end_time.date() + timedelta(days=31)
    issues = []
    for _, row in records.iterrows():
        ar_date = row["date"]
        if ar_date < earliest or ar_date > latest:
            issues.append(
                {
                    "source": row["sourceFile"],
                    "sheet": row["sheet"],
                    "row": int(row["row"]),
                    "date": ar_date.isoformat(),
                    "message": f"AR date is outside the uploaded reconciliation range ({periods[0]} to {periods[-1]}).",
                }
            )
    return issues


def build_reconciliation(
    files: Iterable[tuple[str, bytes]], ar_files: Iterable[tuple[str, bytes]] = ()
) -> tuple[list[tuple[str, bytes]], list[MonthFrames], list[dict]]:
    months = [_load_month(filename, content) for filename, content in files]
    months.sort(key=lambda item: item[0])
    identifiers = [frames.month for _, frames in months]
    if len(identifiers) != len(set(identifiers)):
        raise ValueError("Upload only one workbook for each month")

    results = reconcile_months([frames for _, frames in months])
    receivables, issues = _load_receivables(ar_files)
    issues.extend(_outlying_ar_dates(receivables, [period for period, _ in months]))
    results = reconcile_receivables(results, receivables)
    workbooks = [
        (f"raccoon-{result.month}.xlsx", _write_workbook(result)) for result in results
    ]
    return workbooks, results, issues


def build_reconciliation_files(
    files: Iterable[tuple[str, bytes]], ar_files: Iterable[tuple[str, bytes]] = ()
) -> list[tuple[str, bytes]]:
    return build_reconciliation(files, ar_files)[0]


def _review_summary(results: list[MonthFrames], issues: Iterable[dict] = ()) -> dict:
    months = []
    periods = [pd.Period(result.month, freq="M") for result in results]
    reference_first = (
        len(periods) > 1
        and periods[0].month == 12
        and periods[1].month == 1
        and periods[0] + 1 == periods[1]
    )
    for position, result in enumerate(results):
        rows = []
        credits = result.vnb[pd.to_numeric(result.vnb["credit"], errors="coerce").fillna(0) > 0]
        for index, row in credits.iterrows():
            status = row.get("arStatus", "needs review")
            warning = str(row.get("arWarning", "") or "")
            display_status = "warning" if warning else status
            rows.append(
                {
                    "row": int(index) + 2,
                    "date": row["postDate"].isoformat() if hasattr(row["postDate"], "isoformat") else str(row["postDate"]),
                    "amount": f"${float(row['credit']):,.2f}",
                    "description": str(row["description"]),
                    "status": status,
                    "displayStatus": display_status,
                    "source": str(row.get("arSource", "")),
                    "reference": str(row.get("arReference", "")),
                    "rule": str(row.get("arRule", "")),
                    "notes": str(row.get("arNotes", "")),
                    "arDate": str(row.get("arDate", "") or ""),
                    "arDateLag": row.get("arDateLag", ""),
                    "warning": warning,
                }
            )
        reference = reference_first and position == 0
        considered = [] if reference else [row for row in rows if row["status"] != "internal transfer"]
        months.append(
            {
                "month": result.month,
                "reference": reference,
                "matched": sum(row["status"] == "matched" for row in considered),
                "review": sum(row["status"] == "needs review" for row in considered),
                "warnings": sum(bool(row["warning"]) for row in considered),
                "total": len(considered),
                "rows": rows,
            }
        )
    return {
        "months": months,
        "matched": sum(month["matched"] for month in months),
        "review": sum(month["review"] for month in months),
        "warnings": sum(month["warnings"] for month in months),
        "total": sum(month["total"] for month in months),
        "issues": list(issues),
    }


async def process_raccoon_analysis(
    files: list[UploadFile],
    ar_files: list[UploadFile],
    template,
) -> HTMLResponse:
    if not files:
        return HTMLResponse(template.render(error="Choose at least one VNB/AP workbook."), status_code=400)
    if len(files) > MAX_FILES:
        return HTMLResponse(template.render(error=f"Choose no more than {MAX_FILES} VNB/AP workbooks."), status_code=400)
    if len(ar_files) > MAX_FILES:
        return HTMLResponse(template.render(error=f"Choose no more than {MAX_FILES} AR workbooks."), status_code=400)

    uploads = []
    ar_uploads = []
    for destination, selected in ((uploads, files), (ar_uploads, ar_files)):
        for file in selected:
            filename = file.filename or "workbook"
            if not filename.lower().endswith(EXCEL_SUFFIXES):
                return HTMLResponse(template.render(error=f"{filename} is not an Excel workbook."), status_code=400)
            content = await file.read(MAX_FILE_SIZE + 1)
            if len(content) > MAX_FILE_SIZE:
                return HTMLResponse(
                    template.render(error=f"{filename} is larger than 100 MB."),
                    status_code=400,
                )
            destination.append((filename, content))

    try:
        workbooks, results, issues = await run_in_threadpool(build_reconciliation, uploads, ar_uploads)
    except Exception as error:
        return HTMLResponse(
            template.render(error=f"Could not reconcile these files: {error}"),
            status_code=400,
        )

    return HTMLResponse(
        template.render(
            results=_review_summary(results, issues),
            downloads=[
                {
                    "filename": filename,
                    "month": filename.removeprefix("raccoon-").removesuffix(".xlsx"),
                    "content": base64.b64encode(content).decode("ascii"),
                }
                for filename, content in workbooks
            ],
        )
    )
