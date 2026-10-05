import re
import pandas as pd
from fastapi import UploadFile
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from io import BytesIO
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from app.library.utils import (
    extract_key,
    get_store_names,
    extract_mm_dd,
    extract_payment_id,
    get_today_date,
)

MAX_FILES = 30
MAX_FILE_SIZE = 5 * 1024 * 1024
STORE_SUFFIXES = (".csv", ".xls", ".xlsx")
DATE_RANGE_ORDER_PATTERN = (
    r"\s*\d{1,2}/\d{1,2}/\d{2,4}\s*[-–—]\s*"
    r"\d{1,2}/\d{1,2}/\d{2,4}\s*"
)


def process_costco_analysis(
    files: list[UploadFile] | None = None,
    store_file: UploadFile | None = None,
) -> Response:
    """
    Process Costco PDF payment reports and generate an Excel analysis.

    Args:
        files: List of PDF files to process (max 30)
        store_file: Optional CSV or XLSX file containing updated store mapping data

    Returns:
        StreamingResponse with Excel file or HTMLResponse with error
    """
    if not files:
        return HTMLResponse(
            content="Please upload at least one Costco PDF file.", status_code=400
        )
    if len(files) > MAX_FILES:
        return HTMLResponse(
            content=f"Choose no more than {MAX_FILES} Costco PDF files.",
            status_code=400,
        )

    store_mapping = get_store_names()
    detailed_dataframes = []

    # 1. Load Store Mapping
    if store_file and store_file.filename:
        try:
            if not store_file.filename.lower().endswith(STORE_SUFFIXES):
                return HTMLResponse(
                    content="Store mapping file must be CSV or Excel.", status_code=400
                )
            content = store_file.file.read(MAX_FILE_SIZE + 1)
            if len(content) > MAX_FILE_SIZE:
                return HTMLResponse(
                    content="Store mapping file must be 100 MB or smaller.",
                    status_code=400,
                )
            filename = store_file.filename.lower()
            if filename.endswith(".csv"):
                df_stores = pd.read_csv(BytesIO(content), header=None)
            elif filename.endswith((".xlsx", ".xls")):
                df_stores = pd.read_excel(BytesIO(content), header=None)
            else:
                return HTMLResponse(
                    content="Error: Store mapping file must be .csv or .xlsx",
                    status_code=400,
                )
            store_mapping = get_store_names(df=df_stores)
        except Exception:
            return HTMLResponse(
                content=(
                    "Could not read the store mapping file. "
                    "Check that it is a valid CSV or Excel file."
                ),
                status_code=400,
            )
    # 2. Process PDFs
    if files:
        for file in files:
            if not file.filename or not file.filename.lower().endswith(".pdf"):
                return HTMLResponse(
                    content="Costco report files must be PDFs.", status_code=400
                )

            content = file.file.read(MAX_FILE_SIZE + 1)
            if len(content) > MAX_FILE_SIZE:
                return HTMLResponse(
                    content=f"{file.filename} must be 100 MB or smaller.",
                    status_code=400,
                )
            try:
                import pdfplumber

                file_rows = []
                date_check_num = []

                with pdfplumber.open(BytesIO(content)) as pdf:
                    # Extract date and payment number from first page
                    first_page_text = pdf.pages[0].extract_text()
                    for line in first_page_text.split("\n"):
                        if line.startswith("Date"):
                            matched, res = extract_mm_dd(line)
                            if matched:
                                date_check_num.append(res)
                        elif line.startswith("Payment"):
                            matched, res = extract_payment_id(line)
                            if matched:
                                date_check_num.append(res)
                                break

                    # Extract tables from all pages
                    for page in pdf.pages:
                        tables = page.extract_tables()

                        for table in tables:
                            if not table or len(table) < 2:
                                continue

                            # Skip header row (first row)
                            for row in table[1:]:
                                if not row or len(row) < 7:
                                    continue

                                try:
                                    invoice = row[0] if row[0] else ""
                                    order_number = row[1] if row[1] else ""
                                    description = row[2] if row[2] else ""
                                    date = row[3] if row[3] else ""
                                    # Skip gross amount (row[4]) and discount (row[5])
                                    amount_str = row[6] if row[6] else "0"

                                    # Clean and convert amount
                                    amount_str = amount_str.replace(",", "").strip()
                                    if not amount_str or not invoice:
                                        continue

                                    amount = float(amount_str)

                                    file_rows.append(
                                        {
                                            "invoiceNumber": invoice.strip(),
                                            "orderNumber": order_number.strip(),
                                            "description": description.strip(),
                                            "date": date.strip(),
                                            "amount": amount,
                                        }
                                    )
                                except (ValueError, TypeError, IndexError):
                                    return HTMLResponse(
                                        content=(
                                            f"Could not parse a payment row in {file.filename}."
                                        ),
                                        status_code=400,
                                    )

                if not file_rows:
                    return HTMLResponse(
                        content=f"No payment rows were found in {file.filename}.",
                        status_code=400,
                    )

                df = pd.DataFrame(file_rows)

                # Apply mapping logic
                df["storeKey"] = df["invoiceNumber"].apply(
                    lambda x: extract_key(x, store_mapping)
                )
                df["storeName"] = df["storeKey"].map(
                    lambda key: store_mapping.get(key, "Unknown")
                )

                # Fix missed mappings with a second try (n=-7)
                unknown_mask = df["storeName"] == "Unknown"
                if unknown_mask.any():
                    for idx in df[unknown_mask].index:
                        inv = str(df.loc[idx, "invoiceNumber"])
                        n = -7 if len(inv) >= 11 else -6
                        skey = extract_key(inv, store_mapping, n=n)
                        sval = store_mapping.get(skey, "Unknown")
                        df.at[idx, "storeKey"] = skey
                        df.at[idx, "storeName"] = sval

                # Negative adjustments spanning a date range belong to COSNEXT.
                cosnext_mask = df["amount"].lt(0) & df["orderNumber"].str.fullmatch(
                    DATE_RANGE_ORDER_PATTERN, na=False
                )
                df.loc[cosnext_mask, "storeName"] = "COSNEXT"

                df2 = df[["storeName", "amount"]].copy()
                df2 = df2.groupby("storeName", as_index=False).sum()

                try:
                    filename = f"{date_check_num[0]} #{date_check_num[1]}"
                except Exception as _:
                    filename = file.filename
                detailed_dataframes.append((filename, df, df2))

            except Exception:
                return HTMLResponse(
                    content=f"Could not read {file.filename}. Check that it is a valid Costco PDF.",
                    status_code=400,
                )

    if not detailed_dataframes:
        return HTMLResponse(
            content="No payment rows were found in the uploaded Costco PDFs.",
            status_code=400,
        )

    # 4. Generate Excel
    wb = Workbook()
    for filename, df, df2 in detailed_dataframes:
        safe_name = re.sub(r"[\\*?:/\[\]]", "", filename)[:31]
        ws = wb.create_sheet(title=safe_name)
        for row in dataframe_to_rows(df, index=False, header=True):
            ws.append(row)

        ws.append([])
        for row in dataframe_to_rows(df2, index=False, header=True):
            ws.append(row)

        parts = filename.split(maxsplit=1)
        rdate = parts[0]
        rcheck = parts[1].removeprefix("#") if len(parts) > 1 else "Unknown"
        ws.append([])
        total = df2["amount"].sum()
        ws.append(["Total", total])
        ws.append(["Date", rdate])
        ws.append(["Check Number", rcheck])

    # Remove the default blank sheet created by Workbook()
    if "Sheet" in wb.sheetnames:
        wb.remove(wb["Sheet"])

    # Save to buffer
    output = BytesIO()
    wb.save(output)
    output.seek(0)

    today = get_today_date()
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=Costco_{today}.xlsx"},
    )
