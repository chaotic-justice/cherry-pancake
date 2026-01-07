import re
import pandas as pd
from typing import List, Optional
from fastapi import File, UploadFile
from fastapi.responses import HTMLResponse, StreamingResponse
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


async def process_costco_analysis(
    files: Optional[List[UploadFile]] = File(None),
    store_file: Optional[UploadFile] = File(None),
):
    """
    Process Costco PDF payment reports and generate an Excel analysis.

    Args:
        files: List of PDF files to process (max 30)
        store_file: CSV or XLSX file containing store mapping data

    Returns:
        StreamingResponse with Excel file or HTMLResponse with error
    """
    if files and len(files) > 30:
        return HTMLResponse(
            content="Error: You can upload a maximum of 30 PDF files at once.",
            status_code=400,
        )

    aggregated_data = []
    store_mapping = {}
    detailed_dataframes = {}

    # 1. Load Store Mapping
    if store_file and store_file.filename:
        try:
            content = await store_file.read()
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
        except Exception as e:
            return HTMLResponse(
                content=f"Error loading store mapping: {str(e)}", status_code=400
            )
    else:
        return HTMLResponse(
            content="Please upload a store mapping CSV or XLSX file first.",
            status_code=400,
        )

    # 2. Process PDFs
    if files:
        for file in files:
            if not file.filename.lower().endswith(".pdf"):
                continue

            content = await file.read()
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
                                except (ValueError, TypeError, IndexError) as e:
                                    # Skip rows that can't be parsed
                                    continue

                if not file_rows:
                    continue

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

                df2 = df[["storeName", "amount"]].copy()
                df2 = df2.groupby("storeName", as_index=False).sum()

                try:
                    filename = f"{date_check_num[0]} #{date_check_num[1]}"
                except Exception as _:
                    filename = file.filename
                detailed_dataframes[filename] = (df, df2)
                aggregated_data.append(df[["storeName", "amount"]])

            except Exception as e:
                # Log or ignore error for specific file but keep going
                print(f"Error processing {file.filename}: {e}")
                import traceback

                traceback.print_exc()

    # if not aggregated_data:
    #     return HTMLResponse(
    #         content="No valid transaction data found in uploaded PDFs.", status_code=400
    #     )

    # 3. Aggregate results
    # final_df = pd.concat(aggregated_data)
    # summary_df = final_df.groupby("storeName", as_index=False).sum()
    # summary_df = summary_df.sort_values("amount", ascending=False)
    # store_summary = summary_df.to_dict("records")

    # 4. Generate Excel
    wb = Workbook()
    # Summary Sheet
    # ws_summary = wb.active
    # ws_summary.title = "Aggregated Summary"
    # ws_summary.append(["Store Name", "Total Amount"])
    # for entry in store_summary:
    #     ws_summary.append([entry["storeName"], entry["amount"]])

    # Detail Sheets
    for filename, df_pair in detailed_dataframes.items():
        df, df2 = df_pair
        safe_name = re.sub(r"[\\*?:/\[\]]", "", filename)[:31]
        ws = wb.create_sheet(title=safe_name)
        for row in dataframe_to_rows(df, index=False, header=True):
            ws.append(row)

        ws.append([])
        for row in dataframe_to_rows(df2, index=False, header=True):
            ws.append(row)

        rdate, rcheck = filename.split()
        ws.append([])
        total = df2["amount"].sum()
        ws.append(["Total", total])
        ws.append(["Date", rdate])
        ws.append(["Check Number", rcheck])

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
