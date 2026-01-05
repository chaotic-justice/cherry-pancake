from .library.utils import extract_key, get_store_names
import jinja2
import os
import re
import pandas as pd
from typing import List, Optional
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse, StreamingResponse
from io import BytesIO
from pypdf import PdfReader
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")
environment = jinja2.Environment(loader=jinja2.FileSystemLoader(TEMPLATE_DIR))
template_index = environment.from_string("Hello, {{ name }}!")
template_work = environment.get_template("work.html")

app = FastAPI()


@app.get("/")
async def root():
    message = "This is an example of FastAPI with Jinja2 - go to /hi/<name> to see a template rendered"
    return {"message": message}


@app.get("/hi/{name}")
async def say_hi(name: str):
    message = template_index.render(name=name)
    return {"message": message}


@app.get("/work")
async def work_get():
    html = template_work.render()
    return HTMLResponse(content=html)


@app.post("/work")
async def work_post(
    files: Optional[List[UploadFile]] = File(None),
    store_file: Optional[UploadFile] = File(None),
):
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
                reader = PdfReader(BytesIO(content))
                file_rows = []

                for page in reader.pages:
                    text = page.extract_text()
                    lines = text.split("\n")
                    for line in lines:
                        parts = line.split()

                        # Find the date index
                        date_idx = -1
                        for i, p in enumerate(parts):
                            if re.match(r"\d{1,2}/\d{1,2}/\d{4}", p):
                                date_idx = i
                                break

                        if date_idx != -1 and len(parts) > date_idx:
                            invoice = parts[0]
                            amount_str = parts[-1].replace(",", "")
                            try:
                                amount = float(amount_str)
                                file_rows.append(
                                    {"invoiceNumber": invoice, "amount": amount}
                                )
                            except (ValueError, TypeError):
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

                detailed_dataframes[file.filename] = df
                aggregated_data.append(df[["storeName", "amount"]])

            except Exception as e:
                # Log or ignore error for specific file but keep going
                print(f"Error processing {file.filename}: {e}")

    if not aggregated_data:
        return HTMLResponse(
            content="No valid transaction data found in uploaded PDFs.", status_code=400
        )

    # 3. Aggregate results
    final_df = pd.concat(aggregated_data)
    summary_df = final_df.groupby("storeName", as_index=False).sum()
    summary_df = summary_df.sort_values("amount", ascending=False)
    store_summary = summary_df.to_dict("records")

    # 4. Generate Excel
    wb = Workbook()
    # Summary Sheet
    ws_summary = wb.active
    ws_summary.title = "Aggregated Summary"
    ws_summary.append(["Store Name", "Total Amount"])
    for entry in store_summary:
        ws_summary.append([entry["storeName"], entry["amount"]])

    # Detail Sheets
    for filename, df in detailed_dataframes.items():
        safe_name = re.sub(r"[\\*?:/\[\]]", "", filename)[:31]
        ws = wb.create_sheet(title=safe_name)
        for row in dataframe_to_rows(df, index=False, header=True):
            ws.append(row)
        ws.append([])
        ws.append(["Total", df["amount"].sum()])

    # Save to buffer
    output = BytesIO()
    wb.save(output)
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=Costco_Analysis.xlsx"},
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
