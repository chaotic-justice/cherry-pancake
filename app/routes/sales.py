from fastapi.responses import StreamingResponse, HTMLResponse
from openpyxl.utils.dataframe import dataframe_to_rows
from io import BytesIO
from openpyxl import Workbook
from collections import defaultdict
from typing import Optional, Dict, Tuple
import pandas as pd
from fastapi import File, UploadFile


async def process_sales_analysis(
    file: Optional[UploadFile] = File(None), return_validation: bool = False
) -> Tuple[StreamingResponse, Dict[str, Dict[str, float]]]:
    """
    Process sales analysis from uploaded Excel file.

    Args:
        file: Excel file containing sales data
        return_validation: If True, return validation results along with the file

    Returns:
        Tuple of (StreamingResponse with Excel file, validation_results dict)
    """
    if not file or not file.filename:
        return HTMLResponse(
            content="Please upload an Excel file first.", status_code=400
        ), {}

    try:
        content = await file.read()
        df = pd.read_excel(BytesIO(content))
    except Exception as e:
        return HTMLResponse(
            content=f"Error reading Excel file: {str(e)}", status_code=400
        ), {}

    # Set column names
    df.columns = ["Customer", "Cost", "n/a", "cost-of-goods", "profit-percentage"]
    df = df.dropna(how="all")

    # Extract expected totals from the last rows
    keys = ["period-to-date", "year-to-date", "prior-year"]
    expected = defaultdict(float)
    for i, val in enumerate(df["Cost"].iloc[-4:-1].tolist()):
        expected[keys[i]] = round(float(val), 3)

    # Parse salesperson data
    sales = defaultdict(lambda: defaultdict(float))
    j = -1

    for i, row in df.iterrows():
        customer = row["Customer"]
        if i <= j:
            continue
        if isinstance(customer, str):
            if customer.lower().startswith("salesperson"):
                salesperson = customer.split(" ")[1]
                j = i + 3
                temp = i + 1
                while temp <= j:
                    key = "-".join(df["Customer"][temp].lower().strip().split(" "))
                    amount = df["Cost"][temp]
                    sales[salesperson][key[:-1]] += float(amount)
                    temp += 1

    # Aggregate results and remove empty salespersons
    actual = defaultdict(float)
    popped = []

    for salesperson, values in sales.items():
        if sum(values.values()) == 0:
            popped.append(salesperson)
            continue
        actual["period-to-date"] += values["period-to-date"]
        actual["year-to-date"] += values["year-to-date"]
        actual["prior-year"] += values["prior-year"]

    for k in popped:
        sales.pop(k)

    # Round actual values
    for k in actual:
        actual[k] = round(actual[k], 3)

    # Create validation results
    validation_results = {}
    for k in expected:
        validation_results[k] = {
            "expected": expected[k],
            "actual": actual[k],
            "matched": actual[k] == expected[k],
        }

    # Create DataFrame for Excel export
    sales_df = pd.DataFrame.from_dict(sales, orient="index")
    sales_df.reset_index(inplace=True)
    sales_df.columns = ["Salesperson"] + list(sales_df.columns[1:])

    # Generate Excel workbook
    wb = Workbook()
    wb.remove(wb.active)  # Remove default sheet
    ws = wb.create_sheet(title="Sales Report")

    # Add sales data
    for row in dataframe_to_rows(sales_df, index=False, header=True):
        ws.append(row)

    # Add spacing
    for _ in range(2):
        ws.append([])

    # Add validation summary
    ws.append(["Validation Summary"])
    ws.append(["Metric", "Expected", "Actual", "Matched"])
    for k in keys:
        ws.append(
            [
                k.replace("-", " ").title(),
                validation_results[k]["expected"],
                validation_results[k]["actual"],
                "✓" if validation_results[k]["matched"] else "✗",
            ]
        )

    # Save to buffer
    output = BytesIO()
    wb.save(output)
    output.seek(0)

    response = StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=Sales_Analysis.xlsx"},
    )

    return response, validation_results
