from fastapi.responses import HTMLResponse, Response, StreamingResponse
from openpyxl.utils.dataframe import dataframe_to_rows
from io import BytesIO
from openpyxl import Workbook
from collections import defaultdict
import pandas as pd
from fastapi import UploadFile

MAX_FILE_SIZE = 5 * 1024 * 1024
EXCEL_SUFFIXES = (".xls", ".xlsx")


def process_sales_analysis(
    file: UploadFile | None = None,
) -> tuple[Response, dict[str, dict[str, float | bool]]]:
    """
    Process sales analysis from uploaded Excel file.

    Args:
        file: Excel file containing sales data
    Returns:
        Tuple of (StreamingResponse with Excel file, validation_results dict)
    """
    if not file or not file.filename:
        return HTMLResponse(
            content="Please upload an Excel file first.", status_code=400
        ), {}

    if not file.filename.lower().endswith(EXCEL_SUFFIXES):
        return HTMLResponse(
            content="Sales file must be an Excel workbook.", status_code=400
        ), {}

    try:
        content = file.file.read(MAX_FILE_SIZE + 1)
        if len(content) > MAX_FILE_SIZE:
            return HTMLResponse(
                content="Sales file must be 100 MB or smaller.", status_code=400
            ), {}
        df = pd.read_excel(BytesIO(content))
    except Exception:
        return HTMLResponse(
            content="Could not read the sales workbook. Check that it is a valid Excel export.",
            status_code=400,
        ), {}

    if len(df.columns) != 5:
        return HTMLResponse(
            content="Sales workbook must contain exactly five columns.",
            status_code=400,
        ), {}

    # Set column names
    df.columns = ["Customer", "Cost", "n/a", "cost-of-goods", "profit-percentage"]
    df = df.dropna(how="all").reset_index(drop=True)
    if len(df) < 4:
        return HTMLResponse(
            content="Sales workbook does not contain enough summary rows.",
            status_code=400,
        ), {}

    # Extract expected totals from the last rows
    keys = ["period-to-date", "year-to-date", "prior-year"]
    expected = defaultdict(float)
    try:
        for i, val in enumerate(df["Cost"].iloc[-4:-1].tolist()):
            expected[keys[i]] = round(float(val), 3)
    except (TypeError, ValueError):
        return HTMLResponse(
            content="Sales workbook summary values must be numbers.",
            status_code=400,
        ), {}

    # Parse salesperson data
    sales = defaultdict(lambda: defaultdict(float))
    j = -1

    for i, row in df.iterrows():
        customer = row["Customer"]
        if i <= j:
            continue
        if isinstance(customer, str):
            if customer.lower().startswith("salesperson"):
                parts = customer.split(maxsplit=1)
                if len(parts) != 2 or i + 3 >= len(df):
                    return HTMLResponse(
                        content="Each salesperson must have a name and three summary rows.",
                        status_code=400,
                    ), {}
                salesperson = parts[1]
                j = i + 3
                temp = i + 1
                while temp <= j:
                    label = df.at[temp, "Customer"]
                    try:
                        key = "-".join(label.lower().strip().split()).rstrip(":")
                        if key not in keys:
                            raise ValueError
                        sales[salesperson][key] += float(df.at[temp, "Cost"])
                    except (AttributeError, TypeError, ValueError):
                        return HTMLResponse(
                            content=(
                                f"{salesperson} must have period-to-date, year-to-date, "
                                "and prior-year numeric rows."
                            ),
                            status_code=400,
                        ), {}
                    temp += 1

    if not sales:
        return HTMLResponse(
            content="Sales workbook does not contain any salesperson sections.",
            status_code=400,
        ), {}

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
