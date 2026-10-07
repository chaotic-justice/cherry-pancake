from collections import defaultdict
from io import BytesIO

import pandas as pd
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows


METRICS = ("period-to-date", "year-to-date", "prior-year")


class SalesInputError(ValueError):
    pass


def build_sales_report(content: bytes) -> bytes:
    try:
        frame = pd.read_excel(BytesIO(content))
    except Exception as error:
        raise SalesInputError(
            "Could not read sales workbook. Check that it is a valid Excel export."
        ) from error

    if len(frame.columns) != 5:
        raise SalesInputError("Sales workbook must contain exactly five columns.")

    frame.columns = [
        "Customer",
        "Cost",
        "n/a",
        "cost-of-goods",
        "profit-percentage",
    ]
    frame = frame.dropna(how="all").reset_index(drop=True)
    if len(frame) < 4:
        raise SalesInputError("Sales workbook does not contain enough summary rows.")

    try:
        expected = {
            metric: round(float(value), 3)
            for metric, value in zip(
                METRICS, frame["Cost"].iloc[-4:-1].tolist(), strict=True
            )
        }
    except (TypeError, ValueError) as error:
        raise SalesInputError("Sales workbook summary totals must be numbers.") from error

    sales = defaultdict(lambda: defaultdict(float))
    last_section_row = -1
    for row_index, row in frame.iterrows():
        customer = row["Customer"]
        if row_index <= last_section_row or not isinstance(customer, str):
            continue
        if not customer.lower().startswith("salesperson"):
            continue

        parts = customer.split(maxsplit=1)
        if len(parts) != 2 or row_index + 3 >= len(frame):
            raise SalesInputError(
                "Each salesperson must have a name and three summary rows."
            )

        salesperson = parts[1].strip()
        section = {}
        last_section_row = row_index + 3
        for summary_row in range(row_index + 1, last_section_row + 1):
            label = frame.at[summary_row, "Customer"]
            try:
                metric = "-".join(label.lower().strip().split()).rstrip(":")
                if metric not in METRICS or metric in section:
                    raise ValueError
                section[metric] = float(frame.at[summary_row, "Cost"])
            except (AttributeError, TypeError, ValueError) as error:
                raise SalesInputError(
                    f"{salesperson} must have distinct period-to-date, "
                    "year-to-date, and prior-year numeric rows."
                ) from error

        for metric in METRICS:
            sales[salesperson][metric] += section[metric]

    if not sales:
        raise SalesInputError(
            "Sales workbook does not contain any salesperson sections."
        )

    included = {
        salesperson: values
        for salesperson, values in sales.items()
        if any(values[metric] != 0 for metric in METRICS)
    }
    actual = {
        metric: round(sum(values[metric] for values in included.values()), 3)
        for metric in METRICS
    }
    validation = {
        metric: {
            "expected": expected[metric],
            "actual": actual[metric],
            "matched": actual[metric] == expected[metric],
        }
        for metric in METRICS
    }

    sales_frame = pd.DataFrame(
        [
            {
                "Salesperson": salesperson,
                **{metric: values[metric] for metric in METRICS},
            }
            for salesperson, values in included.items()
        ],
        columns=["Salesperson", *METRICS],
    )

    workbook = Workbook()
    workbook.remove(workbook.active)
    worksheet = workbook.create_sheet(title="Sales Report")
    for row in dataframe_to_rows(sales_frame, index=False, header=True):
        worksheet.append(row)

    worksheet.append([])
    worksheet.append([])
    worksheet.append(["Validation Summary"])
    worksheet.append(["Metric", "Expected", "Actual", "Matched"])
    for metric in METRICS:
        result = validation[metric]
        worksheet.append(
            [
                metric.replace("-", " ").title(),
                result["expected"],
                result["actual"],
                "✓" if result["matched"] else "✗",
            ]
        )

    output = BytesIO()
    workbook.save(output)
    return output.getvalue()
