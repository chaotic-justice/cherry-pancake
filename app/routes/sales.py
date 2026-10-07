from io import BytesIO

from fastapi import UploadFile
from fastapi.responses import HTMLResponse, Response, StreamingResponse

from app.library.sales_analysis import SalesInputError, build_sales_report


MAX_FILE_SIZE = 5 * 1024 * 1024
EXCEL_SUFFIXES = (".xls", ".xlsx")


def process_sales_analysis(file: UploadFile | None = None) -> Response:
    if not file or not file.filename:
        return HTMLResponse(
            content="Please upload an Excel file first.", status_code=400
        )
    if not file.filename.lower().endswith(EXCEL_SUFFIXES):
        return HTMLResponse(
            content="Sales file must be an Excel workbook.", status_code=400
        )

    content = file.file.read(MAX_FILE_SIZE + 1)
    if len(content) > MAX_FILE_SIZE:
        return HTMLResponse(
            content="Sales file must be 5 MB or smaller.", status_code=400
        )

    try:
        report = build_sales_report(content)
    except SalesInputError as error:
        return HTMLResponse(content=str(error), status_code=400)

    return StreamingResponse(
        BytesIO(report),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=Sales_Analysis.xlsx"},
    )
