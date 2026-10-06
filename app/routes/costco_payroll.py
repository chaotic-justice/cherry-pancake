from fastapi import UploadFile
from fastapi.responses import HTMLResponse, Response

from app.library.costco_payroll import (
    PayrollFile,
    PayrollInputError,
    build_costco_payroll_master,
)


def process_costco_payroll(files: list[UploadFile] | None = None) -> Response:
    uploads = [
        PayrollFile(file.filename or "", file.file)
        for file in files or []
        if file.filename
    ]
    try:
        generated = build_costco_payroll_master(uploads)
    except PayrollInputError as error:
        return HTMLResponse(content=str(error), status_code=400)
    return Response(
        content=generated.body,
        media_type=generated.media_type,
        headers={
            "Content-Disposition": f'attachment; filename="{generated.download_name}"'
        },
    )
