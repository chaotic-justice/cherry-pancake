from fastapi import UploadFile
from fastapi.responses import HTMLResponse, Response

from app.library.costco_analysis import (
    CostcoInputError,
    InputFile,
    analyze_costco_reports,
)


def process_costco_analysis(
    files: list[UploadFile] | None = None,
    store_file: UploadFile | None = None,
) -> Response:
    """Adapt Costco uploads to the reporting module and return an HTTP response."""
    reports = [InputFile(file.filename or "", file.file) for file in files or []]
    store_update = (
        InputFile(store_file.filename or "", store_file.file)
        if store_file and store_file.filename
        else None
    )

    try:
        generated = analyze_costco_reports(reports, store_update)
    except CostcoInputError as error:
        return HTMLResponse(content=str(error), status_code=400)

    return Response(
        content=generated.body,
        media_type=generated.media_type,
        headers={
            "Content-Disposition": (
                f'attachment; filename="{generated.download_name}"'
            )
        },
    )
