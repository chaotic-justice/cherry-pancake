from app.routes.costco import process_costco_analysis
from app.routes.raccoon import process_raccoon_analysis
from app.routes.sales import process_sales_analysis
from app.library.costco_upload_policy import COSTCO_UPLOAD_POLICY
import jinja2
import os
from typing import Annotated
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles

TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")
environment = jinja2.Environment(
    loader=jinja2.FileSystemLoader(TEMPLATE_DIR),
    autoescape=jinja2.select_autoescape(["html"]),
)
template_index = environment.get_template("index.html")
template_costco = environment.get_template("costco.html")
template_raccoon = environment.get_template("raccoon.html")
template_sales = environment.get_template("sales.html")
EXCEL_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
DOWNLOAD_RESPONSES = {
    200: {"content": {EXCEL_MEDIA_TYPE: {}}},
    400: {"content": {"text/html": {}}},
}

app = FastAPI()
app.mount(
    "/static",
    StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")),
    name="static",
)


@app.get("/", response_class=HTMLResponse)
def root() -> HTMLResponse:
    html = template_index.render()
    return HTMLResponse(content=html)


@app.get("/costco", response_class=HTMLResponse)
def costco_get() -> HTMLResponse:
    html = template_costco.render(upload_policy=COSTCO_UPLOAD_POLICY)
    return HTMLResponse(content=html)


@app.get("/sales", response_class=HTMLResponse)
def sales_get() -> HTMLResponse:
    html = template_sales.render()
    return HTMLResponse(content=html)


@app.get("/raccoon", response_class=HTMLResponse)
def raccoon_get() -> HTMLResponse:
    return HTMLResponse(content=template_raccoon.render())


@app.post(
    "/costco",
    response_class=Response,
    responses=DOWNLOAD_RESPONSES,
)
def costco_post(
    files: Annotated[list[UploadFile] | None, File()] = None,
    store_file: Annotated[UploadFile | None, File()] = None,
) -> Response:
    """Process Costco analysis - delegates to app.routes.costco"""
    return process_costco_analysis(files, store_file)


@app.post("/raccoon", response_class=HTMLResponse)
async def raccoon_post(
    files: Annotated[list[UploadFile] | None, File()] = None,
    ar_files: Annotated[list[UploadFile] | None, File()] = None,
) -> HTMLResponse:
    return await process_raccoon_analysis(files or [], ar_files or [], template_raccoon)


@app.post(
    "/sales",
    response_class=Response,
    responses=DOWNLOAD_RESPONSES,
)
def sales_post(
    file: Annotated[UploadFile | None, File()] = None,
) -> Response:
    """Process Sales analysis and download the generated workbook."""
    response, _ = process_sales_analysis(file)
    return response


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=5000)
