from app.routes.costco import process_costco_analysis
from app.routes.raccoon import process_raccoon_analysis
from app.routes.sales import process_sales_analysis
import jinja2
import os
from typing import List, Optional
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse
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

app = FastAPI()
app.mount(
    "/static",
    StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")),
    name="static",
)


@app.get("/")
async def root():
    html = template_index.render()
    return HTMLResponse(content=html)


@app.get("/costco")
async def costco_get():
    html = template_costco.render()
    return HTMLResponse(content=html)


@app.get("/sales")
async def sales_get():
    html = template_sales.render()
    return HTMLResponse(content=html)


@app.get("/raccoon")
async def raccoon_get():
    return HTMLResponse(content=template_raccoon.render())


@app.post("/costco")
def costco_post(
    files: Optional[List[UploadFile]] = File(None),
    store_file: Optional[UploadFile] = File(None),
):
    """Process Costco analysis - delegates to app.routes.costco"""
    return process_costco_analysis(files, store_file)


@app.post("/raccoon")
async def raccoon_post(
    files: Optional[List[UploadFile]] = File(None),
    ar_files: Optional[List[UploadFile]] = File(None),
):
    return await process_raccoon_analysis(files or [], ar_files or [], template_raccoon)


@app.post("/sales")
def sales_post(
    file: Optional[UploadFile] = File(None),
):
    """Process Sales analysis and download the generated workbook."""
    response, _ = process_sales_analysis(file)
    return response


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=5000)
