from app.routes.costco import process_costco_analysis
from app.routes.sales import process_sales_analysis
import jinja2
import os
from typing import List, Optional
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse

TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "app", "templates")
environment = jinja2.Environment(loader=jinja2.FileSystemLoader(TEMPLATE_DIR))
template_index = environment.get_template("index.html")
template_costco = environment.get_template("costco.html")
template_sales = environment.get_template("sales.html")

app = FastAPI()

# Store the last analysis result temporarily (in production, use session/cache)
last_sales_analysis = {}


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


@app.post("/costco")
async def costco_post(
    files: Optional[List[UploadFile]] = File(None),
    store_file: Optional[UploadFile] = File(None),
):
    """Process Costco analysis - delegates to app.routes.costco"""
    return await process_costco_analysis(files, store_file)


@app.post("/sales")
async def sales_post(
    file: Optional[UploadFile] = File(None),
):
    """Process Sales analysis and display validation results"""
    response, validation = await process_sales_analysis(file)

    # Store the response for download
    global last_sales_analysis
    last_sales_analysis = {"response": response, "validation": validation}

    # Render template with validation results
    html = template_sales.render(validation=validation, show_download=True)
    return HTMLResponse(content=html)


@app.get("/sales/download")
async def sales_download():
    """Download the last generated sales analysis Excel file"""
    if last_sales_analysis and "response" in last_sales_analysis:
        return last_sales_analysis["response"]
    return HTMLResponse(
        content="No analysis available. Please upload a file first.", status_code=400
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=5000)
