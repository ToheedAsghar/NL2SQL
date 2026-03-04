import warnings
warnings.filterwarnings("ignore", message="Core Pydantic V1 functionality isn't compatible with Python 3.14 or greater.")

import logging
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from starlette.requests import Request
import os
import aiosqlite
from models.schemas import FinalOutput
from orchestrator.pipeline import graph
from db.connector import DatabaseConnector

app = FastAPI(title="NL2SQL AI API")

app.mount("/static", StaticFiles(directory="web/static"), name="static")
templates = Jinja2Templates(directory="web/templates")

class GenerateRequest(BaseModel):
    query: str

class ExecuteRequest(BaseModel):
    sql: str

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/api/generate")
async def generate_sql(req: GenerateRequest):
    try:
        final_state = await graph.ainvoke({"user_query": req.query})
        output: FinalOutput = final_state["output"]
        return {
            "sql": output.sql,
            "explanation": output.explanation,
            "safety_report": output.safety_report,
            "optimization_hints": output.optimization_hints
        }
    except Exception as e:
        logging.exception("Error generating SQL")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/execute")
async def execute_sql(req: ExecuteRequest):
    try:
        connector = DatabaseConnector()
        results = await connector.fetch_all(req.sql)
        return {"results": results}
    except Exception as e:
        logging.exception("Error executing SQL")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
