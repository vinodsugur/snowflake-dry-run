from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from snowflake_dryrun.engine import run_dry_run
from snowflake_dryrun.models import DryRunRequest, DryRunResult

STATIC = Path(__file__).parent / "static"

app = FastAPI(
    title="Snowflake Query Advisor",
    description="Analyze Snowflake SQL via EXPLAIN JSON, suggest rewrites, and size the warehouse.",
    version="0.2.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/assets", StaticFiles(directory=STATIC), name="assets")


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC / "index.html")


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/dry-run", response_model=DryRunResult)
def dry_run(req: DryRunRequest) -> DryRunResult:
    if not (req.sql or "").strip() and not req.explain_json:
        raise HTTPException(status_code=400, detail="Provide sql and/or explain_json.")
    return run_dry_run(req, allow_snowflake=True)
