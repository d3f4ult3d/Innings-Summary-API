"""
innings_routes.py — Route for the innings summary endpoint.

GET /api/v1/innings/{innings_id}/summary

Thin route — all logic lives in InningsSummaryService.
"""
from fastapi import APIRouter, Depends, HTTPException, status

from models import InningsSummaryResponse
from Iservice import InningsNotFoundError, InningsSummaryService

router = APIRouter()


# ---------------------------------------------------------------------------
# DB dependency — replace with your real session factory
# ---------------------------------------------------------------------------
async def get_db():
    """
    Yield a database session.

    Example (SQLAlchemy async):
        async with AsyncSessionLocal() as session:
            yield session
    """
    yield None


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------
@router.get(
    "/innings/{innings_id}/summary",
    response_model=InningsSummaryResponse,
    summary="Full innings summary computed from ball events",
    responses={
        200: {"description": "Innings summary returned successfully"},
        404: {"description": "Innings not found"},
    },
)
async def get_innings_summary(innings_id: int, db=Depends(get_db)):
    """
    Returns a complete innings-level analytics object for **innings_id**.

    All totals — runs, wickets, extras, legal balls, per-batter and
    per-bowler stats — are computed by aggregating the raw ball_events
    rows rather than reading pre-saved summary fields.

    Designed to be a reusable analytics object consumed by:
    - the scoring page
    - the live stream page
    - AI agent tooling
    """
    try:
        service = InningsSummaryService(db)
        return await service.get_innings_summary(innings_id)
    except InningsNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Innings '{innings_id}' not found.",
        )