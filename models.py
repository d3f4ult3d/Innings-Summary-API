"""
models.py — Pydantic response models for all endpoints.

  BatterInfo / BowlerInfo     shared between scoreboard + innings summary
  ScoreboardResponse          GET /scoreboard/{match_code}
  BatterSummary               full per-batter row in innings summary
  BowlerSummary               full per-bowler row in innings summary
  InningsSummaryResponse      GET /innings/{innings_id}/summary
"""
from typing import List, Optional
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Shared performer models (used by both endpoints)
# ---------------------------------------------------------------------------

class BatterInfo(BaseModel):
    name:        str
    runs:        int
    balls:       int
    fours:       int
    sixes:       int
    strike_rate: float = Field(..., description="(runs / balls) × 100")


class BowlerInfo(BaseModel):
    name:          str
    overs:         float
    wickets:       int
    runs_conceded: int
    economy:       float = Field(..., description="runs_conceded / overs_bowled")


class ScoreboardResponse(BaseModel):
    # Match context
    match:          str = Field(..., description="'Home vs Away' label")
    match_code:     str
    venue:          str
    match_type:     str = Field(..., description="T20 | ODI | Test")

    # Innings context
    innings_number: int = Field(..., description="0 if innings not yet started")
    batting_team:   str
    bowling_team:   str

    # Live score
    score:    str   = Field(..., description="'runs/wickets', e.g. '147/3'")
    overs:    str   = Field(..., description="'completed.balls', e.g. '14.3'")
    run_rate: float = Field(..., description="Current run rate (runs per over)")

    # Key performers
    top_batter: Optional[BatterInfo] = None
    top_bowler: Optional[BowlerInfo] = None

    # Ball-by-ball feed
    recent_balls: List[str] = Field(
        ...,
        description="Last ≤12 deliveries as symbols: '1'–'6', 'W', '•', 'Wd', 'Nb'",
    )

    # Match state
    status: str = Field(..., description="scheduled | live | completed")


# ---------------------------------------------------------------------------
# Innings summary models
# ---------------------------------------------------------------------------

class BatterSummary(BaseModel):
    """Full batting row computed from raw ball events."""
    name:           str
    runs:           int
    balls_faced:    int
    fours:          int
    sixes:          int
    dot_balls:      int   = Field(..., description="Legal deliveries faced with 0 runs scored")
    strike_rate:    float = Field(..., description="(runs / balls_faced) × 100")
    is_out:         bool


class BowlerSummary(BaseModel):
    """Full bowling row computed from raw ball events."""
    name:          str
    legal_balls:   int    = Field(..., description="Legal deliveries bowled (excludes wides/no-balls)")
    overs:         str    = Field(..., description="'completed.remainder', e.g. '3.2'")
    wickets:       int
    runs_conceded: int
    wides:         int
    no_balls:      int
    dot_balls:     int    = Field(..., description="Legal deliveries where 0 runs were scored")
    economy:       float  = Field(..., description="runs_conceded / overs as decimal")


class InningsSummaryResponse(BaseModel):
    """
    Complete innings-level analytics object computed entirely from ball_events.
    All totals are derived — nothing is read from manual summary fields.
    """
    innings_id:     int
    innings_number: int
    batting_team:   str
    bowling_team:   str

    # Innings totals (computed from ball events)
    total_runs:   int
    wickets:      int
    legal_balls:  int   = Field(..., description="Total legal deliveries in the innings")
    overs:        str   = Field(..., description="'completed.remainder', e.g. '14.3'")
    run_rate:     float = Field(..., description="Runs per over")
    extras:       int
    wides:        int
    no_balls:     int

    # Full scorecards (every batter/bowler who featured)
    batters:  List[BatterSummary]
    bowlers:  List[BowlerSummary]

    # Key performers
    top_batter: Optional[BatterInfo] = None
    top_bowler: Optional[BowlerInfo] = None

    # Ball-by-ball feed (last ≤12 deliveries)
    recent_balls: List[str] = Field(
        ...,
        description="Last ≤12 deliveries as symbols: '1'–'6', 'W', '•', 'Wd', 'Nb'",
    )