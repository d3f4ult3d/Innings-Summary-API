"""
innings_service.py — Innings summary computed entirely from raw ball events.

Layout
------
  _fetch_*   DB stubs with exact SQL in comments. Swap bodies for real ORM calls.

  _calc_*    Pure helpers. No DB access, fully unit-testable.
             Reuses the same conventions as service.py.

  get_innings_summary   Public entry point called by innings_routes.py.

Key design decision
-------------------
All totals (runs, wickets, extras, legal_balls, per-batter stats, per-bowler
stats) are computed by aggregating the ball_events rows rather than reading
the pre-saved summary fields on the innings row. This makes the endpoint the
single reliable source of truth for innings-level analytics.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional

from models import (
    BatterInfo,
    BatterSummary,
    BowlerInfo,
    BowlerSummary,
    InningsSummaryResponse,
)


class InningsNotFoundError(Exception):
    """Raised when innings_id does not exist in the database."""


class InningsSummaryService:

    def __init__(self, db: Any) -> None:
        self.db = db

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def get_innings_summary(self, innings_id: int) -> InningsSummaryResponse:
        """
        Build and return the full innings summary for innings_id.
        All aggregates are derived from ball_events rows.

        Raises InningsNotFoundError if the innings does not exist.
        """
        innings    = await self._fetch_innings(innings_id)
        ball_events = await self._fetch_all_ball_events(innings_id)

        # Aggregate everything from the raw ball log
        totals         = self._calc_innings_totals(ball_events)
        batter_rows    = await self._fetch_batter_names(innings_id)
        bowler_rows    = await self._fetch_bowler_names(innings_id)
        batters        = self._calc_batter_summaries(ball_events, batter_rows)
        bowlers        = self._calc_bowler_summaries(ball_events, bowler_rows)
        recent_balls   = self._calc_ball_symbols(ball_events[-12:])

        return InningsSummaryResponse(
            innings_id     = innings_id,
            innings_number = innings["innings_number"],
            batting_team   = innings["batting_team"],
            bowling_team   = innings["bowling_team"],
            total_runs     = totals["total_runs"],
            wickets        = totals["wickets"],
            legal_balls    = totals["legal_balls"],
            overs          = self._calc_overs(totals["legal_balls"]),
            run_rate       = self._calc_run_rate(totals["total_runs"], totals["legal_balls"]),
            extras         = totals["extras"],
            wides          = totals["wides"],
            no_balls       = totals["no_balls"],
            batters        = batters,
            bowlers        = bowlers,
            top_batter     = self._calc_top_batter(batters),
            top_bowler     = self._calc_top_bowler(bowlers),
            recent_balls   = recent_balls,
        )

    # ------------------------------------------------------------------
    # Calculation helpers (pure — no DB, fully unit-testable)
    # ------------------------------------------------------------------

    @staticmethod
    def _calc_innings_totals(ball_events: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Single pass over all ball events to derive innings-level totals.
        Returns: total_runs, wickets, legal_balls, extras, wides, no_balls.
        """
        total_runs  = 0
        wickets     = 0
        legal_balls = 0
        extras      = 0
        wides       = 0
        no_balls    = 0

        for b in ball_events:
            total_runs += b["runs_scored"] + b["extras"]
            extras     += b["extras"]
            if b["extra_type"] == "wide":
                wides += 1
            elif b["extra_type"] == "no_ball":
                no_balls += 1
            else:
                # Only legal deliveries count toward overs
                legal_balls += 1
            if b["is_wicket"]:
                wickets += 1

        return {
            "total_runs":  total_runs,
            "wickets":     wickets,
            "legal_balls": legal_balls,
            "extras":      extras,
            "wides":       wides,
            "no_balls":    no_balls,
        }

    @staticmethod
    def _calc_batter_summaries(
        ball_events: List[Dict[str, Any]],
        batter_rows: List[Dict[str, Any]],
    ) -> List[BatterSummary]:
        """
        Aggregate ball events by batsman_id to build per-batter stats.
        batter_rows supplies the player name and is_out flag.
        """
        # Build name + is_out lookup keyed on player_id
        meta: Dict[int, Dict[str, Any]] = {
            r["player_id"]: {"name": r["player_name"], "is_out": r["is_out"]}
            for r in batter_rows
        }

        # Accumulate per-batter counters
        stats: Dict[int, Dict[str, Any]] = defaultdict(lambda: {
            "runs": 0, "balls_faced": 0, "fours": 0, "sixes": 0, "dot_balls": 0
        })

        for b in ball_events:
            pid = b.get("batsman_id")
            if pid is None:
                continue
            s = stats[pid]
            # Wides don't count as a ball faced
            if b.get("extra_type") != "wide":
                s["balls_faced"] += 1
                if b["runs_scored"] == 0 and not b["is_wicket"]:
                    s["dot_balls"] += 1
            s["runs"] += b["runs_scored"]
            if b["runs_scored"] == 4:
                s["fours"] += 1
            elif b["runs_scored"] == 6:
                s["sixes"] += 1

        summaries = []
        for pid, s in stats.items():
            m = meta.get(pid, {"name": f"Player {pid}", "is_out": False})
            summaries.append(BatterSummary(
                name        = m["name"],
                runs        = s["runs"],
                balls_faced = s["balls_faced"],
                fours       = s["fours"],
                sixes       = s["sixes"],
                dot_balls   = s["dot_balls"],
                strike_rate = InningsSummaryService._calc_strike_rate(
                    s["runs"], s["balls_faced"]
                ),
                is_out      = m["is_out"],
            ))

        # Order by batting contribution (runs desc, then balls desc)
        return sorted(summaries, key=lambda x: (-x.runs, -x.balls_faced))

    @staticmethod
    def _calc_bowler_summaries(
        ball_events: List[Dict[str, Any]],
        bowler_rows: List[Dict[str, Any]],
    ) -> List[BowlerSummary]:
        """
        Aggregate ball events by bowler_id to build per-bowler stats.
        bowler_rows supplies the player name.
        """
        name_lookup: Dict[int, str] = {
            r["player_id"]: r["player_name"] for r in bowler_rows
        }

        stats: Dict[int, Dict[str, Any]] = defaultdict(lambda: {
            "legal_balls": 0, "wickets": 0, "runs_conceded": 0,
            "wides": 0, "no_balls": 0, "dot_balls": 0
        })

        for b in ball_events:
            pid = b.get("bowler_id")
            if pid is None:
                continue
            s = stats[pid]
            s["runs_conceded"] += b["runs_scored"] + b["extras"]
            if b["extra_type"] == "wide":
                s["wides"] += 1
            elif b["extra_type"] == "no_ball":
                s["no_balls"] += 1
            else:
                s["legal_balls"] += 1
                if b["runs_scored"] == 0 and not b["is_wicket"]:
                    s["dot_balls"] += 1
            if b["is_wicket"]:
                s["wickets"] += 1

        summaries = []
        for pid, s in stats.items():
            overs_str = InningsSummaryService._calc_overs(s["legal_balls"])
            overs_dec = InningsSummaryService._overs_to_decimal(s["legal_balls"])
            summaries.append(BowlerSummary(
                name          = name_lookup.get(pid, f"Player {pid}"),
                legal_balls   = s["legal_balls"],
                overs         = overs_str,
                wickets       = s["wickets"],
                runs_conceded = s["runs_conceded"],
                wides         = s["wides"],
                no_balls      = s["no_balls"],
                dot_balls     = s["dot_balls"],
                economy       = InningsSummaryService._calc_economy(
                    s["runs_conceded"], overs_dec
                ),
            ))

        # Order by wickets desc, then economy asc
        return sorted(summaries, key=lambda x: (-x.wickets, x.economy))

    @staticmethod
    def _calc_top_batter(batters: List[BatterSummary]) -> Optional[BatterInfo]:
        """Highest run-scorer still not out; falls back to overall highest."""
        if not batters:
            return None
        active = [b for b in batters if not b.is_out] or batters
        best = max(active, key=lambda b: b.runs)
        return BatterInfo(
            name        = best.name,
            runs        = best.runs,
            balls       = best.balls_faced,
            fours       = best.fours,
            sixes       = best.sixes,
            strike_rate = best.strike_rate,
        )

    @staticmethod
    def _calc_top_bowler(bowlers: List[BowlerSummary]) -> Optional[BowlerInfo]:
        """Most wickets; economy as tie-breaker (lower is better)."""
        if not bowlers:
            return None
        best = max(bowlers, key=lambda b: (b.wickets, -b.economy))
        return BowlerInfo(
            name          = best.name,
            overs         = InningsSummaryService._overs_to_decimal(best.legal_balls),
            wickets       = best.wickets,
            runs_conceded = best.runs_conceded,
            economy       = best.economy,
        )

    @staticmethod
    def _calc_overs(legal_balls: int) -> str:
        """73 legal balls → '12.1'  |  90 legal balls → '15.0'"""
        return f"{legal_balls // 6}.{legal_balls % 6}"

    @staticmethod
    def _overs_to_decimal(legal_balls: int) -> float:
        """Convert legal ball count to decimal overs for economy calculation."""
        return round(legal_balls / 6, 4)

    @staticmethod
    def _calc_run_rate(total_runs: int, legal_balls: int) -> float:
        if legal_balls == 0:
            return 0.0
        return round(total_runs / (legal_balls / 6), 2)

    @staticmethod
    def _calc_strike_rate(runs: int, balls: int) -> float:
        if balls == 0:
            return 0.0
        return round((runs / balls) * 100, 2)

    @staticmethod
    def _calc_economy(runs_conceded: int, overs_decimal: float) -> float:
        if overs_decimal == 0:
            return 0.0
        return round(runs_conceded / overs_decimal, 2)

    @staticmethod
    def _calc_ball_symbols(balls: List[Dict[str, Any]]) -> List[str]:
        """Same symbol mapping used across the codebase."""
        symbols = []
        for b in balls:
            if b["is_wicket"]:
                symbols.append("W")
            elif b.get("extra_type") == "wide":
                symbols.append("Wd")
            elif b.get("extra_type") == "no_ball":
                symbols.append("Nb")
            elif b["runs_scored"] == 0:
                symbols.append("•")
            else:
                symbols.append(str(b["runs_scored"]))
        return symbols

    # ------------------------------------------------------------------
    # DB fetch stubs (replace each body with your real ORM/query call)
    # ------------------------------------------------------------------

    async def _fetch_innings(self, innings_id: int) -> Dict[str, Any]:
        """
        SQL:
            SELECT i.id, i.innings_number, i.status,
                   bt.name  AS batting_team,
                   bwt.name AS bowling_team
            FROM   innings i
            JOIN   teams bt  ON bt.id  = i.batting_team_id
            JOIN   teams bwt ON bwt.id = i.bowling_team_id
            WHERE  i.id = :innings_id

        Raise InningsNotFoundError if no row returned.
        """
        if innings_id == 0:
            raise InningsNotFoundError(innings_id)
        return {
            "id":             innings_id,
            "innings_number": 1,
            "batting_team":   "Mumbai Indians",
            "bowling_team":   "Chennai Super Kings",
            "status":         "in_progress",
        }

    async def _fetch_all_ball_events(self, innings_id: int) -> List[Dict[str, Any]]:
        """
        SQL:
            SELECT be.runs_scored, be.extras, be.extra_type,
                   be.is_wicket, be.wicket_type,
                   be.batsman_id, be.bowler_id,
                   be.over_number, be.ball_number
            FROM   ball_events be
            WHERE  be.innings_id = :innings_id
            ORDER  BY be.over_number ASC, be.ball_number ASC

        Returns every ball in chronological order.
        All innings-level aggregates are computed from this list.
        """
        return [
            {"runs_scored": 0, "extras": 0, "extra_type": None,      "is_wicket": False, "batsman_id": 1, "bowler_id": 10, "over_number": 1, "ball_number": 1},
            {"runs_scored": 4, "extras": 0, "extra_type": None,      "is_wicket": False, "batsman_id": 1, "bowler_id": 10, "over_number": 1, "ball_number": 2},
            {"runs_scored": 0, "extras": 1, "extra_type": "wide",    "is_wicket": False, "batsman_id": 1, "bowler_id": 10, "over_number": 1, "ball_number": 2},
            {"runs_scored": 1, "extras": 0, "extra_type": None,      "is_wicket": False, "batsman_id": 1, "bowler_id": 10, "over_number": 1, "ball_number": 3},
            {"runs_scored": 6, "extras": 0, "extra_type": None,      "is_wicket": False, "batsman_id": 2, "bowler_id": 10, "over_number": 1, "ball_number": 4},
            {"runs_scored": 0, "extras": 0, "extra_type": None,      "is_wicket": False, "batsman_id": 2, "bowler_id": 10, "over_number": 1, "ball_number": 5},
            {"runs_scored": 2, "extras": 0, "extra_type": None,      "is_wicket": False, "batsman_id": 1, "bowler_id": 10, "over_number": 1, "ball_number": 6},
            {"runs_scored": 0, "extras": 0, "extra_type": None,      "is_wicket": True,  "batsman_id": 2, "bowler_id": 11, "over_number": 2, "ball_number": 1},
            {"runs_scored": 4, "extras": 0, "extra_type": None,      "is_wicket": False, "batsman_id": 3, "bowler_id": 11, "over_number": 2, "ball_number": 2},
            {"runs_scored": 1, "extras": 0, "extra_type": None,      "is_wicket": False, "batsman_id": 1, "bowler_id": 11, "over_number": 2, "ball_number": 3},
            {"runs_scored": 0, "extras": 1, "extra_type": "no_ball", "is_wicket": False, "batsman_id": 3, "bowler_id": 11, "over_number": 2, "ball_number": 3},
            {"runs_scored": 0, "extras": 0, "extra_type": None,      "is_wicket": False, "batsman_id": 3, "bowler_id": 11, "over_number": 2, "ball_number": 4},
            {"runs_scored": 6, "extras": 0, "extra_type": None,      "is_wicket": False, "batsman_id": 1, "bowler_id": 11, "over_number": 2, "ball_number": 5},
            {"runs_scored": 1, "extras": 0, "extra_type": None,      "is_wicket": False, "batsman_id": 3, "bowler_id": 11, "over_number": 2, "ball_number": 6},
        ]

    async def _fetch_batter_names(self, innings_id: int) -> List[Dict[str, Any]]:
        """
        SQL:
            SELECT bs.player_id, bs.is_out, p.name AS player_name
            FROM   batting_scorecards bs
            JOIN   players p ON p.id = bs.player_id
            WHERE  bs.innings_id = :innings_id

        Used to attach names + dismissal status to ball-event aggregates.
        """
        return [
            {"player_id": 1, "player_name": "Rohit Sharma",      "is_out": False},
            {"player_id": 2, "player_name": "Ishan Kishan",       "is_out": True},
            {"player_id": 3, "player_name": "Suryakumar Yadav",   "is_out": False},
        ]

    async def _fetch_bowler_names(self, innings_id: int) -> List[Dict[str, Any]]:
        """
        SQL:
            SELECT bs.player_id, p.name AS player_name
            FROM   bowling_scorecards bs
            JOIN   players p ON p.id = bs.player_id
            WHERE  bs.innings_id = :innings_id

        Used to attach names to ball-event aggregates.
        """
        return [
            {"player_id": 10, "player_name": "Deepak Chahar"},
            {"player_id": 11, "player_name": "Ravindra Jadeja"},
        ]