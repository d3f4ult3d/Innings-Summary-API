"""
tests.py — Unit tests for all ScoreboardService calculation helpers.

All tests run without a database — the helpers are pure functions.

Run:
    pytest tests.py -v
"""
import pytest
from Iservice import ScoreboardService


# ------------------------------------------------------------------
# _calc_overs
# ------------------------------------------------------------------
class TestCalcOvers:
    def test_full_overs(self):
        assert ScoreboardService._calc_overs(30) == "5.0"

    def test_partial_over(self):
        assert ScoreboardService._calc_overs(87) == "14.3"

    def test_zero_balls(self):
        assert ScoreboardService._calc_overs(0) == "0.0"

    def test_one_ball(self):
        assert ScoreboardService._calc_overs(1) == "0.1"

    def test_end_of_over(self):
        assert ScoreboardService._calc_overs(6) == "1.0"


# ------------------------------------------------------------------
# _calc_run_rate
# ------------------------------------------------------------------
class TestCalcRunRate:
    def test_normal(self):
        assert ScoreboardService._calc_run_rate(147, 87) == pytest.approx(10.14, abs=0.01)

    def test_zero_balls_returns_zero(self):
        assert ScoreboardService._calc_run_rate(0, 0) == 0.0

    def test_zero_runs(self):
        assert ScoreboardService._calc_run_rate(0, 6) == 0.0


# ------------------------------------------------------------------
# _calc_strike_rate
# ------------------------------------------------------------------
class TestCalcStrikeRate:
    def test_normal(self):
        assert ScoreboardService._calc_strike_rate(72, 48) == 150.0

    def test_zero_balls(self):
        assert ScoreboardService._calc_strike_rate(10, 0) == 0.0

    def test_hundred_percent(self):
        assert ScoreboardService._calc_strike_rate(6, 6) == 100.0


# ------------------------------------------------------------------
# _calc_economy
# ------------------------------------------------------------------
class TestCalcEconomy:
    def test_normal(self):
        assert ScoreboardService._calc_economy(24, 3.0) == 8.0

    def test_zero_overs(self):
        assert ScoreboardService._calc_economy(5, 0) == 0.0

    def test_fractional_overs(self):
        assert ScoreboardService._calc_economy(18, 2.3) == pytest.approx(7.83, abs=0.01)


# ------------------------------------------------------------------
# _calc_ball_symbols
# ------------------------------------------------------------------
class TestCalcBallSymbols:
    def _ball(self, runs=0, extra_type=None, is_wicket=False, extras=0):
        return {"runs_scored": runs, "extras": extras,
                "extra_type": extra_type, "is_wicket": is_wicket}

    def test_six(self):
        assert ScoreboardService._calc_ball_symbols([self._ball(6)]) == ["6"]

    def test_four(self):
        assert ScoreboardService._calc_ball_symbols([self._ball(4)]) == ["4"]

    def test_wicket(self):
        assert ScoreboardService._calc_ball_symbols([self._ball(is_wicket=True)]) == ["W"]

    def test_dot_ball(self):
        assert ScoreboardService._calc_ball_symbols([self._ball(0)]) == ["•"]

    def test_wide(self):
        assert ScoreboardService._calc_ball_symbols(
            [self._ball(extra_type="wide", extras=1)]
        ) == ["Wd"]

    def test_no_ball(self):
        assert ScoreboardService._calc_ball_symbols(
            [self._ball(extra_type="no_ball", extras=1)]
        ) == ["Nb"]

    def test_mixed_sequence(self):
        balls = [
            self._ball(4),
            self._ball(0),
            self._ball(is_wicket=True),
            self._ball(extra_type="wide", extras=1),
            self._ball(6),
        ]
        assert ScoreboardService._calc_ball_symbols(balls) == ["4", "•", "W", "Wd", "6"]

    def test_empty_list(self):
        assert ScoreboardService._calc_ball_symbols([]) == []


# ------------------------------------------------------------------
# _calc_top_batter
# ------------------------------------------------------------------
class TestCalcTopBatter:
    CARDS = [
        {"player_name": "Rohit",  "runs_scored": 72, "balls_faced": 48,
         "fours": 7, "sixes": 4, "is_out": False},
        {"player_name": "Kishan", "runs_scored": 41, "balls_faced": 30,
         "fours": 4, "sixes": 2, "is_out": True},
        {"player_name": "SKY",    "runs_scored": 22, "balls_faced": 14,
         "fours": 2, "sixes": 1, "is_out": False},
    ]

    def test_prefers_not_out_highest(self):
        result = ScoreboardService._calc_top_batter(self.CARDS)
        assert result.name == "Rohit"
        assert result.runs == 72
        assert result.strike_rate == 150.0

    def test_falls_back_when_all_out(self):
        all_out = [{**c, "is_out": True} for c in self.CARDS]
        result = ScoreboardService._calc_top_batter(all_out)
        assert result.name == "Rohit"

    def test_returns_none_for_empty(self):
        assert ScoreboardService._calc_top_batter([]) is None


# ------------------------------------------------------------------
# _calc_top_bowler
# ------------------------------------------------------------------
class TestCalcTopBowler:
    CARDS = [
        {"player_name": "Chahar", "overs_bowled": 3.0,
         "wickets_taken": 2, "runs_conceded": 24},
        {"player_name": "Jadeja", "overs_bowled": 2.3,
         "wickets_taken": 1, "runs_conceded": 18},
    ]

    def test_most_wickets_wins(self):
        result = ScoreboardService._calc_top_bowler(self.CARDS)
        assert result.name == "Chahar"
        assert result.wickets == 2
        assert result.economy == 8.0

    def test_economy_tiebreak(self):
        tied = [
            {"player_name": "A", "overs_bowled": 2.0,
             "wickets_taken": 2, "runs_conceded": 20},  # economy 10
            {"player_name": "B", "overs_bowled": 2.0,
             "wickets_taken": 2, "runs_conceded": 14},  # economy 7 — better
        ]
        result = ScoreboardService._calc_top_bowler(tied)
        assert result.name == "B"

    def test_returns_none_for_empty(self):
        assert ScoreboardService._calc_top_bowler([]) is None


# ------------------------------------------------------------------
# _calc_score
# ------------------------------------------------------------------
class TestCalcScore:
    def test_normal(self):
        assert ScoreboardService._calc_score({"total_runs": 147, "total_wickets": 3}) == "147/3"

    def test_all_out(self):
        assert ScoreboardService._calc_score({"total_runs": 203, "total_wickets": 10}) == "203/10"

    def test_zero(self):
        assert ScoreboardService._calc_score({"total_runs": 0, "total_wickets": 0}) == "0/0"


# ==================================================================
# InningsSummaryService tests
# ==================================================================
from Iservice import InningsSummaryService

# Shared ball event fixture used across innings tests
BALL_EVENTS = [
    {"runs_scored": 0, "extras": 0, "extra_type": None,      "is_wicket": False, "batsman_id": 1, "bowler_id": 10},
    {"runs_scored": 4, "extras": 0, "extra_type": None,      "is_wicket": False, "batsman_id": 1, "bowler_id": 10},
    {"runs_scored": 0, "extras": 1, "extra_type": "wide",    "is_wicket": False, "batsman_id": 1, "bowler_id": 10},
    {"runs_scored": 1, "extras": 0, "extra_type": None,      "is_wicket": False, "batsman_id": 1, "bowler_id": 10},
    {"runs_scored": 6, "extras": 0, "extra_type": None,      "is_wicket": False, "batsman_id": 2, "bowler_id": 10},
    {"runs_scored": 0, "extras": 0, "extra_type": None,      "is_wicket": False, "batsman_id": 2, "bowler_id": 10},
    {"runs_scored": 2, "extras": 0, "extra_type": None,      "is_wicket": False, "batsman_id": 1, "bowler_id": 10},
    {"runs_scored": 0, "extras": 0, "extra_type": None,      "is_wicket": True,  "batsman_id": 2, "bowler_id": 11},
    {"runs_scored": 4, "extras": 0, "extra_type": None,      "is_wicket": False, "batsman_id": 3, "bowler_id": 11},
    {"runs_scored": 1, "extras": 0, "extra_type": None,      "is_wicket": False, "batsman_id": 1, "bowler_id": 11},
    {"runs_scored": 0, "extras": 1, "extra_type": "no_ball", "is_wicket": False, "batsman_id": 3, "bowler_id": 11},
    {"runs_scored": 0, "extras": 0, "extra_type": None,      "is_wicket": False, "batsman_id": 3, "bowler_id": 11},
    {"runs_scored": 6, "extras": 0, "extra_type": None,      "is_wicket": False, "batsman_id": 1, "bowler_id": 11},
    {"runs_scored": 1, "extras": 0, "extra_type": None,      "is_wicket": False, "batsman_id": 3, "bowler_id": 11},
]

BATTER_ROWS = [
    {"player_id": 1, "player_name": "Rohit Sharma",    "is_out": False},
    {"player_id": 2, "player_name": "Ishan Kishan",    "is_out": True},
    {"player_id": 3, "player_name": "Suryakumar Yadav","is_out": False},
]

BOWLER_ROWS = [
    {"player_id": 10, "player_name": "Deepak Chahar"},
    {"player_id": 11, "player_name": "Ravindra Jadeja"},
]


# ------------------------------------------------------------------
# _calc_innings_totals
# ------------------------------------------------------------------
class TestCalcInningsTotals:
    def test_total_runs_includes_extras(self):
        totals = InningsSummaryService._calc_innings_totals(BALL_EVENTS)
        # bat runs: 0+4+1+6+0+2+0+4+1+0+0+6+1 = 25
        # extras: 1 (wide) + 1 (no_ball) = 2
        assert totals["total_runs"] == 27

    def test_wickets(self):
        totals = InningsSummaryService._calc_innings_totals(BALL_EVENTS)
        assert totals["wickets"] == 1

    def test_legal_balls_excludes_wides(self):
        totals = InningsSummaryService._calc_innings_totals(BALL_EVENTS)
        # 14 events - 1 wide - 1 no_ball = 12 legal balls
        assert totals["legal_balls"] == 12

    def test_wides_count(self):
        totals = InningsSummaryService._calc_innings_totals(BALL_EVENTS)
        assert totals["wides"] == 1

    def test_no_balls_count(self):
        totals = InningsSummaryService._calc_innings_totals(BALL_EVENTS)
        assert totals["no_balls"] == 1

    def test_empty_innings(self):
        totals = InningsSummaryService._calc_innings_totals([])
        assert totals == {"total_runs": 0, "wickets": 0, "legal_balls": 0,
                          "extras": 0, "wides": 0, "no_balls": 0}


# ------------------------------------------------------------------
# _calc_batter_summaries
# ------------------------------------------------------------------
class TestCalcBatterSummaries:
    def test_returns_one_row_per_batter(self):
        result = InningsSummaryService._calc_batter_summaries(BALL_EVENTS, BATTER_ROWS)
        assert len(result) == 3

    def test_rohit_runs(self):
        result = InningsSummaryService._calc_batter_summaries(BALL_EVENTS, BATTER_ROWS)
        rohit = next(b for b in result if b.name == "Rohit Sharma")
        # balls: 0+4+1+2+1+6 = 14 runs; wide doesn't count as ball faced
        assert rohit.runs == 14

    def test_fours_and_sixes_counted(self):
        result = InningsSummaryService._calc_batter_summaries(BALL_EVENTS, BATTER_ROWS)
        rohit = next(b for b in result if b.name == "Rohit Sharma")
        assert rohit.fours == 1
        assert rohit.sixes == 1

    def test_is_out_flag(self):
        result = InningsSummaryService._calc_batter_summaries(BALL_EVENTS, BATTER_ROWS)
        kishan = next(b for b in result if b.name == "Ishan Kishan")
        assert kishan.is_out is True

    def test_sorted_by_runs_desc(self):
        result = InningsSummaryService._calc_batter_summaries(BALL_EVENTS, BATTER_ROWS)
        assert result[0].runs >= result[1].runs >= result[2].runs


# ------------------------------------------------------------------
# _calc_bowler_summaries
# ------------------------------------------------------------------
class TestCalcBowlerSummaries:
    def test_returns_one_row_per_bowler(self):
        result = InningsSummaryService._calc_bowler_summaries(BALL_EVENTS, BOWLER_ROWS)
        assert len(result) == 2

    def test_jadeja_wickets(self):
        result = InningsSummaryService._calc_bowler_summaries(BALL_EVENTS, BOWLER_ROWS)
        jadeja = next(b for b in result if b.name == "Ravindra Jadeja")
        assert jadeja.wickets == 1

    def test_no_ball_not_counted_as_legal(self):
        result = InningsSummaryService._calc_bowler_summaries(BALL_EVENTS, BOWLER_ROWS)
        jadeja = next(b for b in result if b.name == "Ravindra Jadeja")
        # Jadeja has 6 balls total, 1 is a no-ball → 5 legal balls
        assert jadeja.legal_balls == 5

    def test_sorted_by_wickets_desc(self):
        result = InningsSummaryService._calc_bowler_summaries(BALL_EVENTS, BOWLER_ROWS)
        assert result[0].wickets >= result[1].wickets


# ------------------------------------------------------------------
# _calc_overs / _overs_to_decimal
# ------------------------------------------------------------------
class TestInningsCalcOvers:
    def test_full_over(self):
        assert InningsSummaryService._calc_overs(12) == "2.0"

    def test_partial_over(self):
        assert InningsSummaryService._calc_overs(13) == "2.1"

    def test_decimal_conversion(self):
        assert InningsSummaryService._overs_to_decimal(6) == pytest.approx(1.0)
        assert InningsSummaryService._overs_to_decimal(9) == pytest.approx(1.5)


# ------------------------------------------------------------------
# _calc_run_rate
# ------------------------------------------------------------------
class TestInningsRunRate:
    def test_normal(self):
        assert InningsSummaryService._calc_run_rate(27, 12) == pytest.approx(13.5, abs=0.01)

    def test_zero_balls(self):
        assert InningsSummaryService._calc_run_rate(0, 0) == 0.0


# ------------------------------------------------------------------
# _calc_top_batter / _calc_top_bowler
# ------------------------------------------------------------------
class TestInningsTopPerformers:
    def test_top_batter_prefers_not_out(self):
        batters = InningsSummaryService._calc_batter_summaries(BALL_EVENTS, BATTER_ROWS)
        top = InningsSummaryService._calc_top_batter(batters)
        assert top is not None
        assert top.name == "Rohit Sharma"

    def test_top_bowler_most_wickets(self):
        bowlers = InningsSummaryService._calc_bowler_summaries(BALL_EVENTS, BOWLER_ROWS)
        top = InningsSummaryService._calc_top_bowler(bowlers)
        assert top is not None
        assert top.wickets == 1

    def test_top_batter_none_for_empty(self):
        assert InningsSummaryService._calc_top_batter([]) is None

    def test_top_bowler_none_for_empty(self):
        assert InningsSummaryService._calc_top_bowler([]) is None