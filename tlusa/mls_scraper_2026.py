"""
Touchline USA — MLS Player Data Scraper
========================================
Uses the American Soccer Analysis API (no scraping, no 403s).

Run:
  python mls_scraper_2026.py

Requirements:
  pip install itscalledsoccer pandas
"""

from pathlib import Path
import pandas as pd

try:
    from itscalledsoccer.client import AmericanSoccerAnalysis
except ImportError:
    print("ERROR: Run this first:  pip install itscalledsoccer pandas")
    raise

# ─────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────
SEASON  = "2026"
MIN_MIN = 1        # minimum minutes played — raise to 90+ mid-season
OUT_DIR = Path(__file__).parent

# ─────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────
def safe(df, col):
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce").fillna(0)
    return pd.Series(0.0, index=df.index)

def minmax(s):
    lo, hi = s.min(), s.max()
    if hi == lo:
        return pd.Series(50.0, index=s.index)
    return ((s - lo) / (hi - lo) * 100).round(2)

def p90(s, mins):
    nines = (mins / 90).replace(0, float("nan"))
    return s.div(nines).fillna(0)


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────
def main():
    div = "=" * 54
    print(f"\n{div}")
    print(f"  Touchline USA — MLS Scraper  ({SEASON})")
    print(f"{div}\n")

    print("Connecting to American Soccer Analysis API...")
    asa = AmericanSoccerAnalysis()
    print("Connected.\n")

    # ── 1. Player info (names, positions, teams) ──────────────
    print("[1/5] Fetching player roster info...")
    players = asa.get_players(leagues="mls")
    players = players[["player_id", "player_name", "birth_date", "nationality"]].drop_duplicates("player_id")
    print(f"  {len(players)} players found")

    # ── 2. xGoals (shooting / attacking) ─────────────────────
    print("\n[2/5] Fetching xGoals data...")
    xg = asa.get_player_xgoals(
        leagues="mls",
        season_name=SEASON,
        stage_name="Regular Season"
    )
    print(f"  {len(xg)} rows")

    # ── 3. xPass (passing) ────────────────────────────────────
    print("\n[3/5] Fetching xPass data...")
    xp = asa.get_player_xpass(
        leagues="mls",
        season_name=SEASON,
        stage_name="Regular Season"
    )
    print(f"  {len(xp)} rows")

    # ── 4. Goals Added (g+) ───────────────────────────────────
    print("\n[4/5] Fetching Goals Added (g+) data...")
    ga = asa.get_player_goals_added(
        leagues="mls",
        season_name=SEASON,
        stage_name="Regular Season",
        above_replacement=True
    )
    print(f"  {len(ga)} rows")

    # ── 5. Goalkeeper xGoals ──────────────────────────────────
    print("\n[5/5] Fetching goalkeeper data...")
    gk = asa.get_goalkeeper_xgoals(
        leagues="mls",
        season_name=SEASON,
        stage_name="Regular Season"
    )
    print(f"  {len(gk)} rows")

    # ── Merge outfield ────────────────────────────────────────
    print("\nBuilding outfield dataset...")

    # Join player names onto xg
    df = xg.merge(players, on="player_id", how="left")

    # Join xpass
    xp_cols = [c for c in xp.columns if c not in df.columns or c == "player_id"]
    df = df.merge(xp[xp_cols], on="player_id", how="left")

    # Join goals added
    if "goals_added_above_replacement" in ga.columns:
        ga_slim = ga[["player_id", "goals_added_above_replacement"]].drop_duplicates("player_id")
        df = df.merge(ga_slim, on="player_id", how="left")

    # Filter by minutes
    df = df[safe(df, "minutes_played") >= MIN_MIN].copy()

    # Rename key columns for dashboard compatibility
    rename = {
        "player_name":                  "Player",
        "team_id":                       "Squad",
        "general_position":              "Pos",
        "minutes_played":                "Min",
        "shots":                         "Shots",
        "shots_on_target":               "SoT",
        "goals":                         "Gls",
        "xgoals":                        "xG",
        "key_passes":                    "KP",
        "assists":                       "Ast",
        "xassists":                      "xAG",
        "goals_added_above_replacement": "Goals_Added",
        "pass_completion_percentage":    "Pass_Pct",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    # Add 90s column
    mins = safe(df, "Min")
    df["90s"] = (mins / 90).round(2)
    nines = df["90s"].clip(lower=0.01)

    # Per-90 attacking stats
    gls  = safe(df, "Gls")  / nines
    xg_  = safe(df, "xG")   / nines
    sot  = safe(df, "SoT")  / nines
    ast  = safe(df, "Ast")  / nines
    xag  = safe(df, "xAG")  / nines
    kp   = safe(df, "KP")   / nines
    ga_s = safe(df, "Goals_Added")

    atk_raw = (gls*0.20 + xg_*0.18 + sot*0.10 + ast*0.12 +
               xag*0.10 + kp*0.08 + ga_s*0.22)

    # Per-90 defensive proxy from goals added
    def_raw = ga_s * 0.5 + safe(df, "pass_completion_percentage") * 0.5

    df["Attacking_Efficiency"] = minmax(atk_raw)
    df["Defensive_Efficiency"] = minmax(def_raw)
    df["Goals_p90"]            = (gls).round(3)
    df["xG_p90"]               = (xg_).round(3)
    df["Assists_p90"]          = (ast).round(3)
    df["xAG_p90"]              = (xag).round(3)
    df["SoT_p90"]              = (sot).round(3)
    df["KeyPasses_p90"]        = (kp).round(3)
    df["Goals_Added"]          = ga_s.round(3)

    # Replace team_id with team name where possible
    try:
        teams = asa.get_teams(leagues="mls")[["team_id", "team_name"]].drop_duplicates("team_id")
        df = df.merge(teams, on="team_id", how="left")
        df["Squad"] = df["team_name"].fillna(df.get("Squad", ""))
    except Exception:
        pass

    OUTFIELD_COLS = [
        "Player", "Squad", "Pos", "Min", "90s",
        "Attacking_Efficiency", "Defensive_Efficiency",
        "Goals_p90", "xG_p90", "Assists_p90", "xAG_p90",
        "SoT_p90", "KeyPasses_p90", "Goals_Added",
    ]
    available = [c for c in OUTFIELD_COLS if c in df.columns]
    out = df[available].sort_values("Attacking_Efficiency", ascending=False).reset_index(drop=True)
    out_path = OUT_DIR / "mls_outfield_efficiency.csv"
    out.to_csv(out_path, index=False)
    print(f"  Saved {len(out)} outfield players → {out_path.name}")

    # ── Goalkeepers ───────────────────────────────────────────
    print("\nBuilding goalkeeper dataset...")
    gk = gk.merge(players, on="player_id", how="left")
    gk = gk[safe(gk, "minutes_played") >= MIN_MIN].copy()

    gk_rename = {
        "player_name":       "Player",
        "team_id":           "Squad",
        "minutes_played":    "Min",
        "goals_against":     "GA",
        "shots_on_target_against": "SoTA",
        "saves":             "Saves",
        "save_percentage":   "Save%",
        "goals_against_xgoals": "GA_minus_xGA",
    }
    gk = gk.rename(columns={k: v for k, v in gk_rename.items() if k in gk.columns})

    gk["90s"]       = (safe(gk, "Min") / 90).round(2)
    gk_nines        = gk["90s"].clip(lower=0.01)
    gk["GA_p90"]    = (safe(gk, "GA") / gk_nines).round(3)

    def nm(s, invert=False):
        s = -s if invert else s
        lo, hi = s.min(), s.max()
        if hi == lo:
            return pd.Series(50.0, index=s.index)
        return ((s - lo) / (hi - lo) * 100)

    gk["GK_Efficiency"] = (
        nm(safe(gk, "Save%"))           * 0.35 +
        nm(safe(gk, "GA_minus_xGA"), True) * 0.35 +
        nm(safe(gk, "GA_p90"), True)    * 0.30
    ).round(2)

    try:
        teams = asa.get_teams(leagues="mls")[["team_id", "team_name"]].drop_duplicates("team_id")
        gk = gk.merge(teams, on="team_id", how="left")
        gk["Squad"] = gk["team_name"].fillna(gk.get("Squad", ""))
    except Exception:
        pass

    GK_COLS = ["Player", "Squad", "Min", "90s", "GK_Efficiency",
               "GA_p90", "Save%", "GA_minus_xGA"]
    gk_available = [c for c in GK_COLS if c in gk.columns]
    gk_out = gk[gk_available].sort_values("GK_Efficiency", ascending=False).reset_index(drop=True)
    gk_path = OUT_DIR / "mls_gk_efficiency.csv"
    gk_out.to_csv(gk_path, index=False)
    print(f"  Saved {len(gk_out)} goalkeepers → {gk_path.name}")

    # ── Preview ───────────────────────────────────────────────
    print(f"\n{div}")
    print("TOP ATTACKERS")
    prev_cols = [c for c in ["Player", "Squad", "Pos", "Attacking_Efficiency", "Goals_p90", "xG_p90", "Goals_Added"] if c in out.columns]
    print(out[prev_cols].head(10).to_string(index=False))

    print("\nTOP GOALKEEPERS")
    gk_prev = [c for c in ["Player", "Squad", "GK_Efficiency", "GA_p90", "Save%"] if c in gk_out.columns]
    print(gk_out[gk_prev].head(10).to_string(index=False))

    print(f"\n{div}")
    print("  Done. Import the CSVs into Touchline USA.")
    print(f"{div}\n")


if __name__ == "__main__":
    main()