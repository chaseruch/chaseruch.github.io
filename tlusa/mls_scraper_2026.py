"""
Touchline USA — MLS Player Data Scraper
========================================
Uses the American Soccer Analysis API (no scraping, no 403s).

Run:
  python3 mls_scraper_2026.py

Requirements:
  pip3 install itscalledsoccer pandas
"""

from pathlib import Path
import pandas as pd

try:
    from itscalledsoccer.client import AmericanSoccerAnalysis
except ImportError:
    print("ERROR: Run this first:  pip3 install itscalledsoccer pandas")
    raise

SEASON  = "2026"
MIN_MIN = 1
OUT_DIR = Path(__file__).parent

def safe(df, col):
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce").fillna(0)
    return pd.Series(0.0, index=df.index)

def minmax(s):
    lo, hi = s.min(), s.max()
    if hi == lo:
        return pd.Series(50.0, index=s.index)
    return ((s - lo) / (hi - lo) * 100).round(2)

def nm(s, invert=False):
    s = -s if invert else s
    lo, hi = s.min(), s.max()
    if hi == lo:
        return pd.Series(50.0, index=s.index)
    return ((s - lo) / (hi - lo) * 100)


def main():
    div = "=" * 54
    print(f"\n{div}")
    print(f"  Touchline USA — MLS Scraper  ({SEASON})")
    print(f"{div}\n")

    print("Connecting to American Soccer Analysis API...")
    asa = AmericanSoccerAnalysis()
    print("Connected.\n")

    # ── Team lookup ───────────────────────────────────────────
    print("Fetching team names...")
    teams_df = asa.get_teams(leagues="mls")[["team_id", "team_name"]].drop_duplicates("team_id")
    team_map = dict(zip(teams_df["team_id"], teams_df["team_name"]))
    print(f"  {len(team_map)} teams")

    # ── Player roster ─────────────────────────────────────────
    print("\n[1/7] Fetching player roster...")
    players = asa.get_players(leagues="mls")
    players = players[["player_id", "player_name"]].drop_duplicates("player_id")
    print(f"  {len(players)} players")

    # ── xGoals ───────────────────────────────────────────────
    print("\n[2/7] Fetching xGoals...")
    xg = asa.get_player_xgoals(leagues="mls", season_name=SEASON, stage_name="Regular Season")
    print(f"  {len(xg)} rows")

    # ── xPass ────────────────────────────────────────────────
    print("\n[3/7] Fetching xPass...")
    xp = asa.get_player_xpass(leagues="mls", season_name=SEASON, stage_name="Regular Season")
    print(f"  {len(xp)} rows")

    # ── Goals Added (above replacement, aggregated) ───────────
    print("\n[4/7] Fetching Goals Added (above replacement)...")
    ga = asa.get_player_goals_added(
        leagues="mls", season_name=SEASON,
        stage_name="Regular Season", above_replacement=True
    )
    print(f"  {len(ga)} rows")

    # ── Goals Added by action type ────────────────────────────
    print("\n[5/7] Fetching Goals Added by action type...")
    ga_types = asa.get_player_goals_added(
        leagues="mls", season_name=SEASON,
        stage_name="Regular Season", above_replacement=False
    )
    print(f"  {len(ga_types)} rows")

    # ── Salaries ──────────────────────────────────────────────
    print("\n[6/7] Fetching salary data...")
    try:
        salaries = asa.get_player_salaries(leagues="mls", season_name=SEASON)
        print(f"  {len(salaries)} rows")
    except Exception as e:
        print(f"  WARNING: Could not fetch salaries: {e}")
        salaries = pd.DataFrame()

    # ── GK xGoals ────────────────────────────────────────────
    print("\n[7/7] Fetching goalkeeper data...")
    gk = asa.get_goalkeeper_xgoals(leagues="mls", season_name=SEASON, stage_name="Regular Season")
    print(f"  {len(gk)} rows")

    # ── GK Goals Added ────────────────────────────────────────
    print("  Fetching GK Goals Added...")
    try:
        gk_ga = asa.get_goalkeeper_goals_added(
            leagues="mls", season_name=SEASON,
            stage_name="Regular Season", above_replacement=True
        )
        print(f"  {len(gk_ga)} rows")
    except Exception as e:
        print(f"  WARNING: Could not fetch GK goals added: {e}")
        gk_ga = pd.DataFrame()

    # ══════════════════ PROCESS GOALS ADDED BY ACTION TYPE ══════════════════
    # Pivot so each action_type becomes its own column per player
    ga_pivot = pd.DataFrame()
    if not ga_types.empty and "action_type" in ga_types.columns:
        try:
            # Each row is player_id + action_type + goals_added_raw + goals_added_above_avg
            value_col = "goals_added_above_avg" if "goals_added_above_avg" in ga_types.columns else "goals_added_raw"
            ga_pivot = ga_types.pivot_table(
                index="player_id", columns="action_type",
                values=value_col, aggfunc="sum"
            ).reset_index()
            ga_pivot.columns = [
                f"ga_{c.lower()}" if c != "player_id" else "player_id"
                for c in ga_pivot.columns
            ]
            print(f"\n  Goals Added action types: {[c for c in ga_pivot.columns if c != 'player_id']}")
        except Exception as e:
            print(f"  WARNING: Could not pivot goals added: {e}")

    # ══════════════════ OUTFIELD ══════════════════
    print("\nBuilding outfield dataset...")

    df = xg.merge(players, on="player_id", how="left")

    xp_cols = [c for c in xp.columns if c not in df.columns or c == "player_id"]
    df = df.merge(xp[xp_cols], on="player_id", how="left")

    # Merge aggregate goals added
    if "goals_added_above_replacement" in ga.columns:
        ga_slim = ga[["player_id", "goals_added_above_replacement"]].drop_duplicates("player_id")
        df = df.merge(ga_slim, on="player_id", how="left")

    # Merge goals added by action type
    if not ga_pivot.empty:
        df = df.merge(ga_pivot, on="player_id", how="left")

    # Merge salaries
    if not salaries.empty:
        sal_cols = ["player_id"]
        for col in ["base_salary", "guaranteed_compensation"]:
            if col in salaries.columns:
                sal_cols.append(col)
        if len(sal_cols) > 1:
            sal_slim = salaries[sal_cols].drop_duplicates("player_id")
            df = df.merge(sal_slim, on="player_id", how="left")

    df = df[safe(df, "minutes_played") >= MIN_MIN].copy()

    # Resolve team name
    df["Squad"] = df["team_id"].map(team_map).fillna(df["team_id"])

    # Rename core columns
    df = df.rename(columns={
        "player_name":                   "Player",
        "general_position":              "Pos",
        "minutes_played":                "Min",
        "goals":                         "Gls",
        "xgoals":                        "xG",
        "key_passes":                    "KP",
        "primary_assists":               "Ast",
        "xassists":                      "xAG",
        "shots_on_target":               "SoT",
        "goals_added_above_replacement": "Goals_Added",
        "base_salary":                   "Base_Salary",
        "guaranteed_compensation":       "Guaranteed_Comp",
    })

    mins  = safe(df, "Min")
    nines = (mins / 90).clip(lower=0.01)
    df["90s"] = (mins / 90).round(2)

    gls  = safe(df, "Gls")  / nines
    xg_  = safe(df, "xG")   / nines
    sot  = safe(df, "SoT")  / nines
    ast  = safe(df, "Ast")  / nines
    xag  = safe(df, "xAG")  / nines
    kp   = safe(df, "KP")   / nines
    ga_s = safe(df, "Goals_Added")

    atk_raw = gls*0.20 + xg_*0.18 + sot*0.10 + ast*0.12 + xag*0.10 + kp*0.08 + ga_s*0.22
    def_raw = ga_s * 0.5 + safe(df, "pass_completion_percentage") * 0.5

    df["Attacking_Efficiency"] = minmax(atk_raw)
    df["Defensive_Efficiency"] = minmax(def_raw)
    df["Goals_p90"]     = gls.round(3)
    df["xG_p90"]        = xg_.round(3)
    df["Assists_p90"]   = ast.round(3)
    df["xAG_p90"]       = xag.round(3)
    df["SoT_p90"]       = sot.round(3)
    df["KeyPasses_p90"] = kp.round(3)
    df["Goals_Added"]   = ga_s.round(3)

    # Value metric: Goals Added per $1M guaranteed comp
    if "Guaranteed_Comp" in df.columns:
        comp_m = safe(df, "Guaranteed_Comp") / 1_000_000
        df["Value_per_M"] = (ga_s / comp_m.replace(0, float("nan"))).fillna(0).round(3)

    OUT_COLS = [
        "Player", "Squad", "Pos", "Min", "90s",
        # Raw totals (Simple view)
        "Gls", "xG", "Ast", "xAG", "SoT", "KP",
        # Per-90 (Advanced view)
        "Attacking_Efficiency", "Defensive_Efficiency",
        "Goals_p90", "xG_p90", "Assists_p90", "xAG_p90",
        "SoT_p90", "KeyPasses_p90", "Goals_Added",
        "Base_Salary", "Guaranteed_Comp", "Value_per_M",
        # Goals Added by action type
        "ga_shooting", "ga_passing", "ga_dribbling",
        "ga_receiving", "ga_fouling", "ga_interrupting",
    ]

    out = df[[c for c in OUT_COLS if c in df.columns]]
    out = out.sort_values("Attacking_Efficiency", ascending=False).reset_index(drop=True)
    out_path = OUT_DIR / "mls_outfield_efficiency.csv"
    out.to_csv(out_path, index=False)
    print(f"  Saved {len(out)} outfield players → {out_path.name}")

    # ══════════════════ GOALKEEPERS ══════════════════
    print("\nBuilding goalkeeper dataset...")

    gk = gk.merge(players, on="player_id", how="left")
    gk = gk[safe(gk, "minutes_played") >= MIN_MIN].copy()

    # Merge GK goals added
    if not gk_ga.empty and "goals_added_above_replacement" in gk_ga.columns:
        gk_ga_slim = gk_ga[["player_id", "goals_added_above_replacement"]].drop_duplicates("player_id")
        gk = gk.merge(gk_ga_slim, on="player_id", how="left")

    # Merge salaries for GKs too
    if not salaries.empty and "Base_Salary" not in gk.columns:
        sal_cols = ["player_id"] + [c for c in ["base_salary", "guaranteed_compensation"] if c in salaries.columns]
        if len(sal_cols) > 1:
            gk = gk.merge(salaries[sal_cols].drop_duplicates("player_id"), on="player_id", how="left")

    # Resolve team name
    gk["Squad"] = gk["team_id"].map(team_map).fillna(gk["team_id"])

    # Rename
    gk = gk.rename(columns={
        "player_name":                   "Player",
        "minutes_played":                "Min",
        "goals_conceded":                "GA",
        "shots_faced":                   "SoTA",
        "saves":                         "Saves",
        "goals_minus_xgoals_gk":         "GA_minus_xGA",
        "goals_added_above_replacement": "GK_Goals_Added",
        "base_salary":                   "Base_Salary",
        "guaranteed_compensation":       "Guaranteed_Comp",
    })

    gk_mins  = safe(gk, "Min")
    gk_nines = (gk_mins / 90).clip(lower=0.01)
    gk["90s"]    = (gk_mins / 90).round(2)
    gk["GA_p90"] = (safe(gk, "GA") / gk_nines).round(3)
    gk["Pos"]    = "GK"
    # Calculate Save% from saves and shots faced
    gk["Save%"]  = (safe(gk, "Saves") / safe(gk, "SoTA").clip(lower=0.01) * 100).round(1)



    gk["GK_Efficiency"] = (
        nm(safe(gk, "Save%"))              * 0.30 +
        nm(safe(gk, "GA_minus_xGA"), True) * 0.30 +
        nm(safe(gk, "GA_p90"), True)       * 0.20 +
        nm(safe(gk, "GK_Goals_Added"))     * 0.20
    ).round(2)

    GK_COLS = [
        "Player", "Squad", "Pos", "Min", "90s",
        # Raw totals (Simple view)
        "GA", "Saves", "SoTA",
        # Advanced
        "GK_Efficiency", "GA_p90", "Save%", "GA_minus_xGA",
        "GK_Goals_Added", "Base_Salary", "Guaranteed_Comp",
    ]

    gk_out = gk[[c for c in GK_COLS if c in gk.columns]]
    gk_out = gk_out.sort_values("GK_Efficiency", ascending=False).reset_index(drop=True)
    gk_path = OUT_DIR / "mls_gk_efficiency.csv"
    gk_out.to_csv(gk_path, index=False)
    print(f"  Saved {len(gk_out)} goalkeepers → {gk_path.name}")

    # ── Team data ────────────────────────────────────────────
    print("\n[8/8] Fetching team data...")
    try:
        team_xg = asa.get_team_xgoals(leagues="mls", season_name=SEASON, stage_name="Regular Season")
        print(f"  team_xgoals columns: {list(team_xg.columns)}")
        print(f"  {len(team_xg)} teams")

        game_xg = asa.get_game_xgoals(leagues="mls", season_name=SEASON, stage_name="Regular Season")
        print(f"  game_xgoals columns: {list(game_xg.columns)}")
        print(f"  {len(game_xg)} games")
    except Exception as e:
        print(f"  WARNING: {e}")

    # ── Preview ──────────────────────────────────────────────
    print(f"\n{div}")
    print("TOP ATTACKERS")
    pc = [c for c in ["Player", "Squad", "Pos", "Attacking_Efficiency", "Goals_p90", "xG_p90", "Goals_Added", "Guaranteed_Comp"] if c in out.columns]
    print(out[pc].head(10).to_string(index=False))

    print("\nTOP GOALKEEPERS")
    gc = [c for c in ["Player", "Squad", "GK_Efficiency", "GA_p90", "Save%", "GK_Goals_Added"] if c in gk_out.columns]
    print(gk_out[gc].head(10).to_string(index=False))

    print(f"\n{div}")
    print("  Done! Run inject_data.py then hard-refresh your browser.")
    print(f"{div}\n")


if __name__ == "__main__":
    main()