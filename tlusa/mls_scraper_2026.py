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

    # ── Fetch team lookup first ───────────────────────────────
    print("Fetching team names...")
    teams_df = asa.get_teams(leagues="mls")[["team_id", "team_name"]].drop_duplicates("team_id")
    team_map = dict(zip(teams_df["team_id"], teams_df["team_name"]))
    print(f"  {len(team_map)} teams")

    # ── Player roster ─────────────────────────────────────────
    print("\n[1/5] Fetching player roster...")
    players = asa.get_players(leagues="mls")
    players = players[["player_id", "player_name"]].drop_duplicates("player_id")
    print(f"  {len(players)} players")

    # ── xGoals ───────────────────────────────────────────────
    print("\n[2/5] Fetching xGoals...")
    xg = asa.get_player_xgoals(leagues="mls", season_name=SEASON, stage_name="Regular Season")
    print(f"  {len(xg)} rows")

    # ── xPass ────────────────────────────────────────────────
    print("\n[3/5] Fetching xPass...")
    xp = asa.get_player_xpass(leagues="mls", season_name=SEASON, stage_name="Regular Season")
    print(f"  {len(xp)} rows")

    # ── Goals Added ──────────────────────────────────────────
    print("\n[4/5] Fetching Goals Added...")
    ga = asa.get_player_goals_added(leagues="mls", season_name=SEASON, stage_name="Regular Season", above_replacement=True)
    print(f"  {len(ga)} rows")

    # ── GK xGoals ────────────────────────────────────────────
    print("\n[5/5] Fetching goalkeeper data...")
    gk = asa.get_goalkeeper_xgoals(leagues="mls", season_name=SEASON, stage_name="Regular Season")
    print(f"  {len(gk)} rows")

    # ══════════════════ OUTFIELD ══════════════════
    print("\nBuilding outfield dataset...")

    df = xg.merge(players, on="player_id", how="left")

    xp_cols = [c for c in xp.columns if c not in df.columns or c == "player_id"]
    df = df.merge(xp[xp_cols], on="player_id", how="left")

    if "goals_added_above_replacement" in ga.columns:
        ga_slim = ga[["player_id", "goals_added_above_replacement"]].drop_duplicates("player_id")
        df = df.merge(ga_slim, on="player_id", how="left")

    df = df[safe(df, "minutes_played") >= MIN_MIN].copy()

    # Resolve team name
    df["Squad"] = df["team_id"].map(team_map).fillna(df["team_id"])

    # Rename
    df = df.rename(columns={
        "player_name":                  "Player",
        "general_position":             "Pos",
        "minutes_played":               "Min",
        "goals":                        "Gls",
        "xgoals":                       "xG",
        "key_passes":                   "KP",
        "assists":                      "Ast",
        "xassists":                     "xAG",
        "shots_on_target":              "SoT",
        "goals_added_above_replacement":"Goals_Added",
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

    OUT_COLS = ["Player", "Squad", "Pos", "Min", "90s",
                "Attacking_Efficiency", "Defensive_Efficiency",
                "Goals_p90", "xG_p90", "Assists_p90", "xAG_p90",
                "SoT_p90", "KeyPasses_p90", "Goals_Added"]

    out = df[[c for c in OUT_COLS if c in df.columns]]
    out = out.sort_values("Attacking_Efficiency", ascending=False).reset_index(drop=True)
    out_path = OUT_DIR / "mls_outfield_efficiency.csv"
    out.to_csv(out_path, index=False)
    print(f"  Saved {len(out)} outfield players → {out_path.name}")

    # ══════════════════ GOALKEEPERS ══════════════════
    print("\nBuilding goalkeeper dataset...")

    gk = gk.merge(players, on="player_id", how="left")
    gk = gk[safe(gk, "minutes_played") >= MIN_MIN].copy()

    # Resolve team name
    gk["Squad"] = gk["team_id"].map(team_map).fillna(gk["team_id"])

    # Rename
    gk = gk.rename(columns={
        "player_name":          "Player",
        "minutes_played":       "Min",
        "goals_against":        "GA",
        "shots_on_target_against": "SoTA",
        "saves":                "Saves",
        "save_percentage":      "Save%",
        "goals_against_xgoals": "GA_minus_xGA",
    })

    gk_mins  = safe(gk, "Min")
    gk_nines = (gk_mins / 90).clip(lower=0.01)
    gk["90s"]    = (gk_mins / 90).round(2)
    gk["GA_p90"] = (safe(gk, "GA") / gk_nines).round(3)
    gk["Pos"]    = "GK"

    gk["GK_Efficiency"] = (
        nm(safe(gk, "Save%"))              * 0.35 +
        nm(safe(gk, "GA_minus_xGA"), True) * 0.35 +
        nm(safe(gk, "GA_p90"), True)       * 0.30
    ).round(2)

    GK_COLS = ["Player", "Squad", "Pos", "Min", "90s",
               "GK_Efficiency", "GA_p90", "Save%", "GA_minus_xGA"]

    gk_out = gk[[c for c in GK_COLS if c in gk.columns]]
    gk_out = gk_out.sort_values("GK_Efficiency", ascending=False).reset_index(drop=True)
    gk_path = OUT_DIR / "mls_gk_efficiency.csv"
    gk_out.to_csv(gk_path, index=False)
    print(f"  Saved {len(gk_out)} goalkeepers → {gk_path.name}")

    # ── Preview ──────────────────────────────────────────────
    print(f"\n{div}")
    print("TOP ATTACKERS")
    pc = [c for c in ["Player", "Squad", "Pos", "Attacking_Efficiency", "Goals_p90", "xG_p90"] if c in out.columns]
    print(out[pc].head(10).to_string(index=False))

    print("\nTOP GOALKEEPERS")
    gc = [c for c in ["Player", "Squad", "GK_Efficiency", "GA_p90", "Save%"] if c in gk_out.columns]
    print(gk_out[gc].head(10).to_string(index=False))

    print(f"\n{div}")
    print("  Done! Run inject_data.py then hard-refresh your browser.")
    print(f"{div}\n")


if __name__ == "__main__":
    main()