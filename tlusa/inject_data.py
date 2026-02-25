"""
Touchline USA — Data Injector
==============================
Reads mls_outfield_efficiency.csv and mls_gk_efficiency.csv
and injects player data into index.html.

Run after scraping:
  python3 inject_data.py
"""

import json
import re
from datetime import date
from pathlib import Path
import pandas as pd

DIR      = Path(__file__).parent
HTML     = DIR / "index.html"
OUT_CSV  = DIR / "mls_outfield_efficiency.csv"
GK_CSV   = DIR / "mls_gk_efficiency.csv"
TEAM_CSV  = DIR / "mls_team_stats.csv"
TRAJ_CSV  = DIR / "mls_team_trajectory.csv"
XPASS_CSV = DIR / "mls_team_xpass.csv"
TGA_CSV   = DIR / "mls_team_goals_added.csv"


def fmt_salary(v):
    """Format salary as $1.23M or $450K"""
    if not v or v == 0:
        return None
    v = float(v)
    if v >= 1_000_000:
        return f"${v/1_000_000:.2f}M"
    elif v >= 1_000:
        return f"${v/1_000:.0f}K"
    return f"${v:.0f}"


def csv_to_players(path, is_gk=False):
    if not path.exists():
        print(f"  WARNING: {path.name} not found")
        return []
    df = pd.read_csv(path)
    players = []
    for _, row in df.iterrows():
        def g(col, default=0):
            v = row.get(col, default)
            try:
                if pd.isna(v): return default
            except: pass
            return v

        p = {
            "id":       f"{'gk' if is_gk else 'of'}_{len(players)}",
            "name":     str(g("Player", "")),
            "squad":    str(g("Squad", "")),
            "pos":      str(g("Pos", "GK" if is_gk else "")),
            "age":      str(g("Age", "")),
            "nation":   str(g("Nation", "")),
            "nineties": float(g("90s", 0) or 0),
            "isGK":     is_gk,
            # Salary
            "base_salary":      fmt_salary(g("Base_Salary", 0)),
            "guaranteed_comp":  fmt_salary(g("Guaranteed_Comp", 0)),
            "base_salary_raw":  float(g("Base_Salary", 0) or 0),
            "guaranteed_comp_raw": float(g("Guaranteed_Comp", 0) or 0),
        }

        if is_gk:
            p["GK_Efficiency"]  = float(g("GK_Efficiency", 0) or 0)
            p["GA_p90"]         = float(g("GA_p90", 0) or 0)
            p["Save_pct"]       = float(g("Save%", 0) or 0)
            p["Save%"]          = float(g("Save%", 0) or 0)
            p["GA_minus_xGA"]   = float(g("GA_minus_xGA", 0) or 0)
            p["GK_Goals_Added"] = float(g("GK_Goals_Added", 0) or 0)
            # Raw totals for Simple view
            p["GA_total"]       = float(g("GA", 0) or 0)
            p["Saves_total"]    = float(g("Saves", 0) or 0)
            p["SoTA_total"]     = float(g("SoTA", 0) or 0)
        else:
            gls = float(g("Goals_p90", 0) or 0)
            xg  = float(g("xG_p90", 0) or 0)
            ast = float(g("Assists_p90", 0) or 0)
            xag = float(g("xAG_p90", 0) or 0)
            sot = float(g("SoT_p90", 0) or 0)
            kp  = float(g("KeyPasses_p90", 0) or 0)
            p["Attacking_Efficiency"] = float(g("Attacking_Efficiency", 0) or 0)
            p["Defensive_Efficiency"] = float(g("Defensive_Efficiency", 0) or 0)
            p["Goals_p90"]     = gls
            p["xG_p90"]        = xg
            p["Assists_p90"]   = ast
            p["xAG_p90"]       = xag
            p["SoT_p90"]       = sot
            p["KeyPasses_p90"] = kp
            p["Goals_Added"]   = float(g("Goals_Added", 0) or 0)
            p["Value_per_M"]   = float(g("Value_per_M", 0) or 0)
            p["Tkl_Won_p90"]   = 0.0
            p["Interceptions_p90"] = 0.0
            # Raw totals for Simple view
            p["Gls_total"] = float(g("Gls", 0) or 0)
            p["xG_total"]  = float(g("xG", 0) or 0)
            p["Ast_total"] = float(g("Ast", 0) or 0)
            p["xAG_total"] = float(g("xAG", 0) or 0)
            p["SoT_total"] = float(g("SoT", 0) or 0)
            p["KP_total"]  = float(g("KP", 0) or 0)
            p["Min_total"] = float(g("Min", 0) or 0)
            # Goals Added by action type
            p["ga_shooting"]     = float(g("ga_shooting", 0) or 0)
            p["ga_passing"]      = float(g("ga_passing", 0) or 0)
            p["ga_dribbling"]    = float(g("ga_dribbling", 0) or 0)
            p["ga_receiving"]    = float(g("ga_receiving", 0) or 0)
            p["ga_fouling"]      = float(g("ga_fouling", 0) or 0)
            p["ga_interrupting"] = float(g("ga_interrupting", 0) or 0)
            # aliases
            p["Gls"] = gls
            p["xG"]  = xg
            p["Ast"] = ast
            p["xAG"] = xag
            p["SoT"] = sot
            p["KP"]  = kp
        players.append(p)
    return players


def main():
    print("\n" + "="*50)
    print("  Touchline USA — Data Injector")
    print("="*50 + "\n")

    if not HTML.exists():
        print(f"ERROR: {HTML} not found")
        return

    print("Reading CSVs...")
    outfield = csv_to_players(OUT_CSV, is_gk=False)
    gks      = csv_to_players(GK_CSV,  is_gk=True)
    all_players = outfield + gks
    print(f"  {len(outfield)} outfield  |  {len(gks)} GKs  |  {len(all_players)} total")

    # Load team data
    teams_data = []
    if TEAM_CSV.exists():
        tdf = pd.read_csv(TEAM_CSV)
        for _, row in tdf.iterrows():
            def tg(col, default=0):
                v = row.get(col, default)
                try:
                    if pd.isna(v): return default
                except: pass
                return v
            teams_data.append({
                "team_id":         str(tg("team_id","")),
                "name":            str(tg("Squad","")),
                "gp":              int(tg("GP",0)),
                "gf":              int(tg("GF",0)),
                "ga":              int(tg("GA",0)),
                "gd":              int(tg("GD",0)),
                "xgf":             round(float(tg("xGF",0)),2),
                "xga":             round(float(tg("xGA",0)),2),
                "xgd":             round(float(tg("xGD",0)),2),
                "gd_minus_xgd":    round(float(tg("GD_minus_xGD",0)),2),
                "sf":              int(tg("SF",0)),
                "sa":              int(tg("SA",0)),
                "pts":             int(tg("Pts",0)),
                "xpts":            round(float(tg("xPts",0)),2),
                "efficiency":      round(float(tg("Team_Efficiency",0)),2),
            })
        print(f"  {len(teams_data)} teams loaded")

    # Load trajectory data — group by team as dict of arrays
    traj_data = {}
    if TRAJ_CSV.exists():
        trdf = pd.read_csv(TRAJ_CSV)
        for team, grp in trdf.groupby("team"):
            grp = grp.sort_values("matchday")
            traj_data[team] = {
                "dates":      grp["date"].tolist(),
                "matchdays":  grp["matchday"].tolist(),
                "cum_goals":  grp["cum_goals"].tolist(),
                "cum_xgoals": grp["cum_xgoals"].round(2).tolist(),
                "cum_xpts":   grp["cum_xpoints"].round(2).tolist(),
            }
        print(f"  {len(traj_data)} teams with trajectory data")

    # Load team xPass data
    xpass_data = {}
    if XPASS_CSV.exists():
        xpdf = pd.read_csv(XPASS_CSV)
        for _, row in xpdf.iterrows():
            def xg(col, default=0):
                v = row.get(col, default)
                try:
                    if pd.isna(v): return default
                except: pass
                return v
            xpass_data[str(xg("Squad",""))] = {
                "att_passes":       int(xg("attempted_passes_for",0)),
                "pass_comp_for":    round(float(xg("pass_completion_percentage_for",0)),1),
                "xpass_comp_for":   round(float(xg("xpass_completion_percentage_for",0)),1),
                "pcoe_p100_for":    round(float(xg("passes_completed_over_expected_p100_for",0)),2),
                "avg_vert_for":     round(float(xg("avg_vertical_distance_for",0)),1),
                "pass_comp_ag":     round(float(xg("pass_completion_percentage_against",0)),1),
                "xpass_comp_ag":    round(float(xg("xpass_completion_percentage_against",0)),1),
                "pcoe_p100_ag":     round(float(xg("passes_completed_over_expected_p100_against",0)),2),
                "avg_vert_ag":      round(float(xg("avg_vertical_distance_against",0)),1),
                "pcoe_diff":        round(float(xg("passes_completed_over_expected_difference",0)),2),
            }
        print(f"  {len(xpass_data)} teams with xPass data")

    # Load team goals added breakdown
    tga_data = {}
    if TGA_CSV.exists():
        tgadf = pd.read_csv(TGA_CSV)
        actions = ["dribbling","fouling","interrupting","passing","receiving","shooting"]
        for _, row in tgadf.iterrows():
            def tgg(col, default=0):
                v = row.get(col, default)
                try:
                    if pd.isna(v): return default
                except: pass
                return v
            squad = str(tgg("Squad",""))
            tga_data[squad] = {}
            for a in actions:
                tga_data[squad][f"{a}_for"]     = round(float(tgg(f"ga_for_{a}",0)),4)
                tga_data[squad][f"{a}_against"] = round(float(tgg(f"ga_against_{a}",0)),4)
        print(f"  {len(tga_data)} teams with G+ breakdown data")

    if not all_players:
        print("\n  ERROR: No players loaded — aborting to avoid wiping index.html")
        return

    with open(HTML, "r", encoding="utf-8") as f:
        html = f.read()

    # Update stats date
    today = date.today().strftime('%m/%d/%y').lstrip('0').replace('/0','/')
    html = re.sub(r'STATS AS OF \d+/\d+/\d+', f'STATS AS OF {today}', html)
    print(f"  Date updated to {today}")

    # Strip previous injections
    html = re.sub(r'<!-- TLUSA-PLAYERS-START -->.*?<!-- TLUSA-PLAYERS-END -->', '',
                  html, flags=re.DOTALL)
    html = re.sub(r'<!-- TLUSA-TEAMS-START -->.*?<!-- TLUSA-TEAMS-END -->', '',
                  html, flags=re.DOTALL).rstrip()

    players_json = json.dumps(all_players, ensure_ascii=False)
    teams_json   = json.dumps(teams_data, ensure_ascii=False)
    traj_json    = json.dumps(traj_data, ensure_ascii=False)
    xpass_json   = json.dumps(xpass_data, ensure_ascii=False)
    tga_json     = json.dumps(tga_data, ensure_ascii=False)

    block = f"""

<!-- TLUSA-PLAYERS-START -->
<script>
(function() {{
  var _data = {players_json};

  function tryLoad() {{
    if (typeof players === 'undefined' ||
        typeof computeRatings === 'undefined' ||
        typeof refreshAll === 'undefined') {{
      setTimeout(tryLoad, 100);
      return;
    }}
    players = _data;
    refreshAll();
    console.log('[TLUSA] Loaded ' + players.length + ' players.');
  }}

  if (document.readyState === 'loading') {{
    document.addEventListener('DOMContentLoaded', function() {{ setTimeout(tryLoad, 100); }});
  }} else {{
    setTimeout(tryLoad, 100);
  }}
}})();
</script>
<!-- TLUSA-PLAYERS-END -->"""

    teams_block = f"""

<!-- TLUSA-TEAMS-START -->
<script>
(function() {{
  var _teams = {teams_json};
  var _traj  = {traj_json};
  var _xpass = {xpass_json};
  var _tga   = {tga_json};

  function tryLoadTeams() {{
    if (typeof teamsData === 'undefined' || typeof trajData === 'undefined') {{
      setTimeout(tryLoadTeams, 100);
      return;
    }}
    teamsData = _teams;
    trajData  = _traj;
    xpassData = _xpass;
    tgaData   = _tga;
    if (typeof renderTeams === 'function') renderTeams();
    console.log('[TLUSA] Loaded ' + _teams.length + ' teams.');
  }}

  if (document.readyState === 'loading') {{
    document.addEventListener('DOMContentLoaded', function() {{ setTimeout(tryLoadTeams, 100); }});
  }} else {{
    setTimeout(tryLoadTeams, 100);
  }}
}})();
</script>
<!-- TLUSA-TEAMS-END -->"""

    html += block
    html += teams_block

    with open(HTML, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n  Written to {HTML}")
    print("\n" + "="*50)
    print("  Done! Hard-refresh your browser: Cmd+Shift+R")
    print("="*50 + "\n")


if __name__ == "__main__":
    main()