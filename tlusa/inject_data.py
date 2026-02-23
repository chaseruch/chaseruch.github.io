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

DIR     = Path(__file__).parent
HTML    = DIR / "index.html"
OUT_CSV = DIR / "mls_outfield_efficiency.csv"
GK_CSV  = DIR / "mls_gk_efficiency.csv"


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

    if not all_players:
        print("\n  ERROR: No players loaded — aborting to avoid wiping index.html")
        return

    with open(HTML, "r", encoding="utf-8") as f:
        html = f.read()

    # Update stats date
    today = date.today().strftime('%m/%d/%y').lstrip('0').replace('/0','/')
    html = re.sub(r'STATS AS OF \d+/\d+/\d+', f'STATS AS OF {today}', html)
    print(f"  Date updated to {today}")

    # Strip previous injection
    html = re.sub(r'<!-- TLUSA-PLAYERS-START -->.*?<!-- TLUSA-PLAYERS-END -->', '',
                  html, flags=re.DOTALL).rstrip()

    players_json = json.dumps(all_players, ensure_ascii=False)

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

    html += block

    with open(HTML, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n  Written to {HTML}")
    print("\n" + "="*50)
    print("  Done! Hard-refresh your browser: Cmd+Shift+R")
    print("="*50 + "\n")


if __name__ == "__main__":
    main()