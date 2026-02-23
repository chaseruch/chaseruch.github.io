"""
Touchline USA — Data Injector
==============================
Reads mls_outfield_efficiency.csv and mls_gk_efficiency.csv
and injects the player data directly into index.html.

Run this once after running mls_scraper_2026.py:
  python3 inject_data.py

No server needed — just refresh index.html in your browser afterward.
"""

import json
import re
from pathlib import Path

import pandas as pd

DIR     = Path(__file__).parent
HTML    = DIR / "index.html"
OUT_CSV = DIR / "mls_outfield_efficiency.csv"
GK_CSV  = DIR / "mls_gk_efficiency.csv"


def csv_to_players(path: Path, is_gk: bool = False) -> list:
    if not path.exists():
        print(f"  WARNING: {path.name} not found, skipping")
        return []

    df = pd.read_csv(path)
    players = []

    for _, row in df.iterrows():
        def g(col, default=None):
            val = row.get(col, default)
            try:
                if pd.isna(val):
                    return default
            except Exception:
                pass
            return val

        p = {
            "id":       f"{'gk' if is_gk else 'of'}_{len(players)}",
            "name":     g("Player", ""),
            "squad":    g("Squad", ""),
            "pos":      g("Pos", "GK" if is_gk else ""),
            "age":      str(g("Age", "")),
            "nation":   g("Nation", ""),
            "nineties": float(g("90s", 0) or 0),
            "isGK":     is_gk,
        }

        if is_gk:
            p["GK_Efficiency"] = float(g("GK_Efficiency", 0) or 0)
            p["GA_p90"]        = float(g("GA_p90", 0) or 0)
            p["Save_pct"]      = float(g("Save%", 0) or 0)
            p["GA_minus_xGA"]  = float(g("GA_minus_xGA", 0) or 0)
        else:
            p["Attacking_Efficiency"] = float(g("Attacking_Efficiency", 0) or 0)
            p["Defensive_Efficiency"] = float(g("Defensive_Efficiency", 0) or 0)
            p["Goals_p90"]     = float(g("Goals_p90", 0) or 0)
            p["xG_p90"]        = float(g("xG_p90", 0) or 0)
            p["Assists_p90"]   = float(g("Assists_p90", 0) or 0)
            p["xAG_p90"]       = float(g("xAG_p90", 0) or 0)
            p["SoT_p90"]       = float(g("SoT_p90", 0) or 0)
            p["KeyPasses_p90"] = float(g("KeyPasses_p90", 0) or 0)
            p["Goals_Added"]   = float(g("Goals_Added", 0) or 0)
            # Aliases the dashboard uses
            p["Gls"]           = p["Goals_p90"]
            p["xG"]            = p["xG_p90"]
            p["Ast"]           = p["Assists_p90"]
            p["xAG"]           = p["xAG_p90"]
            p["SoT"]           = p["SoT_p90"]
            p["KP"]            = p["KeyPasses_p90"]
            p["Tkl_Won_p90"]   = 0.0
            p["Interceptions_p90"] = 0.0

        players.append(p)

    return players


def main():
    print("\n" + "=" * 50)
    print("  Touchline USA — Data Injector")
    print("=" * 50 + "\n")

    if not HTML.exists():
        print(f"ERROR: {HTML.name} not found in {DIR}")
        return

    print("Reading CSVs...")
    outfield = csv_to_players(OUT_CSV, is_gk=False)
    gks      = csv_to_players(GK_CSV,  is_gk=True)
    all_players = outfield + gks
    print(f"  {len(outfield)} outfield players")
    print(f"  {len(gks)} goalkeepers")
    print(f"  {len(all_players)} total")

    with open(HTML, "r", encoding="utf-8") as f:
        html = f.read()

    # Remove any previous injection
    html = re.sub(r'<!-- PLAYER-DATA-START -->.*?<!-- PLAYER-DATA-END -->\n?', '', html, flags=re.DOTALL)

    players_json = json.dumps(all_players, ensure_ascii=False)

    # The loader uses setTimeout(0) so it fires AFTER the page init
    # sequence (toggleGKFields, renderMatchday, refreshAll) completes
    inject_block = f"""<!-- PLAYER-DATA-START -->
<script>
(function() {{
  var _playerData = {players_json};
  setTimeout(function() {{
    try {{
      if (typeof players === 'undefined' || typeof computeRatings === 'undefined') {{
        console.warn('Dashboard not ready, retrying...');
        return;
      }}
      players = _playerData;
      computeRatings(players);
      refreshAll();
      console.log('Loaded ' + players.length + ' players from CSV data.');
    }} catch(e) {{ console.warn('Player load error:', e); }}
  }}, 200);
}})();
</script>
<!-- PLAYER-DATA-END -->
"""

    html = html.rstrip() + "\n" + inject_block

    with open(HTML, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n  Injected {len(all_players)} players into {HTML.name}")
    print("\n" + "=" * 50)
    print("  Done! Hard-refresh index.html in your browser (Cmd+Shift+R).")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    main()