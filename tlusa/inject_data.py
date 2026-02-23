"""
Touchline USA — Data Injector
==============================
Reads mls_outfield_efficiency.csv and mls_gk_efficiency.csv
and injects the player data directly into index.html.

Run this once after running mls_scraper_2026.py:
  python3 inject_data.py

No server needed — just open index.html in your browser afterward.
"""

import json
import re
from pathlib import Path

import pandas as pd

DIR      = Path(__file__).parent
HTML     = DIR / "index.html"
OUT_CSV  = DIR / "mls_outfield_efficiency.csv"
GK_CSV   = DIR / "mls_gk_efficiency.csv"


def csv_to_players(path: Path, is_gk: bool = False) -> list:
    if not path.exists():
        print(f"  WARNING: {path.name} not found, skipping")
        return []

    df = pd.read_csv(path)
    players = []

    for _, row in df.iterrows():
        def g(col, default=None):
            val = row.get(col, default)
            if pd.isna(val):
                return default
            return val

        p = {
            "id":     f"{'gk' if is_gk else 'of'}_{len(players)}_{str(g('Player','x')).replace(' ','_')}",
            "name":   g("Player", ""),
            "squad":  g("Squad", ""),
            "pos":    g("Pos", "GK" if is_gk else ""),
            "age":    g("Age", ""),
            "nation": g("Nation", g("nationality", "")),
            "nineties": g("90s", 0),
            "isGK":   is_gk,
        }

        if is_gk:
            p["GK_Efficiency"]  = g("GK_Efficiency", 0)
            p["GA_p90"]         = g("GA_p90", 0)
            p["Save_pct"]       = g("Save%", 0)
            p["GA_minus_xGA"]   = g("GA_minus_xGA", 0)
        else:
            p["Attacking_Efficiency"] = g("Attacking_Efficiency", 0)
            p["Defensive_Efficiency"] = g("Defensive_Efficiency", 0)
            p["Gls"]    = g("Goals_p90", 0)
            p["xG"]     = g("xG_p90", 0)
            p["Ast"]    = g("Assists_p90", 0)
            p["xAG"]    = g("xAG_p90", 0)
            p["SoT"]    = g("SoT_p90", 0)
            p["KP"]     = g("KeyPasses_p90", 0)
            p["Goals_Added"] = g("Goals_Added", 0)

        players.append(p)

    return players


def main():
    print("\n" + "=" * 50)
    print("  Touchline USA — Data Injector")
    print("=" * 50 + "\n")

    if not HTML.exists():
        print(f"ERROR: {HTML.name} not found in {DIR}")
        return

    # Load CSVs
    print("Reading CSVs...")
    outfield = csv_to_players(OUT_CSV, is_gk=False)
    gks      = csv_to_players(GK_CSV,  is_gk=True)
    all_players = outfield + gks
    print(f"  {len(outfield)} outfield players")
    print(f"  {len(gks)} goalkeepers")
    print(f"  {len(all_players)} total")

    # Read HTML
    with open(HTML, "r", encoding="utf-8") as f:
        html = f.read()

    # Build the data island
    players_json = json.dumps(all_players, ensure_ascii=False)
    data_island = f'\n<div id="player-data-store" style="display:none" data-players=\'{players_json}\'></div>\n'

    # Remove any existing player data island
    html = re.sub(r'\n<div id="player-data-store"[^>]*>.*?</div>\n', '\n', html, flags=re.DOTALL)

    # Insert just before </body>
    html = html.replace('</body>', data_island + '</body>')

    # Inject loader JS if not already present
    loader = """
// Load players from injected data island
(function() {
  try {
    const store = document.getElementById('player-data-store');
    if (!store) return;
    const data = JSON.parse(store.dataset.players || '[]');
    if (!data.length) return;
    players = data;
    computeRatings(players);
    refreshAll();
  } catch(e) { console.warn('Player store load error:', e); }
})();"""

    # Remove old loader if present
    html = re.sub(r'\n// Load players from injected data island.*?\}\)\(\);', '', html, flags=re.DOTALL)

    # Insert loader just before the closing </script> of the main script block
    html = html.replace(
        'toggleGKFields();\nrenderMatchday();\nrefreshAll();',
        'toggleGKFields();\nrenderMatchday();\nrefreshAll();\n' + loader
    )

    # Write back
    with open(HTML, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n  Injected into {HTML.name}")
    print("\n" + "=" * 50)
    print("  Done! Open index.html in your browser.")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    main()