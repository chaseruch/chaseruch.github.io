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

    # ── Remove any previously injected blocks ──────────────────
    html = re.sub(r'<!-- PLAYER-DATA-START -->.*?<!-- PLAYER-DATA-END -->\n?', '', html, flags=re.DOTALL)

    # ── Build data + loader block ──────────────────────────────
    players_json = json.dumps(all_players, ensure_ascii=False)

    inject_block = f"""<!-- PLAYER-DATA-START -->
<script id="player-data-store" type="application/json">
{players_json}
</script>
<script>
(function() {{
  try {{
    var raw = document.getElementById('player-data-store').textContent;
    var data = JSON.parse(raw);
    if (!data || !data.length) return;
    players = data;
    computeRatings(players);
    refreshAll();
    console.log('Loaded ' + data.length + ' players from CSV data.');
  }} catch(e) {{ console.warn('Player store load error:', e); }}
}})();
</script>
<!-- PLAYER-DATA-END -->
"""

    # ── Append block right at the end of the file ─────────────
    html = html.rstrip() + "\n" + inject_block

    # Write back
    with open(HTML, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n  Injected {len(all_players)} players into {HTML.name}")
    print("\n" + "=" * 50)
    print("  Done! Refresh index.html in your browser.")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    main()