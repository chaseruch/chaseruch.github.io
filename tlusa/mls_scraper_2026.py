"""
MLS Player Data Scraper — FBref  (v2)
======================================
Run:  python mls_scraper_v2.py

Outputs (same directory as script):
  mls_outfield_efficiency.csv
  mls_gk_efficiency.csv

Requires:  pip install requests beautifulsoup4 pandas lxml
"""

import io
import re
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup, Comment

# ─────────────────────────────────────────────────────────────
#  CONFIG  — change season here if needed
# ─────────────────────────────────────────────────────────────
SEASON   = "2026"
MIN_90S  = 1          # drop players with fewer 90-min chunks
DELAY    = 5          # seconds between requests (be polite to FBref)
RETRIES  = 3
OUT_DIR  = Path(__file__).parent   # CSVs land next to the script

FBREF = "https://fbref.com"
URLS = {
    "standard":   f"{FBREF}/en/comps/22/{SEASON}/stats/Major-League-Soccer-Stats",
    "shooting":   f"{FBREF}/en/comps/22/{SEASON}/shooting/Major-League-Soccer-Stats",
    "passing":    f"{FBREF}/en/comps/22/{SEASON}/passing/Major-League-Soccer-Stats",
    "defense":    f"{FBREF}/en/comps/22/{SEASON}/defense/Major-League-Soccer-Stats",
    "possession": f"{FBREF}/en/comps/22/{SEASON}/possession/Major-League-Soccer-Stats",
    "gk":         f"{FBREF}/en/comps/22/{SEASON}/keepers/Major-League-Soccer-Stats",
    "gk_adv":     f"{FBREF}/en/comps/22/{SEASON}/keepersadv/Major-League-Soccer-Stats",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


# ─────────────────────────────────────────────────────────────
#  FETCH + PARSE
# ─────────────────────────────────────────────────────────────

def get_html(url: str) -> str:
    """Fetch a URL with retries, return raw HTML text."""
    for attempt in range(1, RETRIES + 1):
        try:
            print(f"    GET {url}  (attempt {attempt})")
            r = requests.get(url, headers=HEADERS, timeout=25)
            r.raise_for_status()
            return r.text
        except Exception as exc:
            print(f"    ✗  {exc}")
            if attempt < RETRIES:
                time.sleep(DELAY * attempt)
    raise RuntimeError(f"Failed to fetch: {url}")


def _read_tables(html_fragment: str, header: int) -> list[pd.DataFrame]:
    """Read all tables from an HTML string with the given header row index."""
    try:
        return pd.read_html(io.StringIO(html_fragment), header=header)
    except Exception:
        return []


def extract_table(html: str) -> pd.DataFrame:
    """
    FBref wraps its main stats tables inside HTML comments to prevent
    naive scrapers.  Strategy:
      1. Parse the page with BeautifulSoup
      2. Pull every HTML comment that contains a <table>
         → read with header=0  (comment tables have a single clean <thead>)
      3. If no comment-tables found, fall back to visible tables
         → read with header=1  (FBref live pages use a double-row header)
      4. Pick the widest DataFrame (most stat columns)
      5. Clean / deduplicate
    """
    soup = BeautifulSoup(html, "lxml")

    # ── Step 1: comment-wrapped tables (header row index = 0) ─
    comment_tables: list[pd.DataFrame] = []
    for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
        if "<table" not in str(comment):
            continue
        comment_tables.extend(_read_tables(str(comment), header=0))

    # ── Step 2: visible-page fallback (double-row header = 1) ─
    visible_tables: list[pd.DataFrame] = []
    if not comment_tables:
        visible_tables = _read_tables(html, header=1)

    all_tables = comment_tables + visible_tables
    if not all_tables:
        raise ValueError("No tables found on page")

    # ── Step 3: widest table wins ─────────────────────────────
    df = max(all_tables, key=lambda t: t.shape[1])

    # ── Step 4: clean ─────────────────────────────────────────
    df = _clean(df)
    return df


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten MultiIndex columns, strip FBref repeat-header rows."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            " ".join(str(c) for c in col if str(c) not in ("", "nan", "Unnamed")).strip()
            for col in df.columns
        ]

    # Remove annotation suffixes like "+2" that FBref appends
    df.columns = [re.sub(r"\s*\+\d+$", "", c).strip() for c in df.columns]

    # Drop FBref's repeated "Rk" header rows
    first_col = df.columns[0]
    df = df[df[first_col].astype(str).str.strip() != "Rk"]
    df = df[df[first_col].astype(str).str.strip() != first_col]

    # Drop rows where Player is NaN or empty
    if "Player" in df.columns:
        df = df[df["Player"].astype(str).str.strip().ne("")]
        df = df[df["Player"].notna()]

    return df.reset_index(drop=True)


# ─────────────────────────────────────────────────────────────
#  SCRAPE ALL TABLES
# ─────────────────────────────────────────────────────────────

def scrape_all() -> dict[str, pd.DataFrame]:
    raw: dict[str, pd.DataFrame] = {}
    for key, url in URLS.items():
        try:
            html = get_html(url)
            raw[key] = extract_table(html)
            print(f"    ✓  {key}: {raw[key].shape[0]} rows × {raw[key].shape[1]} cols")
        except Exception as exc:
            print(f"    ✗  {key} failed: {exc}")
            raw[key] = pd.DataFrame()
        time.sleep(DELAY)
    return raw


# ─────────────────────────────────────────────────────────────
#  MERGE HELPERS
# ─────────────────────────────────────────────────────────────

# Columns that identify a unique player row — used as merge keys
MERGE_KEYS = ["Player", "Squad"]

# Columns we never want duplicated from supplemental tables
SHARED_COLS = {"Player", "Squad", "Pos", "Nation", "Age", "Born", "MP",
               "Starts", "Min", "90s", "Matches", "Rk"}


def _merge(base: pd.DataFrame, supplement: pd.DataFrame) -> pd.DataFrame:
    """Left-join supplement onto base, deduplicating overlapping columns."""
    if supplement.empty:
        return base
    drop = [c for c in supplement.columns if c in base.columns and c not in MERGE_KEYS]
    return base.merge(supplement.drop(columns=drop, errors="ignore"),
                      on=MERGE_KEYS, how="left")


# ─────────────────────────────────────────────────────────────
#  OUTFIELD PROCESSING
# ─────────────────────────────────────────────────────────────

def build_outfield(raw: dict) -> pd.DataFrame:
    base = raw.get("standard", pd.DataFrame())
    if base.empty:
        raise RuntimeError("Standard stats table is empty — cannot proceed.")

    # Outfield only
    if "Pos" in base.columns:
        base = base[~base["Pos"].astype(str).str.contains("GK", na=False)]

    for key in ("shooting", "passing", "defense", "possession"):
        base = _merge(base, raw.get(key, pd.DataFrame()))

    return base


def safe(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce").fillna(0)
    return pd.Series(0.0, index=df.index)


def p90(s: pd.Series, nines: pd.Series) -> pd.Series:
    return s.div(nines.replace(0, float("nan"))).fillna(0)


def minmax(s: pd.Series) -> pd.Series:
    lo, hi = s.min(), s.max()
    if hi == lo:
        return pd.Series(50.0, index=s.index)
    return ((s - lo) / (hi - lo) * 100).round(2)


def compute_outfield(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    nines = safe(df, "90s").clip(lower=0.01)

    # ── Per-90 attacking stats ────────────────────────────────
    gls  = p90(safe(df, "Gls"),    nines)
    xg   = p90(safe(df, "xG"),     nines)
    sot  = p90(safe(df, "SoT"),    nines)
    ast  = p90(safe(df, "Ast"),    nines)
    xag  = p90(safe(df, "xAG"),    nines)
    kp   = p90(safe(df, "KP"),     nines)
    prgp = p90(safe(df, "PrgP"),   nines)
    prgc = p90(safe(df, "PrgC"),   nines)
    att3 = p90(safe(df, "Att 3rd"),nines)

    atk_raw = (gls*0.20 + xg*0.18 + sot*0.10 + ast*0.12 + xag*0.10
               + kp*0.08 + prgp*0.08 + prgc*0.08 + att3*0.06)

    # ── Per-90 defensive stats ────────────────────────────────
    tkl  = p90(safe(df, "TklW"),   nines)
    intr = p90(safe(df, "Int"),    nines)
    blk  = p90(safe(df, "Blocks"), nines)
    clr  = p90(safe(df, "Clr"),    nines)
    pres = p90(safe(df, "Press"),  nines)
    ppct = safe(df, "Press%")
    awon = p90(safe(df, "Won"),    nines)

    def_raw = (tkl*0.22 + intr*0.20 + blk*0.12 + clr*0.12
               + pres*0.14 + ppct*0.10 + awon*0.10)

    # ── Efficiency scores (0–100) ─────────────────────────────
    df["Attacking_Efficiency"] = minmax(atk_raw)
    df["Defensive_Efficiency"] = minmax(def_raw)

    # ── Store key per-90 cols ─────────────────────────────────
    df["Goals_p90"]         = gls.round(3)
    df["xG_p90"]            = xg.round(3)
    df["Assists_p90"]       = ast.round(3)
    df["xAG_p90"]           = xag.round(3)
    df["SoT_p90"]           = sot.round(3)
    df["KeyPasses_p90"]     = kp.round(3)
    df["ProgPasses_p90"]    = prgp.round(3)
    df["ProgCarries_p90"]   = prgc.round(3)
    df["Tkl_Won_p90"]       = tkl.round(3)
    df["Interceptions_p90"] = intr.round(3)
    df["Blocks_p90"]        = blk.round(3)
    df["Clearances_p90"]    = clr.round(3)
    df["Pressures_p90"]     = pres.round(3)
    df["Press_pct"]         = ppct.round(1)

    return df


OUTFIELD_COLS = [
    "Player", "Squad", "Pos", "Nation", "Age", "90s",
    "Attacking_Efficiency", "Defensive_Efficiency",
    # Attacking
    "Goals_p90", "xG_p90", "Assists_p90", "xAG_p90",
    "SoT_p90", "KeyPasses_p90", "ProgPasses_p90", "ProgCarries_p90",
    # Defensive
    "Tkl_Won_p90", "Interceptions_p90", "Blocks_p90",
    "Clearances_p90", "Pressures_p90", "Press_pct",
]


# ─────────────────────────────────────────────────────────────
#  GOALKEEPER PROCESSING
# ─────────────────────────────────────────────────────────────

def build_gk(raw: dict) -> pd.DataFrame:
    gk  = raw.get("gk",     pd.DataFrame())
    adv = raw.get("gk_adv", pd.DataFrame())
    if gk.empty:
        return pd.DataFrame()

    merged = _merge(gk, adv)

    # Keep GK position only
    if "Pos" in merged.columns:
        merged = merged[merged["Pos"].astype(str).str.contains("GK", na=False)]

    return merged


def compute_gk(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    nines = safe(df, "90s").clip(lower=0.01)

    sv_pct   = safe(df, "Save%")
    psxg_ga  = p90(safe(df, "PSxG-GA"), nines)   # positive = better than expected
    ga_p90   = p90(safe(df, "GA"),      nines)
    cs_pct   = safe(df, "CS%")
    launch   = safe(df, "Launch%")
    cmp      = safe(df, "Cmp%")
    stp      = safe(df, "Stp%")
    opa      = p90(safe(df, "#OPA"),    nines)

    def nm(s: pd.Series, invert: bool = False) -> pd.Series:
        s = -s if invert else s
        lo, hi = s.min(), s.max()
        if hi == lo:
            return pd.Series(50.0, index=s.index)
        return ((s - lo) / (hi - lo) * 100)

    score = (nm(sv_pct)           * 0.22 +
             nm(psxg_ga)          * 0.22 +
             nm(ga_p90, True)     * 0.15 +
             nm(cs_pct)           * 0.12 +
             nm(cmp)              * 0.10 +
             nm(stp)              * 0.10 +
             nm(opa)              * 0.09)

    df["GK_Efficiency"]  = score.round(2)
    df["GA_p90"]         = ga_p90.round(3)
    df["PSxG-GA_p90"]    = psxg_ga.round(4)

    return df


GK_COLS = [
    "Player", "Squad", "Pos", "Nation", "Age", "90s",
    "GK_Efficiency",
    "GA_p90", "PSxG-GA_p90",
    "Save%", "CS%", "Launch%", "Cmp%", "Stp%",
]


# ─────────────────────────────────────────────────────────────
#  EXPORT
# ─────────────────────────────────────────────────────────────

def export_csv(df: pd.DataFrame, col_order: list, path: Path,
               sort_col: str) -> pd.DataFrame:
    available = [c for c in col_order if c in df.columns]
    out = df[available].copy()

    # Min-minutes filter
    if "90s" in out.columns:
        out = out[pd.to_numeric(out["90s"], errors="coerce").fillna(0) >= MIN_90S]

    out.sort_values(sort_col, ascending=False, inplace=True, ignore_index=True)
    out.to_csv(path, index=False)
    print(f"  ✓  Saved {len(out)} rows → {path.name}")
    return out


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────

def main():
    sep = "=" * 58
    print(f"\n{sep}")
    print(f"  MLS Efficiency Scraper  —  {SEASON} Season")
    print(f"{sep}\n")

    # ── 1. Scrape ─────────────────────────────────────────────
    print("[1/4] Scraping FBref…")
    raw = scrape_all()

    # ── 2. Outfield ───────────────────────────────────────────
    print("\n[2/4] Processing outfield players…")
    try:
        outfield = build_outfield(raw)
        outfield = compute_outfield(outfield)
        before = len(outfield)
        nines  = pd.to_numeric(outfield["90s"], errors="coerce").fillna(0)
        outfield = outfield[nines >= MIN_90S].reset_index(drop=True)
        print(f"  {before} players → {len(outfield)} after min-minutes filter")
    except Exception as exc:
        print(f"  ERROR: {exc}")
        outfield = pd.DataFrame()

    # ── 3. Goalkeepers ────────────────────────────────────────
    print("\n[3/4] Processing goalkeepers…")
    try:
        gks = build_gk(raw)
        if not gks.empty:
            gks = compute_gk(gks)
            nines_gk = pd.to_numeric(gks["90s"], errors="coerce").fillna(0)
            gks = gks[nines_gk >= MIN_90S].reset_index(drop=True)
        print(f"  {len(gks)} goalkeepers kept")
    except Exception as exc:
        print(f"  ERROR: {exc}")
        gks = pd.DataFrame()

    # ── 4. Write CSVs ─────────────────────────────────────────
    print("\n[4/4] Writing CSVs…")

    out_path = OUT_DIR / "mls_outfield_efficiency.csv"
    gk_path  = OUT_DIR / "mls_gk_efficiency.csv"

    if not outfield.empty:
        out_df = export_csv(outfield, OUTFIELD_COLS, out_path, "Attacking_Efficiency")
    else:
        print("  ✗  No outfield data to write.")
        out_df = pd.DataFrame()

    if not gks.empty:
        gk_df = export_csv(gks, GK_COLS, gk_path, "GK_Efficiency")
    else:
        print("  ✗  No GK data to write.")
        gk_df = pd.DataFrame()

    # ── Preview ───────────────────────────────────────────────
    print(f"\n{sep}")
    if not out_df.empty:
        preview_cols = ["Player","Squad","Pos","Attacking_Efficiency","Goals_p90","xG_p90","Assists_p90"]
        print("  TOP ATTACKERS")
        print(out_df[[c for c in preview_cols if c in out_df.columns]].head(10).to_string(index=False))

        print("\n  TOP DEFENDERS")
        def_cols = ["Player","Squad","Pos","Defensive_Efficiency","Tkl_Won_p90","Interceptions_p90","Pressures_p90"]
        srt = out_df.sort_values("Defensive_Efficiency", ascending=False)
        print(srt[[c for c in def_cols if c in srt.columns]].head(10).to_string(index=False))

    if not gk_df.empty:
        print("\n  TOP GOALKEEPERS")
        gk_prev = ["Player","Squad","GK_Efficiency","GA_p90","PSxG-GA_p90","Save%","CS%"]
        print(gk_df[[c for c in gk_prev if c in gk_df.columns]].head(10).to_string(index=False))

    print(f"\n{sep}")
    print("  Done.  Load the two CSVs into the dashboard to see live data.")
    print(f"{sep}\n")


if __name__ == "__main__":
    main()