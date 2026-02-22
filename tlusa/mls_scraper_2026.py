"""
Touchline USA — MLS Player Data Scraper
========================================
Scrapes FBref for 2026 MLS season stats and outputs two CSVs:
  mls_outfield_efficiency.csv
  mls_gk_efficiency.csv

Run:
  python mls_scraper_2026.py

Requirements:
  pip install requests beautifulsoup4 pandas lxml
"""

import io
import re
import sys
import time
import random
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup, Comment

# ─────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────
SEASON  = "2026"
MIN_90S = 1           # Low early in season — raise to 3+ mid-season
OUT_DIR = Path(__file__).parent

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

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]


# ─────────────────────────────────────────────────────────────
#  SESSION — looks like a real browser navigating FBref
# ─────────────────────────────────────────────────────────────
def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent":                random.choice(USER_AGENTS),
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language":           "en-US,en;q=0.9",
        "Accept-Encoding":           "gzip, deflate, br",
        "DNT":                       "1",
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest":            "document",
        "Sec-Fetch-Mode":            "navigate",
        "Sec-Fetch-Site":            "none",
        "Sec-Fetch-User":            "?1",
        "Cache-Control":             "max-age=0",
    })
    return s


def warm_up(session):
    """Visit FBref homepage first so we arrive like a real user."""
    try:
        print("  Warming up session on fbref.com...")
        session.get(FBREF, timeout=20)
        time.sleep(random.uniform(3, 6))
        session.get(f"{FBREF}/en/comps/22/Major-League-Soccer-Stats", timeout=20)
        time.sleep(random.uniform(3, 6))
        print("  Session ready.\n")
    except Exception as e:
        print(f"  Warm-up note: {e} — continuing anyway\n")


# ─────────────────────────────────────────────────────────────
#  FETCH
# ─────────────────────────────────────────────────────────────
def fetch(session, url, retries=4):
    for attempt in range(1, retries + 1):
        delay = random.uniform(8, 15) if attempt == 1 else random.uniform(25, 50)
        print(f"    Waiting {delay:.1f}s...")
        time.sleep(delay)

        session.headers["User-Agent"] = random.choice(USER_AGENTS)
        session.headers["Referer"]    = f"{FBREF}/en/comps/22/Major-League-Soccer-Stats"

        try:
            print(f"    GET {url}  (attempt {attempt}/{retries})")
            r = session.get(url, timeout=30)

            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 90))
                print(f"    Rate limited — waiting {wait}s...")
                time.sleep(wait + random.uniform(10, 20))
                continue

            if r.status_code == 403:
                print(f"    403 Forbidden — backing off before retry...")
                time.sleep(random.uniform(60, 120))
                continue

            r.raise_for_status()
            print(f"    OK ({len(r.content)//1024}KB)")
            return r.text

        except requests.exceptions.Timeout:
            print(f"    Timeout on attempt {attempt}")
        except requests.exceptions.RequestException as e:
            print(f"    Error: {e}")

    raise RuntimeError(f"Failed after {retries} attempts: {url}")


# ─────────────────────────────────────────────────────────────
#  PARSE
# ─────────────────────────────────────────────────────────────
def extract_table(html):
    soup = BeautifulSoup(html, "lxml")

    comment_dfs = []
    for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
        if "<table" not in str(comment):
            continue
        try:
            comment_dfs.extend(pd.read_html(io.StringIO(str(comment)), header=0))
        except Exception:
            pass

    visible_dfs = []
    if not comment_dfs:
        try:
            visible_dfs = pd.read_html(io.StringIO(html), header=1)
        except Exception:
            pass

    all_dfs = comment_dfs + visible_dfs
    if not all_dfs:
        raise ValueError("No tables found on page")

    df = max(all_dfs, key=lambda d: d.shape[1])
    return clean_df(df)


def clean_df(df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            " ".join(str(c) for c in col if str(c) not in ("", "nan", "Unnamed")).strip()
            for col in df.columns
        ]
    df.columns = [re.sub(r"\s*\+\d+$", "", c).strip() for c in df.columns]

    first = df.columns[0]
    df = df[df[first].astype(str).str.strip() != "Rk"]
    df = df[df[first].astype(str).str.strip() != first]

    if "Player" in df.columns:
        df = df[df["Player"].astype(str).str.strip().ne("")]
        df = df[df["Player"].notna()]

    return df.reset_index(drop=True)


# ─────────────────────────────────────────────────────────────
#  SCRAPE ALL
# ─────────────────────────────────────────────────────────────
def scrape_all(session):
    raw = {}
    keys = list(URLS.keys())
    for i, key in enumerate(keys):
        print(f"\n  [{i+1}/{len(keys)}] {key}")
        try:
            html    = fetch(session, URLS[key])
            df      = extract_table(html)
            raw[key] = df
            print(f"    {df.shape[0]} rows x {df.shape[1]} cols")
        except Exception as e:
            print(f"    FAILED: {e}")
            raw[key] = pd.DataFrame()
    return raw


# ─────────────────────────────────────────────────────────────
#  MERGE HELPERS
# ─────────────────────────────────────────────────────────────
MERGE_KEYS  = ["Player", "Squad"]
SHARED_COLS = {"Player", "Squad", "Pos", "Nation", "Age", "Born",
               "MP", "Starts", "Min", "90s", "Matches", "Rk"}

def merge(base, supp):
    if supp.empty:
        return base
    drop = [c for c in supp.columns if c in base.columns and c not in MERGE_KEYS]
    return base.merge(supp.drop(columns=drop, errors="ignore"), on=MERGE_KEYS, how="left")

def safe(df, col):
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce").fillna(0)
    return pd.Series(0.0, index=df.index)

def p90(s, nines):
    return s.div(nines.replace(0, float("nan"))).fillna(0)

def minmax(s):
    lo, hi = s.min(), s.max()
    if hi == lo:
        return pd.Series(50.0, index=s.index)
    return ((s - lo) / (hi - lo) * 100).round(2)


# ─────────────────────────────────────────────────────────────
#  OUTFIELD
# ─────────────────────────────────────────────────────────────
def build_outfield(raw):
    base = raw.get("standard", pd.DataFrame())
    if base.empty:
        raise RuntimeError("Standard stats missing — cannot continue")
    if "Pos" in base.columns:
        base = base[~base["Pos"].astype(str).str.contains("GK", na=False)]
    for key in ("shooting", "passing", "defense", "possession"):
        base = merge(base, raw.get(key, pd.DataFrame()))
    return base

def compute_outfield(df):
    df    = df.copy()
    nines = safe(df, "90s").clip(lower=0.01)

    gls  = p90(safe(df, "Gls"),     nines)
    xg   = p90(safe(df, "xG"),      nines)
    sot  = p90(safe(df, "SoT"),     nines)
    ast  = p90(safe(df, "Ast"),     nines)
    xag  = p90(safe(df, "xAG"),     nines)
    kp   = p90(safe(df, "KP"),      nines)
    prgp = p90(safe(df, "PrgP"),    nines)
    prgc = p90(safe(df, "PrgC"),    nines)
    att3 = p90(safe(df, "Att 3rd"), nines)

    atk_raw = (gls*0.20 + xg*0.18 + sot*0.10 + ast*0.12 + xag*0.10
            + kp*0.08 + prgp*0.08 + prgc*0.08 + att3*0.06)

    tkl  = p90(safe(df, "TklW"),   nines)
    intr = p90(safe(df, "Int"),    nines)
    blk  = p90(safe(df, "Blocks"), nines)
    clr  = p90(safe(df, "Clr"),    nines)
    pres = p90(safe(df, "Press"),  nines)
    ppct = safe(df, "Press%")
    awon = p90(safe(df, "Won"),    nines)

    def_raw = (tkl*0.22 + intr*0.20 + blk*0.12 + clr*0.12
            + pres*0.14 + ppct*0.10 + awon*0.10)

    df["Attacking_Efficiency"] = minmax(atk_raw)
    df["Defensive_Efficiency"] = minmax(def_raw)
    df["Goals_p90"]            = gls.round(3)
    df["xG_p90"]               = xg.round(3)
    df["Assists_p90"]          = ast.round(3)
    df["xAG_p90"]              = xag.round(3)
    df["SoT_p90"]              = sot.round(3)
    df["KeyPasses_p90"]        = kp.round(3)
    df["ProgPasses_p90"]       = prgp.round(3)
    df["ProgCarries_p90"]      = prgc.round(3)
    df["Tkl_Won_p90"]          = tkl.round(3)
    df["Interceptions_p90"]    = intr.round(3)
    df["Blocks_p90"]           = blk.round(3)
    df["Clearances_p90"]       = clr.round(3)
    df["Pressures_p90"]        = pres.round(3)
    df["Press_pct"]            = ppct.round(1)
    return df

OUTFIELD_COLS = [
    "Player", "Squad", "Pos", "Nation", "Age", "90s",
    "Attacking_Efficiency", "Defensive_Efficiency",
    "Goals_p90", "xG_p90", "Assists_p90", "xAG_p90",
    "SoT_p90", "KeyPasses_p90", "ProgPasses_p90", "ProgCarries_p90",
    "Tkl_Won_p90", "Interceptions_p90", "Blocks_p90",
    "Clearances_p90", "Pressures_p90", "Press_pct",
]


# ─────────────────────────────────────────────────────────────
#  GOALKEEPERS
# ─────────────────────────────────────────────────────────────
def build_gk(raw):
    gk  = raw.get("gk",     pd.DataFrame())
    adv = raw.get("gk_adv", pd.DataFrame())
    if gk.empty:
        return pd.DataFrame()
    merged = merge(gk, adv)
    if "Pos" in merged.columns:
        merged = merged[merged["Pos"].astype(str).str.contains("GK", na=False)]
    return merged

def compute_gk(df):
    df    = df.copy()
    nines = safe(df, "90s").clip(lower=0.01)

    sv   = safe(df, "Save%")
    psxg = p90(safe(df, "PSxG-GA"), nines)
    ga   = p90(safe(df, "GA"),      nines)
    cs   = safe(df, "CS%")
    cmp  = safe(df, "Cmp%")
    stp  = safe(df, "Stp%")
    opa  = p90(safe(df, "#OPA"),    nines)

    def nm(s, invert=False):
        s = -s if invert else s
        lo, hi = s.min(), s.max()
        if hi == lo:
            return pd.Series(50.0, index=s.index)
        return (s - lo) / (hi - lo) * 100

    df["GK_Efficiency"] = (
        nm(sv)        * 0.22 +
        nm(psxg)      * 0.22 +
        nm(ga, True)  * 0.15 +
        nm(cs)        * 0.12 +
        nm(cmp)       * 0.10 +
        nm(stp)       * 0.10 +
        nm(opa)       * 0.09
    ).round(2)
    df["GA_p90"]      = ga.round(3)
    df["PSxG-GA_p90"] = psxg.round(4)
    return df

GK_COLS = [
    "Player", "Squad", "Pos", "Nation", "Age", "90s",
    "GK_Efficiency", "GA_p90", "PSxG-GA_p90",
    "Save%", "CS%", "Launch%", "Cmp%", "Stp%",
]


# ─────────────────────────────────────────────────────────────
#  EXPORT
# ─────────────────────────────────────────────────────────────
def export(df, cols, path, sort_col):
    available = [c for c in cols if c in df.columns]
    out = df[available].copy()
    if "90s" in out.columns:
        out = out[pd.to_numeric(out["90s"], errors="coerce").fillna(0) >= MIN_90S]
    out.sort_values(sort_col, ascending=False, inplace=True, ignore_index=True)
    out.to_csv(path, index=False)
    print(f"  Saved {len(out)} rows to {path.name}")
    return out


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────
def main():
    div = "=" * 54
    print(f"\n{div}")
    print(f"  Touchline USA — MLS Scraper  ({SEASON})")
    print(f"{div}\n")

    session = make_session()
    warm_up(session)

    print("[1/4] Scraping FBref...\n")
    raw = scrape_all(session)

    print("\n[2/4] Processing outfield players...")
    try:
        outfield = build_outfield(raw)
        outfield = compute_outfield(outfield)
        nines    = pd.to_numeric(outfield["90s"], errors="coerce").fillna(0)
        outfield = outfield[nines >= MIN_90S].reset_index(drop=True)
        print(f"  {len(outfield)} outfield players")
    except Exception as e:
        print(f"  ERROR: {e}")
        outfield = pd.DataFrame()

    print("\n[3/4] Processing goalkeepers...")
    try:
        gks = build_gk(raw)
        if not gks.empty:
            gks   = compute_gk(gks)
            ng    = pd.to_numeric(gks["90s"], errors="coerce").fillna(0)
            gks   = gks[ng >= MIN_90S].reset_index(drop=True)
        print(f"  {len(gks)} goalkeepers")
    except Exception as e:
        print(f"  ERROR: {e}")
        gks = pd.DataFrame()

    print("\n[4/4] Writing CSVs...")
    out_path = OUT_DIR / "mls_outfield_efficiency.csv"
    gk_path  = OUT_DIR / "mls_gk_efficiency.csv"

    out_df = export(outfield, OUTFIELD_COLS, out_path, "Attacking_Efficiency") if not outfield.empty else pd.DataFrame()
    gk_df  = export(gks, GK_COLS, gk_path, "GK_Efficiency")                   if not gks.empty      else pd.DataFrame()

    print(f"\n{div}")
    if not out_df.empty:
        cols = ["Player", "Squad", "Pos", "Attacking_Efficiency", "Goals_p90", "xG_p90"]
        print("TOP ATTACKERS")
        print(out_df[[c for c in cols if c in out_df]].head(10).to_string(index=False))
        dc = ["Player", "Squad", "Pos", "Defensive_Efficiency", "Tkl_Won_p90", "Interceptions_p90"]
        print("\nTOP DEFENDERS")
        print(out_df.sort_values("Defensive_Efficiency", ascending=False)[[c for c in dc if c in out_df]].head(10).to_string(index=False))
    if not gk_df.empty:
        gc = ["Player", "Squad", "GK_Efficiency", "GA_p90", "Save%", "CS%"]
        print("\nTOP GOALKEEPERS")
        print(gk_df[[c for c in gc if c in gk_df]].head(10).to_string(index=False))

    print(f"\n{div}")
    print("  Done. Import the CSVs into Touchline USA.")
    print(f"{div}\n")


if __name__ == "__main__":
    main()