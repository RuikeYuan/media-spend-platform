"""Generate realistic, messy agency delivery files for September 2026.

Three fictional agencies each deliver weekly files in their own layout.
Known data problems are injected on purpose so the pipeline has something
real to catch:

  agency_alpha  week 1: exact duplicate rows, one row with clicks > impressions
  agency_alpha  week 2: a v2 restatement file with corrected (higher) spend
  agency_bravo  week 2: a negative spend row, a row with unmapped channel "radio"
  agency_bravo  week 3: an impossible date (31/09/2026)
  agency_charlie week 2: a fat-finger spend (100x)
  agency_charlie week 3: a row with an empty campaign id
  agency_charlie week 4: a campaign id missing from the campaign master
  agency_charlie week 5: delivered late (only written with --late)
"""
from __future__ import annotations

import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

WEEKS = [
    (date(2026, 9, 1), date(2026, 9, 7)),
    (date(2026, 9, 8), date(2026, 9, 14)),
    (date(2026, 9, 15), date(2026, 9, 21)),
    (date(2026, 9, 22), date(2026, 9, 28)),
    (date(2026, 9, 29), date(2026, 9, 30)),
]

# channel -> (cpm range, ctr range)
ECONOMICS = {
    "TV": ((8.0, 14.0), (0.0, 0.0)),
    "ONLINE_VIDEO": ((12.0, 22.0), (0.002, 0.006)),
    "SOCIAL": ((4.0, 9.0), (0.006, 0.015)),
    "SEARCH": ((25.0, 45.0), (0.03, 0.07)),
    "OOH": ((3.0, 6.0), (0.0, 0.0)),
    "DISPLAY": ((1.5, 4.0), (0.001, 0.004)),
}
IMPRESSIONS = {
    "TV": (400_000, 900_000),
    "ONLINE_VIDEO": (80_000, 250_000),
    "SOCIAL": (150_000, 500_000),
    "SEARCH": (8_000, 30_000),
    "OOH": (200_000, 600_000),
    "DISPLAY": (300_000, 900_000),
}

# Per agency: campaign -> list of (canonical channel, raw channel label, publisher)
PLANS = {
    "agency_alpha": {
        "NL-1001": [("TV", "TV", "NPO"), ("ONLINE_VIDEO", "Online Video", "YouTube"), ("SOCIAL", "Social", "Instagram")],
        "NL-1002": [("SEARCH", "Search", "Google"), ("SOCIAL", "Social", "TikTok")],
        "NL-1003": [("OOH", "OOH", "JCDecaux"), ("ONLINE_VIDEO", "Online Video", "Videoland")],
    },
    "agency_bravo": {
        "UK-2001": [("TV", "Television", "ITV"), ("SEARCH", "Paid Search", "Google")],
        "UK-2002": [("SOCIAL", "Social Media", "Meta"), ("ONLINE_VIDEO", "OLV", "YouTube"), ("OOH", "Out-of-Home", "Clear Channel")],
        "UK-2003": [("SEARCH", "Paid Search", "Bing"), ("SOCIAL", "Social Media", "Snapchat")],
    },
    "agency_charlie": {
        "US-3001": [("TV", "Linear TV", "NBCU"), ("SOCIAL", "Social", "Meta"), ("DISPLAY", "Display", "The Trade Desk")],
        "US-3002": [("ONLINE_VIDEO", "Video", "Hulu"), ("SEARCH", "SEM", "Google")],
        "US-3003": [("DISPLAY", "Display", "Google DV360"), ("SOCIAL", "Social", "Pinterest")],
    },
}


def _days(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def _metrics(rng: random.Random, channel: str) -> tuple[int, int, float]:
    imps = rng.randint(*IMPRESSIONS[channel])
    cpm = rng.uniform(*ECONOMICS[channel][0])
    ctr = rng.uniform(*ECONOMICS[channel][1])
    return imps, int(imps * ctr), round(imps / 1000 * cpm, 2)


def _base_rows(rng, agency, start, end):
    rows = []
    for day in _days(start, end):
        for campaign, lines in PLANS[agency].items():
            for canonical, raw_channel, publisher in lines:
                imps, clicks, spend = _metrics(rng, canonical)
                rows.append(dict(day=day, campaign=campaign, channel=raw_channel,
                                 publisher=publisher, imps=imps, clicks=clicks, spend=spend))
    return rows


# ---------------------------------------------------------------- writers --
def _write_alpha(path: Path, rows):
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Date", "Campaign ID", "Channel", "Publisher", "Impressions", "Clicks", "Net Cost (EUR)"])
        for r in rows:
            w.writerow([r["day"].isoformat(), r["campaign"], r["channel"], r["publisher"],
                        r["imps"], r["clicks"], f'{r["spend"]:.2f}'])


def _write_bravo(path: Path, rows):
    with path.open("w", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["day", "campaign_code", "media_type", "vendor", "imps", "clicks", "spend", "currency"])
        for r in rows:
            day = r["day"] if isinstance(r["day"], str) else r["day"].strftime("%d/%m/%Y")
            w.writerow([day, r["campaign"], r["channel"].lower(), r["publisher"],
                        r["imps"], r["clicks"], f'{r["spend"]:.2f}'.replace(".", ","), "GBP"])


def _write_charlie(path: Path, rows):
    with path.open("w") as f:
        for r in rows:
            f.write(json.dumps({
                "date": r["day"].strftime("%m/%d/%Y"),
                "campaignId": r["campaign"],
                "mediaChannel": r["channel"],
                "site": r["publisher"],
                "impressions": r["imps"],
                "clicks": r["clicks"],
                "costUsd": r["spend"],
            }) + "\n")


def _fname(prefix, start, end, version=1, ext="csv"):
    return f"{prefix}_{start.isoformat()}_{end.isoformat()}_v{version}.{ext}"


def generate(landing_dir: Path, late: bool = False, seed: int = 42) -> list[Path]:
    """Write agency files into landing_dir/<agency>/. Returns the paths written."""
    written: list[Path] = []
    for agency in PLANS:
        (landing_dir / agency).mkdir(parents=True, exist_ok=True)

    for i, (start, end) in enumerate(WEEKS, start=1):
        # ---- agency_alpha (NL, EUR, CSV) ----
        rng = random.Random(f"{seed}-alpha-{i}")
        rows = _base_rows(rng, "agency_alpha", start, end)
        if i == 1:
            rows += [dict(rows[3]), dict(rows[4])]                 # exact duplicates
            rows[5]["clicks"] = rows[5]["imps"] + 50               # clicks > impressions
        p = landing_dir / "agency_alpha" / _fname("alpha", start, end)
        _write_alpha(p, rows)
        written.append(p)
        if i == 2:                                                 # restatement: fees corrected
            restated = [dict(r, spend=round(r["spend"] * 1.05, 2)) for r in rows]
            p2 = landing_dir / "agency_alpha" / _fname("alpha", start, end, version=2)
            _write_alpha(p2, restated)
            written.append(p2)

        # ---- agency_bravo (UK, GBP, semicolon CSV, decimal comma) ----
        rng = random.Random(f"{seed}-bravo-{i}")
        rows = _base_rows(rng, "agency_bravo", start, end)
        if i == 2:
            rows[2]["spend"] = -350.00                             # negative spend
            rows.append(dict(rows[7], channel="Radio", publisher="Global Radio"))  # unmapped channel
        if i == 3:
            rows.append(dict(rows[1], day="31/09/2026", publisher="ITV Hub"))      # impossible date
        p = landing_dir / "agency_bravo" / _fname("bravo", start, end)
        _write_bravo(p, rows)
        written.append(p)

        # ---- agency_charlie (US, USD, JSON lines) ----
        if i == 5 and not late:
            continue                                               # last file not delivered yet
        rng = random.Random(f"{seed}-charlie-{i}")
        rows = _base_rows(rng, "agency_charlie", start, end)
        if i == 2:
            rows[6]["spend"] = round(rows[6]["spend"] * 100, 2)    # fat finger
        if i == 3:
            rows[4]["campaign"] = ""                               # missing campaign id
        if i == 4:
            for r in rows[:2]:
                r["campaign"] = "US-3999"                          # not in campaign master
        p = landing_dir / "agency_charlie" / _fname("charlie", start, end, ext="jsonl")
        _write_charlie(p, rows)
        written.append(p)

    return written
