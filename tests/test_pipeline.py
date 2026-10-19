"""End-to-end behaviour of the pipeline on the injected-problem sample data."""
import csv

from conftest import query
from msp.generate import generate
from msp.pipeline import run

QUIET = dict(log=lambda *_: None)


def test_run_succeeds_with_expected_warnings(loaded):
    _, outcome = loaded
    assert outcome.status == "success_with_warnings"
    assert outcome.exit_code == 0
    warned = {c.name for c in outcome.checks if c.status == "warn"}
    assert warned == {
        "clicks_not_above_impressions",
        "inferred_campaigns",
        "source_freshness_lag_days",
        "spend_spike_rows",
    }
    assert not [c for c in outcome.checks if c.status == "fail"]


def test_bad_rows_are_quarantined_with_reason(loaded):
    settings, _ = loaded
    reasons = sorted(r for (r,) in query(settings, "SELECT reject_reason FROM quarantine_media_rows"))
    assert reasons == ["invalid_date", "missing_campaign_id", "negative_metric", "unmapped_channel:radio"]


def test_rerun_is_idempotent(loaded):
    settings, _ = loaded
    before = query(settings, "SELECT count(*), sum(spend_eur) FROM fact_media_spend")[0]
    outcome = run(settings, **QUIET)
    after = query(settings, "SELECT count(*), sum(spend_eur) FROM fact_media_spend")[0]
    assert before == after
    assert query(settings, f"SELECT count(*) FROM ingestion_log WHERE run_id = '{outcome.run_id}'")[0][0] == 0


def test_restatement_replaces_original_delivery(loaded):
    settings, _ = loaded
    files = query(settings, """
        SELECT DISTINCT loaded_file_name FROM silver_media_spend
        WHERE source = 'agency_alpha' AND activity_date BETWEEN '2026-09-08' AND '2026-09-14'""")
    assert files == [("alpha_2026-09-08_2026-09-14_v2.csv",)]


def test_exact_duplicates_collapse(loaded):
    settings, _ = loaded
    dupes = query(settings, """
        SELECT count(*) FROM (
            SELECT source, date_key, campaign_id, channel, publisher
            FROM fact_media_spend GROUP BY ALL HAVING count(*) > 1)""")[0][0]
    assert dupes == 0


def test_late_file_clears_freshness_warning(loaded):
    settings, _ = loaded
    generate(settings.landing_dir, late=True)
    outcome = run(settings, **QUIET)
    freshness = next(c for c in outcome.checks if c.name == "source_freshness_lag_days")
    assert freshness.passed
    assert query(settings, "SELECT max(activity_date) FROM silver_media_spend WHERE source='agency_charlie'")[0][0].day == 30


def test_currency_converted_to_eur(loaded):
    settings, _ = loaded
    rows = query(settings, """
        SELECT DISTINCT currency, round(spend_eur / nullif(spend_local, 0), 2)
        FROM fact_media_spend WHERE spend_local > 100 ORDER BY 1""")
    assert rows == [("EUR", 1.0), ("GBP", 1.17), ("USD", 0.92)]


def test_totals_reconcile_across_layers(loaded):
    settings, _ = loaded
    silver, fact, mart = query(settings, """
        SELECT (SELECT sum(spend_eur) FROM silver_media_spend),
               (SELECT sum(spend_eur) FROM fact_media_spend),
               (SELECT sum(spend_eur) FROM mart_advertiser_monthly)""")[0]
    assert silver == fact == mart


def test_schema_drift_rejects_file_and_alerts(settings):
    generate(settings.landing_dir)
    bad = settings.landing_dir / "agency_alpha" / "alpha_2026-10-01_2026-10-07_v1.csv"
    with bad.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Date", "Campaign ID", "Channel", "Publisher", "Impressions", "Clicks"])  # no cost column
        w.writerow(["2026-10-01", "NL-1001", "TV", "NPO", "1000", "0"])
    messages = []
    outcome = run(settings, log=messages.append)
    status, message = query(settings, f"SELECT status, message FROM ingestion_log WHERE file_name = '{bad.name}'")[0]
    assert status == "rejected" and "Net Cost (EUR)" in message
    assert any("File rejected at ingest" in m for m in messages)
    assert outcome.exit_code == 0  # other files still load; the bad file is isolated


def test_report_is_written(loaded):
    _, outcome = loaded
    html = outcome.report.read_text()
    assert "Data quality checks" in html and "Quarantine by reason" in html
