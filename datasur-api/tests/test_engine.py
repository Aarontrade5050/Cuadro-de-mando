import datetime as dt

import polars as pl

from app.core.engine import TradeEngine


def _sample_df(include_transport: bool = True) -> pl.DataFrame:
    data = {
        "fecha": [dt.date(2024, 1, 1), dt.date(2024, 1, 20), dt.date(2024, 2, 1)],
        "company": ["Acme", "Acme", "Beta"],
        "value": [100.0, 200.0, 50.0],
        "partner": ["Chile", "Peru", "Chile"],
        "aduana": ["A", "B", "A"],
        "hs_code": [1010, 1010, 2020],
        "desc_aran": ["Prod A", "Prod A", "Prod B"],
    }
    if include_transport:
        data["via de transporte"] = ["Maritimo", "Aereo", "Maritimo"]
    return pl.DataFrame(data)


def test_scan_parquet_is_called(monkeypatch):
    called = {"value": False}

    def fake_scan_parquet(path):
        called["value"] = True
        assert path == "data/*.parquet"
        return _sample_df().lazy()

    monkeypatch.setattr("app.core.engine.pl.scan_parquet", fake_scan_parquet)

    report = TradeEngine().get_report("all", "exportaciones", "empresa", "acme")

    assert called["value"] is True
    assert report["total_fob"] == 300.0


def test_missing_transport_column_is_handled(monkeypatch):
    monkeypatch.setattr(
        "app.core.engine.pl.scan_parquet", lambda _: _sample_df(include_transport=False).lazy()
    )

    report = TradeEngine().get_report("all", "exportaciones", "empresa", "all")

    assert report["transport_ranking"] == []


def test_total_aggregation_matches_expected(monkeypatch):
    monkeypatch.setattr("app.core.engine.pl.scan_parquet", lambda _: _sample_df().lazy())

    report = TradeEngine().get_report("all", "exportaciones", "empresa", "acme")

    assert report["total_fob"] == 300.0
