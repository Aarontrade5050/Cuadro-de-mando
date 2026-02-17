from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl


@dataclass(slots=True)
class TradeEngine:
    """Business logic for trade intelligence reports."""

    parquet_glob: str = "data/*.parquet"

    def get_report(
        self,
        pais: str,
        operacion: str,
        categoria: str,
        filtro: str,
    ) -> dict[str, Any]:
        """Build a report using lazy parquet scans and aggregated metrics."""
        lf = pl.scan_parquet(self.parquet_glob)

        if pais and pais.lower() not in {"all", "*"} and "partner" in lf.collect_schema().names():
            lf = lf.filter(pl.col("partner").str.to_lowercase() == pais.lower())

        categoria_norm = (categoria or "").strip().lower()
        filtro_norm = (filtro or "").strip().lower()

        if filtro_norm and filtro_norm not in {"all", "*"}:
            if categoria_norm in {"empresa", "company"}:
                lf = lf.filter(
                    pl.col("company")
                    .cast(pl.Utf8)
                    .str.to_lowercase()
                    .str.contains(filtro_norm, literal=True)
                )
            elif categoria_norm in {"producto", "product", "hs_code"}:
                lf = lf.filter(
                    pl.col("hs_code")
                    .cast(pl.Utf8)
                    .str.to_lowercase()
                    .str.contains(filtro_norm, literal=True)
                )

        total_value = (
            lf.select(pl.col("value").sum().alias("total_fob")).collect().item(0, 0) or 0.0
        )

        partner_ranking = (
            lf.group_by("partner")
            .agg(pl.col("value").sum().alias("value"))
            .sort("value", descending=True)
            .limit(10)
            .collect()
            .to_dicts()
        )

        try:
            transport_ranking = (
                lf.group_by("via de transporte")
                .agg(pl.col("value").sum().alias("value"))
                .sort("value", descending=True)
                .collect()
                .to_dicts()
            )
        except pl.exceptions.ColumnNotFoundError:
            transport_ranking = []

        monthly_evolution = (
            lf.with_columns(pl.col("fecha").dt.truncate("1mo").alias("month"))
            .group_by("month")
            .agg(pl.col("value").sum().alias("value"))
            .sort("month")
            .collect()
            .with_columns(pl.col("month").cast(pl.Utf8))
            .to_dicts()
        )

        return {
            "meta": {
                "pais": pais,
                "operacion": operacion,
                "categoria": categoria,
                "filtro": filtro,
            },
            "total_fob": float(total_value),
            "partner_ranking": partner_ranking,
            "transport_ranking": transport_ranking,
            "monthly_evolution": monthly_evolution,
        }
