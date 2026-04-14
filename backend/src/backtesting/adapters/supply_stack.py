"""Adapter for supply stack forecast pipeline."""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from src.backtesting.adapters.base import ForecastAdapter, ForecastResult
from src.supply_stack_model.configs import SupplyStackConfig
from src.supply_stack_model.pipelines.forecast import run as run_supply_stack


def _parse_quantile_col(col: str) -> float | None:
    """Extract quantile level from column name like 'q_0.10'."""
    if not col.startswith("q_"):
        return None
    try:
        return float(col[2:])
    except ValueError:
        return None


class SupplyStackAdapter(ForecastAdapter):
    """Runs supply stack pipeline and returns normalized hourly outputs."""

    name = "supply_stack"

    def __init__(
        self,
        quantiles: list[float],
        base_config: SupplyStackConfig | None = None,
    ) -> None:
        self.quantiles = sorted(quantiles)
        self.base_config = base_config or SupplyStackConfig()

    def _build_config(self, forecast_date: date) -> SupplyStackConfig:
        return SupplyStackConfig(
            forecast_date=forecast_date,
            schema=self.base_config.schema,
            hub=self.base_config.hub,
            region=self.base_config.region,
            region_preset=self.base_config.region_preset,
            gas_hub_col=self.base_config.gas_hub_col,
            outage_column=self.base_config.outage_column,
            outages_lookback_days=self.base_config.outages_lookback_days,
            congestion_adder_usd=self.base_config.congestion_adder_usd,
            coal_price_usd_mmbtu=self.base_config.coal_price_usd_mmbtu,
            oil_price_usd_mmbtu=self.base_config.oil_price_usd_mmbtu,
            scarcity_price_cap_usd_mwh=self.base_config.scarcity_price_cap_usd_mwh,
            quantiles=list(self.quantiles),
            n_monte_carlo_draws=self.base_config.n_monte_carlo_draws,
            monte_carlo_seed=self.base_config.monte_carlo_seed,
            net_load_error_std_pct=self.base_config.net_load_error_std_pct,
            gas_price_error_std_pct=self.base_config.gas_price_error_std_pct,
            outage_error_std_pct=self.base_config.outage_error_std_pct,
            fleet_csv_path=self.base_config.fleet_csv_path,
        )

    def forecast_for_date(
        self,
        forecast_date: date,
        force_retrain: bool = False,
        df_features: pd.DataFrame | None = None,
    ) -> ForecastResult:
        # Deterministic model — force_retrain and df_features are no-ops.
        _ = force_retrain
        _ = df_features

        config = self._build_config(forecast_date)
        result = run_supply_stack(config=config)
        if "error" in result:
            raise RuntimeError(result["error"])

        df_forecast: pd.DataFrame = result["df_forecast"]

        point_by_he: dict[int, float] = {}
        for _, row in df_forecast.iterrows():
            he = int(row["hour_ending"])
            val = row.get("point_forecast")
            if pd.notna(val):
                point_by_he[he] = float(val)

        quantiles_by_he: dict[int, dict[float, float]] = {h: {} for h in range(1, 25)}
        q_cols = [c for c in df_forecast.columns if c.startswith("q_")]
        for _, row in df_forecast.iterrows():
            he = int(row["hour_ending"])
            for qc in q_cols:
                q = _parse_quantile_col(qc)
                if q is None:
                    continue
                val = row.get(qc)
                if pd.notna(val):
                    quantiles_by_he[he][q] = float(val)

        reference_date = pd.to_datetime(result["reference_date"]).date()

        return ForecastResult(
            model=self.name,
            forecast_date=forecast_date,
            reference_date=reference_date,
            point_by_he=point_by_he,
            quantiles_by_he=quantiles_by_he,
            metadata={
                "region": config.region,
                "region_preset": config.region_preset,
                "hub": config.hub,
                "congestion_adder_usd": config.congestion_adder_usd,
            },
        )
