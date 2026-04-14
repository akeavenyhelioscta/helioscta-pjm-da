"""Adapter for like-day forecast pipeline."""
from __future__ import annotations

from dataclasses import asdict
from datetime import date

import pandas as pd

from src.backtesting.adapters.base import ForecastAdapter, ForecastResult
from src.like_day_forecast import configs as ld_configs
from src.like_day_forecast.pipelines.forecast import run as run_like_day


def _parse_quantile_label(label: str) -> float | None:
    label = str(label).strip().upper()
    if not label.startswith("P"):
        return None
    try:
        return int(label[1:]) / 100.0
    except ValueError:
        return None


class LikeDayAdapter(ForecastAdapter):
    """Runs like-day pipeline and returns normalized hourly outputs."""

    name = "like_day"

    def __init__(
        self,
        quantiles: list[float],
        base_config: ld_configs.ScenarioConfig | None = None,
        cache_dir=ld_configs.CACHE_DIR,
        cache_enabled: bool = ld_configs.CACHE_ENABLED,
        cache_ttl_hours: float = ld_configs.CACHE_TTL_HOURS,
        force_refresh: bool = ld_configs.FORCE_CACHE_REFRESH,
    ) -> None:
        self.quantiles = sorted(quantiles)
        self.base_config = base_config or ld_configs.ScenarioConfig()
        self.cache_dir = cache_dir
        self.cache_enabled = cache_enabled
        self.cache_ttl_hours = cache_ttl_hours
        self.force_refresh = force_refresh

    def _build_config(self, forecast_date: date) -> ld_configs.ScenarioConfig:
        cfg_dict = asdict(self.base_config)
        cfg_dict["forecast_date"] = str(forecast_date)
        cfg_dict["quantiles"] = list(self.quantiles)
        return ld_configs.ScenarioConfig(**cfg_dict)

    def forecast_for_date(
        self,
        forecast_date: date,
        force_retrain: bool = False,
        df_features: pd.DataFrame | None = None,
    ) -> ForecastResult:
        # force_retrain not used for like-day; kept for interface parity.
        _ = force_retrain
        config = self._build_config(forecast_date)
        result = run_like_day(
            forecast_date=str(forecast_date),
            config=config,
            cache_dir=self.cache_dir,
            cache_enabled=self.cache_enabled,
            cache_ttl_hours=self.cache_ttl_hours,
            force_refresh=self.force_refresh,
            df_features=df_features,
        )
        if "error" in result:
            raise RuntimeError(result["error"])

        output_table: pd.DataFrame = result["output_table"]
        quantiles_table: pd.DataFrame = result["quantiles_table"]
        fc_rows = output_table[output_table["Type"] == "Forecast"]
        if len(fc_rows) == 0:
            raise RuntimeError(f"{self.name}: missing Forecast row for {forecast_date}")
        fc = fc_rows.iloc[0]

        point_by_he: dict[int, float] = {}
        for h in range(1, 25):
            val = fc.get(f"HE{h}")
            if pd.notna(val):
                point_by_he[h] = float(val)

        quantiles_by_he: dict[int, dict[float, float]] = {h: {} for h in range(1, 25)}
        for _, qrow in quantiles_table.iterrows():
            q = _parse_quantile_label(qrow.get("Type"))
            if q is None:
                continue
            for h in range(1, 25):
                val = qrow.get(f"HE{h}")
                if pd.notna(val):
                    quantiles_by_he[h][q] = float(val)

        return ForecastResult(
            model=self.name,
            forecast_date=forecast_date,
            reference_date=pd.to_datetime(result["reference_date"]).date(),
            point_by_he=point_by_he,
            quantiles_by_he=quantiles_by_he,
            metadata={
                "n_analogs_used": result.get("n_analogs_used"),
                "has_actuals_pipeline": result.get("has_actuals"),
            },
        )
