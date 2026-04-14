"""Adapter for LASSO quantile regression forecast pipeline."""
from __future__ import annotations

from dataclasses import asdict
from datetime import date

import pandas as pd

from src.backtesting.adapters.base import ForecastAdapter, ForecastResult
from src.lasso_quantile_regression.configs import LassoQRConfig
from src.lasso_quantile_regression.pipelines.forecast import run as run_lasso_qr


def _parse_quantile_label(label: str) -> float | None:
    label = str(label).strip().upper()
    if not label.startswith("P"):
        return None
    try:
        return int(label[1:]) / 100.0
    except ValueError:
        return None


class LassoQRAdapter(ForecastAdapter):
    """Runs LASSO QR pipeline and returns normalized hourly outputs."""

    name = "lasso_qr"

    def __init__(
        self,
        quantiles: list[float],
        base_config: LassoQRConfig | None = None,
    ) -> None:
        self.quantiles = sorted(quantiles)
        self.base_config = base_config or LassoQRConfig()

    def _build_config(self, forecast_date: date, force_retrain: bool) -> LassoQRConfig:
        cfg_dict = asdict(self.base_config)
        cfg_dict["forecast_date"] = str(forecast_date)
        cfg_dict["quantiles"] = list(self.quantiles)
        if force_retrain:
            # Backtesting strict mode: always retrain with train_end=forecast_date-1.
            cfg_dict["retrain_if_stale_hours"] = 0
        return LassoQRConfig(**cfg_dict)

    def forecast_for_date(
        self,
        forecast_date: date,
        force_retrain: bool = False,
        df_features: pd.DataFrame | None = None,
    ) -> ForecastResult:
        config = self._build_config(forecast_date, force_retrain=force_retrain)
        result = run_lasso_qr(config=config, df_features=df_features)
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
            metadata=result.get("model_info", {}),
        )
