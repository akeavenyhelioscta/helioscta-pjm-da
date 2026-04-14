from __future__ import annotations

from datetime import date

from src.utils.day_type import (
    DAY_TYPE_SATURDAY,
    DAY_TYPE_SUNDAY,
    DAY_TYPE_WEEKDAY,
    resolve_day_type,
)
from src.like_day_forecast.configs import ScenarioConfig
from src.lasso_quantile_regression.configs import LassoQRConfig
from src.lasso_quantile_regression.training.trainer import _artifact_filename
from src.lightgbm_quantile.configs import LGBMQRConfig
from src.lightgbm_quantile.training.trainer import _artifact_filename as lgbm_artifact_filename


def test_resolve_day_type() -> None:
    assert resolve_day_type(date(2026, 4, 13)) == DAY_TYPE_WEEKDAY  # Monday
    assert resolve_day_type(date(2026, 4, 11)) == DAY_TYPE_SATURDAY
    assert resolve_day_type(date(2026, 4, 12)) == DAY_TYPE_SUNDAY


def test_like_day_applies_saturday_profile() -> None:
    cfg = ScenarioConfig()
    sat_cfg, day_type = cfg.with_day_type_overrides(date(2026, 4, 11))

    assert day_type == DAY_TYPE_SATURDAY
    assert sat_cfg.same_dow_group is True
    assert sat_cfg.n_analogs == 12
    assert sat_cfg.resolved_weights()["lmp_profile"] == 0.25
    assert sat_cfg.resolved_weights()["target_renewable_level"] == 3.25


def test_lasso_applies_sunday_profile_and_tags_artifacts() -> None:
    cfg = LassoQRConfig()
    sun_cfg, day_type = cfg.with_day_type_overrides(date(2026, 4, 12))

    assert day_type == DAY_TYPE_SUNDAY
    assert sun_cfg.day_type_tag == DAY_TYPE_SUNDAY
    assert sun_cfg.alpha == 0.1
    assert sun_cfg.include_lagged_lmp is False

    fname = _artifact_filename(date(2026, 4, 11), sun_cfg.day_type_tag)
    assert fname == "lasso_qr_2026-04-11_sunday.joblib"


def test_lgbm_applies_saturday_profile_and_tags_artifacts() -> None:
    cfg = LGBMQRConfig()
    sat_cfg, day_type = cfg.with_day_type_overrides(date(2026, 4, 11))

    assert day_type == DAY_TYPE_SATURDAY
    assert sat_cfg.day_type_tag == DAY_TYPE_SATURDAY
    assert sat_cfg.include_lagged_lmp is False

    fname = lgbm_artifact_filename(date(2026, 4, 10), sat_cfg.day_type_tag)
    assert fname == "lgbm_qr_2026-04-10_saturday.joblib"


def test_lgbm_weekday_profile_preserves_defaults() -> None:
    cfg = LGBMQRConfig()
    wd_cfg, day_type = cfg.with_day_type_overrides(date(2026, 4, 13))  # Monday

    assert day_type == DAY_TYPE_WEEKDAY
    assert wd_cfg.day_type_tag == DAY_TYPE_WEEKDAY
    assert wd_cfg.include_lagged_lmp == cfg.include_lagged_lmp
    assert wd_cfg.n_estimators == cfg.n_estimators

