from datetime import datetime, timedelta

import logging
logging.basicConfig(level=logging.DEBUG)

SCHEMA: str = "dbt_pjm_v1_2026_feb_19"
HUB: str = "WESTERN HUB"

DATE_COL: str = "date"
HOUR_ENDING_COL: str = "hour_ending"

TARGET_DATE: datetime = datetime.now().date() + timedelta(days=1)

TARGET_COL: str = "lmp_total"

FEATURE_COLS: list[str] = [
    "lmp_total",
    "lmp_system_energy_price",
    "lmp_congestion_price",
    "lmp_marginal_loss_price",
]
