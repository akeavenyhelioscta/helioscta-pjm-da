"""Model registry for saving/loading model artifacts."""
import json
import logging
from datetime import datetime
from pathlib import Path

from src.pjm_da_forecast.models.lightgbm_quantile import LightGBMQuantile

logger = logging.getLogger(__name__)

ARTIFACTS_DIR = Path(__file__).parent.parent.parent.parent / "artifacts"


def save_model(
    model: LightGBMQuantile,
    metrics: dict | None = None,
    run_id: str | None = None,
) -> Path:
    """Save a trained model and optional metrics.

    Args:
        model: Trained model instance.
        metrics: Optional evaluation metrics dict.
        run_id: Optional run identifier. Defaults to timestamp.

    Returns:
        Path to the saved model directory.
    """
    if run_id is None:
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    model_dir = ARTIFACTS_DIR / "models" / f"{model.name}_{run_id}"
    model.save(str(model_dir))

    if metrics:
        metrics_dir = ARTIFACTS_DIR / "metrics"
        metrics_dir.mkdir(parents=True, exist_ok=True)
        metrics_file = metrics_dir / f"{model.name}_{run_id}.json"
        with open(metrics_file, "w") as f:
            json.dump(metrics, f, indent=2, default=str)
        logger.info(f"Saved metrics to {metrics_file}")

    return model_dir


def load_model(model_name: str, run_id: str) -> LightGBMQuantile:
    """Load a specific model by name and run ID."""
    model_dir = ARTIFACTS_DIR / "models" / f"{model_name}_{run_id}"
    return LightGBMQuantile.load(str(model_dir))


def get_latest(model_name: str = "lgbm_quantile") -> LightGBMQuantile:
    """Load the most recently saved model by name."""
    models_dir = ARTIFACTS_DIR / "models"
    if not models_dir.exists():
        raise FileNotFoundError(f"No models directory at {models_dir}")

    matching = sorted(
        [d for d in models_dir.iterdir() if d.is_dir() and d.name.startswith(model_name)],
        key=lambda d: d.name,
    )
    if not matching:
        raise FileNotFoundError(f"No saved models found for '{model_name}'")

    latest = matching[-1]
    logger.info(f"Loading latest model: {latest.name}")
    return LightGBMQuantile.load(str(latest))


def list_models() -> list[dict]:
    """List all saved models."""
    models_dir = ARTIFACTS_DIR / "models"
    if not models_dir.exists():
        return []

    result = []
    for d in sorted(models_dir.iterdir()):
        if d.is_dir():
            result.append({
                "name": d.name,
                "path": str(d),
            })
    return result
