"""Package entry point for the Spark ETL application."""

from .nse_pipeline import main as run_nse_pipeline
from .transactions_pipeline import main as run_transactions_pipeline

__all__ = [
    "run_nse_pipeline",
    "run_transactions_pipeline",
]
