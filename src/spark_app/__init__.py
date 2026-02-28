"""Package entry point for the Spark application."""

from .main import create_spark_session
from .nse_pipeline import main as run_nse_pipeline
from .transactions_pipeline import main as run_transactions_pipeline
__all__ = ["run", "create_spark_session"]
