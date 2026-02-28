"""
Command‑line entry point for the Spark ETL application.
Allows selecting which pipeline to run: NSE or Transactions.
"""

import argparse
import logging
import sys
from spark_app.nse_pipeline import main as run_nse_pipeline
from spark_app.transactions_pipeline import main as run_transactions_pipeline

def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run Spark ETL pipelines")

    parser.add_argument(
        "--pipeline",
        required=True,
        choices=["nse", "transactions", "show_nse", "show_transactions"],
        help="Pipeline to run: 'nse' or 'transactions' or 'show'"
    )

    return parser.parse_args(argv)


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO)
    args = parse_args(argv)

    try:
        if args.pipeline == "nse":
            run_nse_pipeline()
            logging.info("Pipeline finished successfully")
        elif args.pipeline == "transactions":
            run_transactions_pipeline()
            logging.info("Pipeline finished successfully")
        elif args.pipeline == "show_nse": 
            from spark_app.display_data import main as show_data
            show_data("/app/src/data/bronze/nse_id")
        elif args.pipeline == "show_transactions":
            from spark_app.display_data import main as show_data
            show_data("/app/src/data/silver/transactions")

        
        return 0

    except Exception:
        logging.exception("Failure during execution")
        return 1


if __name__ == "__main__":
    sys.exit(main())
