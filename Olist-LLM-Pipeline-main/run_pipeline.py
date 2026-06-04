"""
run_pipeline.py
Continuous Batch Pipeline Runner

Flow per batch:
  1. generate_data.py → push fresh rows to Snowflake (Bronze RAW tables)
  2. main.py pipeline  → Bronze Inspector → Silver → PII Masking → Gold

Usage:
    python run_pipeline.py                       # run once
    python run_pipeline.py --loop --interval 60  # run every 60s indefinitely
    python run_pipeline.py --loop --batches 5    # run exactly 5 batches
    python run_pipeline.py --rows 300            # 300 customers per batch
"""
import sys
import time
import argparse
import datetime
from config.logger import logger
from generate_data import run_batch
from main import main as run_pipeline


SEPARATOR = "=" * 70


def run_once(n_rows: int, batch_num: int = 1) -> dict:
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(SEPARATOR)
    logger.info(f"  CONTINUOUS PIPELINE | BATCH #{batch_num} | {ts}")
    logger.info(SEPARATOR)

    # Step 1: Generate fresh data → Snowflake Bronze
    logger.info("📦 STEP 1 | Generating fresh batch → Snowflake Bronze (RAW tables)")
    batch_id = run_batch(n_customers=n_rows)

    # Step 2: Run the full pipeline on the latest data
    logger.info(f"🚀 STEP 2 | Running pipeline on batch {batch_id} ...")
    state = run_pipeline()

    return state


def main():
    parser = argparse.ArgumentParser(description="Continuous Olist Pipeline Runner")
    parser.add_argument("--loop",     action="store_true", help="Run continuously in a loop")
    parser.add_argument("--interval", type=int, default=60, help="Seconds between batches (default: 60)")
    parser.add_argument("--batches",  type=int, default=0,  help="Number of batches to run (0 = infinite)")
    parser.add_argument("--rows",     type=int, default=200, help="Customers per batch (default: 200)")
    args = parser.parse_args()

    if not args.loop:
        # Single run
        run_once(n_rows=args.rows, batch_num=1)
        return

    batch_num = 0
    while True:
        batch_num += 1
        try:
            run_once(n_rows=args.rows, batch_num=batch_num)
        except KeyboardInterrupt:
            logger.info("\n⛔ Pipeline stopped by user.")
            sys.exit(0)
        except Exception as e:
            logger.error(f"Batch #{batch_num} failed: {e}. Continuing to next batch...")

        if args.batches > 0 and batch_num >= args.batches:
            logger.info(f"✅ Completed {batch_num} batch(es). Exiting.")
            break

        logger.info(f"⏱  Next batch in {args.interval}s... (Ctrl+C to stop)")
        try:
            time.sleep(args.interval)
        except KeyboardInterrupt:
            logger.info("\n⛔ Pipeline stopped by user.")
            sys.exit(0)


if __name__ == "__main__":
    main()
