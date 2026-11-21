"""Run the whole pipeline in the correct order with one command.

    python run_pipeline.py                 # download fresh data + process
    python run_pipeline.py --skip-ingest   # use the raw files already in storage

Order: ingest -> stage -> validate staged -> build curated
       -> validate curated -> publish.
If an ERROR-level check fails, the pipeline stops BEFORE publishing,
so the previously published data product stays untouched.
"""

import argparse
import logging
import sys
import time
import config
import ingest
import transform
import validate
from common import get_spark, setup_logging

log = logging.getLogger("pipeline")


def step(number, name, function, *args):
    log.info("===== STEP %d: %s =====", number, name)
    started = time.time()
    result = function(*args)
    log.info("----- STEP %d OK (%.1fs) -----", number, time.time() - started)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-ingest", action="store_true",
                        help="do not download; reuse the existing raw files")
    args = parser.parse_args()

    setup_logging()
    log.info("Pipeline started | storage: %s | period %s..%s | %d countries",
             config.DATA_ROOT, config.START_MONTH, config.END_MONTH, len(config.COUNTRIES))
    try:
        if args.skip_ingest:
            log.info("===== STEP 1: ingest SKIPPED (using existing raw files) =====")
        else:
            step(1, "ingest (raw)", ingest.run)

        spark = get_spark()
        step(2, "stage (raw -> staged)", transform.stage, spark)
        step(3, "validate staged", validate.validate_staged,
             spark.read.parquet(config.STAGED_TOURISM),
             spark.read.parquet(config.STAGED_WEATHER))
        curated = step(4, "build curated (join + aggregate)",
                       transform.build_curated, spark).cache()
        step(5, "validate curated", validate.validate_curated, curated)
        step(6, "publish curated", transform.publish, curated)
    except validate.DataQualityError as exc:
        log.error("PIPELINE STOPPED - data quality: %s", exc)
        log.error("The curated data product was NOT updated.")
        return 1
    except Exception:
        log.exception("PIPELINE FAILED")
        return 1

    log.info("Pipeline finished successfully. Sample of the data product:")
    curated.filter("country_code = 'HU' AND year = 2024").show(12, truncate=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())