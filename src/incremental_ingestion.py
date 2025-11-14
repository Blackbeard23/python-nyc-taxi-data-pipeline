import pandas as pd
from utils import db_connection, custom_logging
import time
import io

# terminate_db_connections()

logger = custom_logging('logs/pipeline.log')

year = 2024
# # out_dir = Path.cwd() / "data"
# out_dir.mkdir(parents=True, exist_ok=True)

download_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"


def download_url_template(year: int, month: int) -> str:
    """url for pytest
    """
    return download_url.format(year=year, month=month)


def incremental_data_ingestion(year: int, month: int, cur) -> None:
    """Download one month's parquet, load into raw_stage, and call incremental proc."""
    url = download_url.format(year=year, month=month)
    logger.info(f"Downloading and loading {url}")
    t4 = time.perf_counter()

    # 1) Download parquet into DataFrame
    df = pd.read_parquet(url, engine="pyarrow")
    t5 = time.perf_counter()
    logger.info(f'read_parquet: {t5 - t4:,.1f} seconds, rows={df.shape[0]:,}')

    try:
        cur.execute('DROP TABLE IF EXISTS raw_stage')
        cur.execute('CREATE TEMP TABLE IF NOT EXISTS raw_stage (LIKE bronze.yellow_taxi_raw)')

        # Stream DataFrame to raw_stage using COPY
        logger.info("  Copying DataFrame to raw_stage via COPY ...")
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, na_rep="")
        buffer.seek(0)

        t0 = time.perf_counter()
        cur.copy_expert(
            """
            COPY raw_stage
            FROM STDIN
            WITH (FORMAT csv, DELIMITER ',', NULL '')
            """,
            buffer,
        )

        t1 = time.perf_counter()
        logger.info(f"  COPY into raw_stage: {t1 - t0:,.1f} seconds")

        # Call the incremental procedure
        logger.info("  Calling bronze.incremental_load() ...")
        t2 = time.perf_counter()
        cur.execute("CALL bronze.incremental_load();")
        t3 = time.perf_counter()
        logger.info(f"  Procedure runtime (including SQL work): {t3 - t2:,.1f} seconds")

    except Exception as e:
        logger.exception(f"❌ Error during Ingestion: {e}")
        raise

    else:
        logger.info(f"""✅✅✅ Finished Incremental Ingestion for {year}-{month:02d}
                    Total Ingestion runtime: {t3 - t4:,.1f} seconds""")

    # finally:
    #     cur.close()


if __name__ == '__main__':
    def main() -> None:
        admin_conn = db_connection()
        try:
            incr_cur = admin_conn.cursor()
            for month in range(1, 2):
                incremental_data_ingestion(year, month, incr_cur)
        finally:
            incr_cur.close()
            # admin_conn.close()
            logger.info("All done.")

    main()
