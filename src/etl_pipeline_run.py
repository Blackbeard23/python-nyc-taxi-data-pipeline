import time
from utils import custom_logging, db_connection
from incremental_ingestion import incremental_data_ingestion
from silver_gold_etl import run_gold_layer, run_silver_layer


logger = custom_logging('logs/pipeline.log')

year = 2024
month_start = 1
month_end = 12

try:
    start_pipeline = time.perf_counter()
    # connecting to database
    admin_conn = db_connection()
    admin_cur = admin_conn.cursor()

    for month in range(month_start, month_end + 1):
        incremental_data_ingestion(year, month, admin_cur)
    logger.info(f'📥 Incrementally Ingested {month_end} periodic files')

    silver_start = time.perf_counter()
    run_silver_layer(admin_cur)
    silver_end = time.perf_counter()
    logger.info(f'Silver transformation layer ran for {silver_end - silver_start} seconds')

    gold_start = time.perf_counter()
    run_gold_layer(admin_cur)
    gold_end = time.perf_counter()
    logger.info(f'Gold aggregation layer ran for {gold_end - gold_start} seconds')

    end_pipeline = time.perf_counter()

except Exception as e:
    logger.exception(f"Error during end-to-end pipeline: {e}")

else:
    logger.info(f"🎊🎉🎊 Pipeline ran successfully for {end_pipeline - start_pipeline:.2f} seconds")

finally:
    admin_cur.close()
    admin_conn.close()
