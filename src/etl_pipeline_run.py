import time
from utils import custom_logging, db_connection
from incremental_ingestion import incremental_data_ingestion
from silver_gold_etl import run_gold_layer, run_silver_layer


logger = custom_logging('logs/pipeline.log')

year = 2024
month_start = 1
month_end = 3

try:
    start_pipeline = time.perf_counter()
    # connecting to database
    admin_conn = db_connection()
    admin_cur = admin_conn.cursor()

    for month in range(month_start, month_end + 1):
        incremental_data_ingestion(year, month, admin_cur)
    logger.info(f'📥 Incrementally Ingested {month_end} periodic files')

    run_silver_layer(admin_cur)

    run_gold_layer(admin_cur)
    end_pipeline = time.perf_counter()

except Exception as e:
    logger.exception(f"Error during end-to-end pipeline: {e}")

else:
    logger.info(f"🎊🎉🎊 Pipeline ran successfully for {end_pipeline - start_pipeline:.2f} seconds")

finally:
    admin_cur.close()
    admin_conn.close()
