from utils import *
from incremental_ingestion import *
from silver_gold_etl import *
import time

logger = custom_logging('logs/pipeline.log')

year = 2024
month_start = 1
month_end = 5

try:
    start_pipeline = time.perf_counter()
    # connecting to database
    admin_conn = db_connection()
    admin_cur = admin_conn.cursor()

    for month in range(month_start, month_end+1):
        incremental_data_ingestion(year, month, admin_cur)

    run_silver_layer(admin_cur)

    run_gold_layer(admin_cur)

except Exception as e:
    logger.exception(f"Error during end-to-end pipeline: {e}")

else:
    logger.info("🎊🎉🎊 Pipeline ran successfully")

finally:
    admin_cur.close()
    admin_conn.close()


        