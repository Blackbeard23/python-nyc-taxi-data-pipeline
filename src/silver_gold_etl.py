from utils import *

logger = custom_logging('logs/pipeline.log')

def run_silver_layer(admin_cur):
    try:
        # silver_conn = db_connection()

        silver_path = get_sql_file_text('silver_full_refresh_transformation.sql')

        run_sql_file(silver_path, admin_cur)

    except Exception as e:
        logger.exception(f"❌ Error during Silver Transformation Layer: {e}")
        raise

    else:
        logger.info("""✅✅✅ SILVER layer transformation completed: raw Bronze trips normalized into relational tables.
                    derived features calculated, and values standardized for downstream Gold models.
                    """)

    # finally:
        # silver_conn.close()


def run_gold_layer(admin_cur):
    try:

        gold_path = get_sql_file_text('gold_aggregate_layer.sql')

        run_sql_file(gold_path, admin_cur)

    except Exception as e:
        logger.exception(f"❌ Error during Gold aggregation Layer: {e}")
        raise

    else:
        logger.info("✅✅✅ Aggregation layer model completed.")

    # finally:
    #     gold_conn.close()

if __name__ == '__main__':
    try:
        admin_conn = db_connection()
        sg_cursor = admin_conn.cursor()
        run_silver_layer(sg_cursor)
        run_gold_layer(sg_cursor)
    finally:
        sg_cursor.close()
        admin_conn.close()
