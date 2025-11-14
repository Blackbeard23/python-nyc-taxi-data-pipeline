# from pathlib import Path
import os
from dotenv import load_dotenv
import psycopg2
from utils import custom_logging, run_sql_file, get_sql_file_text
from utils import terminate_db_connections

# Logging setup
logger = custom_logging('logs/db_setup.log')


# ---------------------------------
# Env + basic DB config
# ---------------------------------
load_dotenv()

host = os.getenv("HOST")
user = os.getenv("USER")
port = os.getenv("PORT")
password = os.getenv("PASSWORD")
db_name = os.getenv("DBNAME").lower()

terminate_db_connections()


def database_connection(host, port, user, password, db=None):
    """
    Simple psycopg2 connection used for DDL and DB create/drop.
    If db is None, connects to the default database (usually 'postgres').
    """
    conn = psycopg2.connect(
        database=db,
        host=host,
        port=port,
        user=user,
        password=password,
    )
    conn.autocommit = True
    cursor = conn.cursor()
    return cursor, conn


# def run_sql_file(path: Path, cursor) -> None:
#     """Read a .sql file and execute its contents using a psycopg2 cursor."""
#     sql = path.read_text(encoding="utf-8")
#     cursor.execute(sql)


def main() -> None:
    try:
        # -----------------------------
        # 1. Create (or recreate) DB
        # -----------------------------
        admin_cur, admin_conn = database_connection(
            host=host, port=port, user=user, password=password, db=None
        )

        # Drop/create using the env DBNAME (unquoted -> folded to lowercase)
        admin_cur.execute(f"DROP DATABASE IF EXISTS {db_name} WITH (FORCE)")
        admin_cur.execute(f"CREATE DATABASE {db_name}")
        logger.info("✅ Database %s was created successfully", db_name)

        admin_cur.close()
        admin_conn.close()

        # -----------------------------
        # 2. Connect to the new DB
        # -----------------------------
        db_cur, db_conn = database_connection(
            host=host, port=port, user=user, password=password, db=db_name
        )

        # -----------------------------
        # 3. BRONZE schema & table
        # -----------------------------
        db_cur.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        db_cur.execute("DROP TABLE IF EXISTS bronze.yellow_taxi_raw")
        db_cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze.yellow_taxi_raw (
                vendorid integer,
                tpep_pickup_datetime timestamp,
                tpep_dropoff_datetime timestamp,
                passenger_count text,
                trip_distance NUMERIC,
                ratecodeid text,
                store_and_fwd_flag text,
                pulocationid integer,
                dolocationid integer,
                payment_type integer,
                fare_amount NUMERIC,
                extra NUMERIC,
                mta_tax NUMERIC,
                tip_amount NUMERIC,
                tolls_amount NUMERIC,
                improvement_surcharge NUMERIC,
                total_amount NUMERIC,
                congestion_surcharge NUMERIC,
                airport_fee NUMERIC
            )
            """
        )
        logger.info("✅ BRONZE schema and layer were created successfully")

        # -----------------------------
        # 4. META schema: metadata + invalid_records
        # -----------------------------
        db_cur.execute("CREATE SCHEMA IF NOT EXISTS meta")
        db_cur.execute("DROP TYPE IF EXISTS meta.status_enum CASCADE")
        db_cur.execute("CREATE TYPE meta.status_enum AS ENUM ('success','failed')")

        db_cur.execute("DROP TABLE IF EXISTS meta.metadata_table")
        db_cur.execute(
            """
            CREATE TABLE IF NOT EXISTS meta.metadata_table (
                last_load_date timestamp,
                status meta.status_enum,
                runtime INTERVAL,
                error_message text
            )
            """
        )

        db_cur.execute("DROP TABLE IF EXISTS meta.invalid_records")
        db_cur.execute(
            """
            CREATE TABLE IF NOT EXISTS meta.invalid_records (
                LIKE bronze.yellow_taxi_raw,
                PRIMARY KEY (
                    vendorid,
                    tpep_pickup_datetime,
                    tpep_dropoff_datetime,
                    trip_distance,
                    pulocationid,
                    dolocationid,
                    total_amount
                )
            )
            """
        )
        logger.info("✅ META schema and layers were created successfully")

        # -----------------------------
        # 5. SILVER schema + dimension tables + partitioned fact
        # -----------------------------
        db_cur.execute("CREATE SCHEMA IF NOT EXISTS silver")

        db_cur.execute("DROP TABLE IF EXISTS silver.vendor")
        db_cur.execute(
            """
            CREATE TABLE IF NOT EXISTS silver.vendor (
                vendorid integer,
                vendor text,
                PRIMARY KEY (vendorid)
            )
            """
        )

        db_cur.execute("DROP TABLE IF EXISTS silver.payment_type")
        db_cur.execute(
            """
            CREATE TABLE IF NOT EXISTS silver.payment_type (
                payment_type_id integer,
                payment_type text,
                PRIMARY KEY (payment_type_id)
            )
            """
        )

        db_cur.execute("DROP TABLE IF EXISTS silver.ratecode")
        db_cur.execute(
            """
            CREATE TABLE IF NOT EXISTS silver.ratecode (
                ratecodeid integer,
                rate text,
                PRIMARY KEY (ratecodeid)
            )
            """
        )

        db_cur.execute("DROP TABLE IF EXISTS silver.yellow_taxi")
        db_cur.execute(
            """
            CREATE TABLE IF NOT EXISTS silver.yellow_taxi (
                vendorid integer REFERENCES silver.vendor (vendorid),
                tpep_pickup_datetime timestamp,
                tpep_dropoff_datetime timestamp,
                minute_duration integer,
                passenger_count text,
                trip_distance NUMERIC,
                ratecodeid integer REFERENCES silver.ratecode (ratecodeid),
                store_and_fwd_flag text,
                pulocationid integer,
                dolocationid integer,
                payment_type integer REFERENCES silver.payment_type (payment_type_id),
                fare_amount NUMERIC,
                extra NUMERIC,
                mta_tax NUMERIC,
                tip_amount NUMERIC,
                tolls_amount NUMERIC,
                improvement_surcharge NUMERIC,
                total_amount NUMERIC,
                congestion_surcharge NUMERIC,
                airport_fee NUMERIC
            )
            PARTITION BY RANGE (tpep_pickup_datetime)
            """
        )

        partitions = [
            """
            CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_01
            PARTITION OF silver.yellow_taxi
            FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')
            """,
            """
            CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_02
            PARTITION OF silver.yellow_taxi
            FOR VALUES FROM ('2024-02-01') TO ('2024-03-01')
            """,
            """
            CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_03
            PARTITION OF silver.yellow_taxi
            FOR VALUES FROM ('2024-03-01') TO ('2024-04-01')
            """,
            """
            CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_04
            PARTITION OF silver.yellow_taxi
            FOR VALUES FROM ('2024-04-01') TO ('2024-05-01')
            """,
            """
            CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_05
            PARTITION OF silver.yellow_taxi
            FOR VALUES FROM ('2024-05-01') TO ('2024-06-01')
            """,
            """
            CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_06
            PARTITION OF silver.yellow_taxi
            FOR VALUES FROM ('2024-06-01') TO ('2024-07-01')
            """,
            """
            CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_07
            PARTITION OF silver.yellow_taxi
            FOR VALUES FROM ('2024-07-01') TO ('2024-08-01')
            """,
            """
            CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_08
            PARTITION OF silver.yellow_taxi
            FOR VALUES FROM ('2024-08-01') TO ('2024-09-01')
            """,
            """
            CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_09
            PARTITION OF silver.yellow_taxi
            FOR VALUES FROM ('2024-09-01') TO ('2024-10-01')
            """,
            """
            CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_10
            PARTITION OF silver.yellow_taxi
            FOR VALUES FROM ('2024-10-01') TO ('2024-11-01')
            """,
            """
            CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_11
            PARTITION OF silver.yellow_taxi
            FOR VALUES FROM ('2024-11-01') TO ('2024-12-01')
            """,
            """
            CREATE TABLE IF NOT EXISTS silver.yellow_taxi_p_2024_12
            PARTITION OF silver.yellow_taxi
            FOR VALUES FROM ('2024-12-01') TO ('2025-01-01')
            """
        ]

        for stmt in partitions:
            db_cur.execute(stmt)

        logger.info("✅ SILVER table partitions were created successfully")

        # -----------------------------
        # 6. GOLD schema
        # -----------------------------
        db_cur.execute("CREATE SCHEMA IF NOT EXISTS gold")
        logger.info("✅ GOLD schema and layer were created successfully")

        # -----------------------------
        # 7. Apply stored procedure from src/sql/bronze_incremental_load.sql
        # -----------------------------
        # db_setup.py is in src/, so parent is src/
        # src_dir = Path(__file__).resolve().parent
        # sql_file = src_dir / "sql" / "bronze_incremental_load.sql"
        sql_file = get_sql_file_text('bronze_incremental_load.sql')

        logger.info("Applying stored procedure from %s", sql_file)
        run_sql_file(sql_file, db_cur)
        logger.info("✅ bronze.incremental_load created/updated successfully.")

        db_cur.close()
        db_conn.close()

        logger.info("✅✅✅ DATABASE SETUP COMPLETED")

    except Exception as e:
        logger.exception("Error during DB setup: %s", e)
        try:
            db_cur.close()
            db_conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
