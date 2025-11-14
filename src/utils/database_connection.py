import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg2


load_dotenv()

host = os.getenv('HOST')
user = os.getenv('USER')
port = os.getenv('PORT')
password = os.getenv('PASSWORD')
db_name = os.getenv('DBNAME').lower()


def db_connection():
    """ create database connection.
    """
    conn = psycopg2.connect(
        database=db_name,
        host=host,
        port=port,
        user=user,
        password=password
    )

    conn.autocommit = True

    # cursor = conn.cursor()

    return conn


def terminate_db_connections():
    """
    Kill all other connections to `dbname` so we can DROP DATABASE.
    Connects to the 'postgres' maintenance DB to do it.
    """
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db_name
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(
        """
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = %s
          AND pid <> pg_backend_pid();
        """,
        (db_name,),
    )

    cur.close()
    conn.close()


def get_sql_file_text(file_name: str) -> str:
    """convert .sql file to readable text file
        to execute as sql statement
    """
    file_path = Path(__file__).resolve().parent.parent
    sql_path = file_path / "sql" / file_name
    return sql_path


def run_sql_file(path: Path, cursor):
    """ Helps to run .sql file
    """
    sql_text = path.read_text(encoding='utf-8')
    cursor.execute(sql_text)
