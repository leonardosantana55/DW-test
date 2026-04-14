import psycopg2
from psycopg2 import sql
import os
import csv
from credentials.postgres import *
import io
import ftfy

# ----------------------------------------------------------------------
# Utilities
# ----------------------------------------------------------------------
def sanitize_column(name: str) -> str:
    """
    Clean column names for use in PostgreSQL identifiers.
    Converts to snake_case, removes BOM, trims whitespace.
    """
    return (
        name.replace("\ufeff", "")
            .strip()
            .replace(" ", "_")
            .replace("-", "_")
            .lower()
    )


def create_staging_table(cursor, table_name, header):
    """
    Creates a staging table with VARCHAR columns using sanitized headers.
    """
    sanitized_columns = [
        sql.SQL("{} VARCHAR").format(sql.Identifier(sanitize_column(col)))
        for col in header
    ]

    query = sql.SQL("""
        CREATE SCHEMA IF NOT EXISTS staging;
        DROP TABLE IF EXISTS staging.{table};
        CREATE TABLE IF NOT EXISTS staging.{table} ( {cols} );
    """).format(
        table=sql.Identifier(table_name),
        cols=sql.SQL(", ").join(sanitized_columns)
    )
    cursor.execute(query)

def clean_csv(filepath):
    with open(filepath, "r", encoding="utf8") as f:
        raw = f.read()
    
    clean = ftfy.fix_text(raw)
    return io.StringIO(clean)

def copy_into_staging(cursor, table_name, filepath):
    """
    Uses COPY FROM STDIN to load CSV into the staging table.
    """
    copy_sql = sql.SQL("""
        COPY staging.{} FROM STDIN WITH (
            FORMAT csv,
            HEADER true,
            DELIMITER ','
        )
    """).format(sql.Identifier(table_name))
    cursor.copy_expert(copy_sql, clean_csv(filepath))


# ----------------------------------------------------------------------
# Main ETL logic
# ----------------------------------------------------------------------
def load_csv_file(filepath, table_name, conn):
    """
    Load a single CSV file into a staging table.
    """
    cursor = conn.cursor()

    # Read header first (to generate table schema if needed)
    with open(filepath, encoding="utf8") as f:
        reader = csv.reader(f)
        header = next(reader)

    try:
        print(f"[INFO] Creating staging table: {table_name}")
        create_staging_table(cursor, table_name, header)

        print(f"[INFO] Copying data from {filepath} into {table_name}")
        copy_into_staging(cursor, table_name, filepath)

        conn.commit()
        print(f"[OK] {filepath} Loaded\n")

    except Exception as e:
        conn.rollback()  # ← Critical: reset the aborted transaction
        print(f"[ERROR] Failed to load {filepath}: {e}")
        raise  # ← Re-raise so you actually see what went wrong

# ----------------------------------------------------------------------
# Entrypoint
# ----------------------------------------------------------------------
def main():
    import glob

    path = "./data"
    files = glob.glob(path + "/**/*.csv", recursive=True)

    # Connect once for the entire batch
    conn = psycopg2.connect(**DB_CONFIG)
    conn.set_client_encoding("UTF8")

    for f in files:
        table_name = os.path.splitext(os.path.basename(f))[0].lower()
        load_csv_file(f, table_name, conn)

    conn.close()
    print("All files successfully loaded.")

if __name__ == "__main__":
    main();
