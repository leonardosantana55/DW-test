import psycopg2
from pathlib import Path
from credentials.postgres import *

from el.staging_insert_csv import main as insert_csv
from el.staging_insert_ibge import main as insert_ibge


def run_sql_file(cursor, path):
    print(f"Running {path.name}")
    with open(path, encoding="utf-8") as f:
        cursor.execute(f.read())

def modeling(items):
    #estavbelecer uma conexao
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        #criar um cursor
        with conn.cursor() as cursor:
            #pra cada item tentar rodar o arquivo
            for i in items:
                run_sql_file(cursor, i)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()
            
def main():

    # python files
    insert_csv()
    insert_ibge()

    #sql files
    p = Path("modeling")
    files_list = [
        p / "dim_geo.sql",
        p / "dim_lead.sql",
        p / "fact_leadhistory.sql"
    ]
    modeling(files_list)

if __name__ == "__main__":
    main();

