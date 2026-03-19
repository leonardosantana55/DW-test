import csv
import psycopg2
import glob

host = "localhost"
dbname = "mydb"
user = "XQ6460"
password = "08021994"

#connect to pg
conn = psycopg2.connect(
    host=host,
    dbname=dbname,
    user=user,
    password=password
)
cur = conn.cursor()

#read csv files on a folder
#TODO test for subdirectories
path = "./data"
files = glob.glob(path + '/**/*.csv', recursive=True)
for f in files:
    table_name = f.removeprefix("./data/").removesuffix(".csv")
    with open(f, encoding="utf8") as f:
        reader = csv.reader(f)
        header = next(reader)
        sample = [next(reader) for _ in range(5)]

    cols = []
    sql_query = ""
    for col_name in header:
        safe_name = col_name.replace(" ", "_")

    cols.append(f"{sql.Identifier(safe_name)} TEXT")

    sql_query = sql.SQL("CREATE TABLE IF NOT EXISTS {table} ({cols})")\
    .format(table=sql.Identifier(table_name), cols=sql.SQL(', ').join(cols))

# TODO fiquei impacado nessa parte de compor a string. tenho que fumar um tabaco e ler a documentação com calma. https://www.psycopg.org/docs/sql.html#psycopg2.sql.Identifier
