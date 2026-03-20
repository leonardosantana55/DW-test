import csv
import psycopg2
import glob

host = "localhost"
dbname = "mydb"
user = "XQ6460"
password = "08021994"

# connect to pg
conn = psycopg2.connect(
    host=host,
    dbname=dbname,
    user=user,
    password=password
)
cur = conn.cursor()

# read csv files on a path
# TODO test for subdirectories
path = "./data"
files = glob.glob(path + '/**/*.csv', recursive=True)

# create a table for each file in the directory
for f in files:
    table_name = f.removeprefix("./data/").removesuffix(".csv")
    with open(f, encoding="utf8") as f:
        reader = csv.reader(f)
        header = next(reader)
        sample = [next(reader) for _ in range(5)]

    # create a list of sql.SQL objects with the column name and type
    cols = []
    sql_query = ""
    for col_name in header:
        # fix some problems with utf8 csv reading and bad naming
        safe_name = col_name.replace(" ", "_")
        safe_name = col_name.replace("\ufeff", "") # fix for BOM
        # append to the cols list in a format that psycopg2 wants
        cols.append(
            sql.SQL("{} {}").format(
                sql.Identifier(safe_name),
                sql.SQL("VARCHAR(255)")))
    # compose the final query
    sql_query = sql.SQL("CREATE TABLE IF NOT EXISTS {table} ({cols})").format(
        table=sql.Identifier(table_name),
        cols=sql.SQL(", ").join(cols))
    # execute query
    cur.execute(sql_query)
    # make the changes to the database persistent
    conn.commit()
