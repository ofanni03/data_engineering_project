# Query the curated data product with Spark SQL.
# python query.py                          # run all queries in sql/example_queries.sql
# python query.py "SELECT * FROM tourism_weather LIMIT 5"


import sys
from pathlib import Path

import config
from common import get_spark

SQL_FILE = Path(__file__).parent / "sql" / "example_queries.sql"


def main():
    spark = get_spark("tourism-query")
    spark.read.parquet(config.CURATED_TABLE).createOrReplaceTempView("tourism_weather")

    if len(sys.argv) > 1:
        queries = [sys.argv[1]]
    else:
        queries = [q.strip() for q in SQL_FILE.read_text().split(";") if q.strip()]

    for query in queries:
        print("\n" + query)
        spark.sql(query).show(100, truncate=False)


if __name__ == "__main__":
    main()