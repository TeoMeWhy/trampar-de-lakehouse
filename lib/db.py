def import_query(path):
    with open(path, 'r') as of:
        return of.read()


def table_exists(catalog, database, table, spark):
    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
                  .filter(f"database = '{database}'")
                  .filter(f"tableName = '{table}'")
                  .count())
    return count == 1