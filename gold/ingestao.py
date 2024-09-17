# Databricks notebook source
import sys

sys.path.insert(0, "../lib")

import db
import ingestors

table = dbutils.widgets.get("table")

query = db.import_query(f"{table}.sql")

# COMMAND ----------

ingestor = ingestors.IngestorFullGold(table=table, spark=spark)
ingestor.auto()
