# Databricks notebook source
import sys

sys.path.insert(0, "../lib")

import db
import ingestors

table = dbutils.widget.get("table")

query = db.import_query(f"{table}.sql")

# COMMAND ----------

ingestor = ingestors.IngestorFullSilver(table=table, spark=spark)
ingestor.auto()
