# Databricks notebook source
# DBTITLE 1,SETUP
import delta
import sys
sys.path.insert(0, "../lib/")

import db
import ingestors

catalog = 'bronze'
database = 'trampar_de_casa'
table = dbutils.widgets.get("table")
id_field = dbutils.widgets.get("id_field")

checkpoint_path = f"/Volumes/raw/trampar_de_casa/full-load/checkpoint_{table}/"

# COMMAND ----------

# DBTITLE 1,INGESTÃO FULL-LOAD
if not db.table_exists(catalog=catalog,
                       database=database,
                       table=table,
                       spark=spark):
    
    print("Criando a tabela...")
    ingestao = ingestors.IngestaoFullBronze(table=table,
                                            idField=id_field,
                                            spark=spark)
    ingestao.auto()

    dbutils.fs.rm(checkpoint_path, True)
    print("ok")

# COMMAND ----------

# DBTITLE 1,INGESTAO STREAMING
print("Executando streaming...")
ingestao_stream = ingestors.IngestaoStreamingBronze(table=table,
                                                    idField=id_field,
                                                    spark=spark)
ingestao_stream.auto()
print("ok")
