import delta
import db

class IngestaoFullBronze:

    def __init__(self, table, idField, spark):
        self.table = table
        self.spark = spark
        self.idField = idField
        self.path = f"/Volumes/raw/trampar_de_casa/full-load/{table}/"
        self.tablename = f'bronze.trampar_de_casa.{table}'
        self.query = db.import_query(f"{table}.sql")
                        
    def read(self):
        df = (self.spark
                  .read
                  .option("delimiter", ",")
                  .option("quote", '"') 
                  .option("escape", '"')
                  .csv(self.path, header=True, inferSchema=True))
        return df
    
    def transform(self, df):
        query = self.query.format(table=self.table)
        df.createOrReplaceTempView(self.table)
        df_transform = self.spark.sql(query)
        return df_transform

    def save(self, df):
        (df.write
           .mode("overwrite")
           .format("delta")
           .option("overwriteSchema", "true")
           .saveAsTable(self.tablename))

    def auto(self):
        df = self.read()
        df_transform = self.transform(df)
        self.save(df_transform)


class IngestaoStreamingBronze(IngestaoFullBronze):

    def __init__(self, table, idField, spark):
        super().__init__(table, idField, spark)
        self.checkpoint_path = f"/Volumes/raw/trampar_de_casa/full-load/checkpoint_{table}/"
    
    def read(self):
        df_stream = (self.spark
                         .readStream
                         .format("cloudFiles")
                         .option("cloudFiles.format", "csv")
                         .option("quote", '"')
                         .option("escape", '"')
                         .option("header", 'true')
                         .option("sep", ',')
                         .option("cloudFiles.schemaLocation", self.checkpoint_path)
                         .load(self.path))
        return df_stream        
        
    def upsert(self, df, delta_table):
        query = self.query.format(table=f"global_temp.{self.table}")
        df.createOrReplaceGlobalTempView(self.table)
        df_transform = self.spark.sql(query)
        join_on = f"d.{self.idField} = n.{self.idField}"
        

        (delta_table.alias("d")
                    .merge(df_transform.alias("n"), join_on)
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute())
        
    def transform(self, df):
        delta_table = delta.DeltaTable.forName(self.spark, self.tablename)
        stream = (df.writeStream
                    .foreachBatch(lambda df, batchId: self.upsert(df, delta_table))
                    .option("checkpointLocation", self.checkpoint_path)
                    .trigger(availableNow=True))
        return stream

    def save(self, df):
        return df.start()
    
    def auto(self):
        df = self.read()
        df_transform = self.transform(df)
        return self.save(df_transform)
    

class IngestorFullSilver:

    def __init__(self, table, spark):
        self.table = table
        self.spark = spark
        self.query = db.import_query(f"{table}.sql")
        self.tablename = f"silver.trampar_de_casa.{table}"

    def read(self):
        query = self.query.format(table=self.table)
        df = self.spark.sql(self.query)
        return df
    
    def save(self, df):
        (df.write
           .mode("overwrite")
           .format("delta")
           .option("overwriteSchema", "true")
           .saveAsTable(self.tablename))

    def auto(self):
        df = self.read()
        self.save(df)


