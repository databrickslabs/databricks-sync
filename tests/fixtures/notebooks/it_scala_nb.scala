// Databricks notebook source
// MAGIC %md # Advertising Analytics Click Prediction: ETL
// MAGIC ####[Ad impressions with clicks dataset](https://www.kaggle.com/c/avazu-ctr-prediction/data)
// MAGIC 
// MAGIC <!-- <img src="/files/img/fraud_ml_pipeline.png" alt="workflow" width="500"> -->
// MAGIC 
// MAGIC This is the ETL notebook for the series of Advertising Analytics Click Prediction notebooks.  For this stage, we will focus in the Ingest and Exploration of data.  For information on how to import data into Databricks, refer to [Accessing Data](https://docs.databricks.com/user-guide/importing-data.html).

// COMMAND ----------

// MAGIC %python 

// COMMAND ----------

// DBTITLE 1,6 GB csv file
// MAGIC %sh ls -lh /dbfs/mnt/adtech/impression/csv/train.csv/

// COMMAND ----------

// DBTITLE 1,Raw data
// MAGIC %fs head /mnt/adtech/impression/csv/train.csv/part-00000-tid-695242982481829702-ed48f068-bfdf-484a-a397-c4144e4897d8-0-c000.csv

// COMMAND ----------

// MAGIC %fs ls /mnt/adtech/impression/

// COMMAND ----------

// DBTITLE 1,Read data, infer schema
val df = spark.read
  .option("header", true)
  .option("inferSchema", true)
  .csv("/mnt/adtech/impression/csv/train.csv/")

// COMMAND ----------

// DBTITLE 1,~ 40 M rows
df.count

// COMMAND ----------

df.printSchema

// COMMAND ----------

df.coalesce(4)
  .write
  .mode("overwrite")
  .parquet("/mnt/adtech/impression/parquet/train.csv")

// COMMAND ----------

// MAGIC %fs ls /mnt/adtech/impression/parquet/train.csv/

// COMMAND ----------

// MAGIC %sh ls -lh /dbfs/mnt/adtech/impression/parquet/train.csv/

// COMMAND ----------

// MAGIC %fs *select: from
