# Databricks notebook source
# MAGIC %fs ls /databricks-datasets/COVID/USAFacts/

# COMMAND ----------

display(spark.read.text("dbfs:/databricks-datasets/COVID/USAFacts/USAFacts_readme.md"))

# COMMAND ----------

df = spark.read.option("inferSchema","true").option("header","true").csv("dbfs:/databricks-datasets/COVID/USAFacts/covid_confirmed_usafacts.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

display(df)
