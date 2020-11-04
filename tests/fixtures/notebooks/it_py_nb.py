# Databricks notebook source
# MAGIC %md # Advertising Analytics Click Prediction: ML
# MAGIC ####[Ad impressions with clicks dataset](https://www.kaggle.com/c/avazu-ctr-prediction/data)
# MAGIC 
# MAGIC <!-- <img src="/files/img/fraud_ml_pipeline.png" alt="workflow" width="500"> -->
# MAGIC 
# MAGIC This is the ML notebook for the series of Advertising Analytics Click Prediction notebooks.  For this stage, we will focus on creating features and training and evaluating the ML model.
# MAGIC 
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/pub-tc/ML-workflow.png" width="800">

# COMMAND ----------

# DBTITLE 1,Load data
impression = spark.read \
  .parquet("/mnt/adtech/impression/parquet/train.csv/") \
  .selectExpr("*", "substr(hour, 7) as hr").repartition(64)

# COMMAND ----------

# DBTITLE 1,Number of distinct values per column
from pyspark.sql.functions import *

strCols = map(lambda t: t[0], filter(lambda t: t[1] == 'string', impression.dtypes))
intCols = map(lambda t: t[0], filter(lambda t: t[1] == 'int', impression.dtypes))

# [row_idx][json_idx]
strColsCount = sorted(map(lambda c: (c, impression.select(countDistinct(c)).collect()[0][0]), strCols), key=lambda x: x[1], reverse=True)
intColsCount = sorted(map(lambda c: (c, impression.select(countDistinct(c)).collect()[0][0]), intCols), key=lambda x: x[1], reverse=True)

# COMMAND ----------

# distinct counts for str columns
display(strColsCount)

# COMMAND ----------

# distinct counts for int columns
display(intColsCount)

# COMMAND ----------

# DBTITLE 1,Prepare features
# Include PySpark Feature Engineering methods
from pyspark.ml.feature import StringIndexer, VectorAssembler

# All of the columns (string or integer) are categorical columns
#  except for the [click] column
maxBins = 70
categorical = map(lambda c: c[0], filter(lambda c: c[1] <= maxBins, strColsCount))
categorical += map(lambda c: c[0], filter(lambda c: c[1] <= maxBins, intColsCount))
categorical.remove('click')

# Apply string indexer to all of the categorical columns
#  And add _idx to the column name to indicate the index of the categorical value
stringIndexers = map(lambda c: StringIndexer(inputCol = c, outputCol = c + "_idx"), categorical)

# Assemble the put as the input to the VectorAssembler 
#   with the output being our features
assemblerInputs = map(lambda c: c + "_idx", categorical)
vectorAssembler = VectorAssembler(inputCols = assemblerInputs, outputCol = "features")

# The [click] column is our label 
labelStringIndexer = StringIndexer(inputCol = "click", outputCol = "label")

# The stages of our ML pipeline 
stages = stringIndexers + [vectorAssembler, labelStringIndexer]

# COMMAND ----------

from pyspark.ml import Pipeline

# Create our pipeline
pipeline = Pipeline(stages = stages)

# create transformer to add features
featurizer = pipeline.fit(impression)

# COMMAND ----------

# dataframe with feature and intermediate transformation columns appended
featurizedImpressions = featurizer.transform(impression)

# COMMAND ----------

# DBTITLE 1,Feature and label columns
display(featurizedImpressions.select('features', 'label'))

# COMMAND ----------

# DBTITLE 1,Split training and test data
train, test = featurizedImpressions \
  .select(["label", "features", "hr"]) \
  .randomSplit([0.7, 0.3], 42)
train.cache()
test.cache()

# COMMAND ----------

# DBTITLE 1,Train the model
from pyspark.ml.classification import GBTClassifier

# Train our GBTClassifier model 
classifier = GBTClassifier(labelCol="label", featuresCol="features", maxBins=maxBins, maxDepth=10, maxIter=10)
model = classifier.fit(train)

# COMMAND ----------

# DBTITLE 1,Predict
# Execute our predictions
predictions = model.transform(test)

# COMMAND ----------

# DBTITLE 1,Evaluation
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Evaluate our GBTClassifier model using BinaryClassificationEvaluator()
ev = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", metricName="areaUnderROC")
print(ev.evaluate(predictions)

# COMMAND ----------

# DBTITLE 1,Features by weight
import json
features = map(lambda c: str(json.loads(json.dumps(c))['name']), \
               predictions.schema['features'].metadata.get('ml_attr').get('attrs').values()[0])
# convert numpy.float64 to str for spark.createDataFrame()
weights=map(lambda w: '%.10f' % w, model.featureImportances)
weightedFeatures = sorted(zip(weights, features), key=lambda x: x[1], reverse=True)
spark.createDataFrame(weightedFeatures).toDF("weight", "feature").createOrReplaceTempView('wf')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select feature, weight 
# MAGIC from wf 
# MAGIC order by weight desc

# COMMAND ----------

predictions.createOrReplaceTempView("predictions")

# COMMAND ----------

# MAGIC %sql describe predictions

# COMMAND ----------

# MAGIC %sql select sum(case when prediction = label then 1 else 0 end) / (count(1) * 1.0) as accuracy
# MAGIC from predictions

# COMMAND ----------


