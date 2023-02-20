"""
Combine the PageRank Value(measuring the importance) and Term Frequency(measuring the relevance)
to get a ranking score(called PT score) to rank the web pages.
"""
# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

date = '2020-10'
input_path = "dbfs:/mnt/lsde/group16/" + date 
word_fre = spark.read.format("parquet").load(input_path + "/news_word_frequency/")
# pagerank = spark.read.format("parquet").load(input_path + "/pagerank/vertices")

# COMMAND ----------

temp = word_fre.join(pagerank, word_fre.url == pagerank.id).select("url", "word_frequency", "pagerank")
temp = temp.withColumn("pagerank_scaled", (col("pagerank") - 1) / (10 - 1)).withColumn("word_fre_scaled", (col("word_frequency") - 0) / (1 - 0))
output = temp.withColumn("PT_score",col("pagerank_scaled") + col("word_fre_scaled")).select("url", "PT_score")

# COMMAND ----------

output_path = "dbfs:/mnt/lsde/group16/" + date + "/PT_score/"
output.write.format("parquet").mode("overwrite").option("compression", "snappy").save(output_path)

# COMMAND ----------

