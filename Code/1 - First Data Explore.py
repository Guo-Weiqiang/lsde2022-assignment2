"""
Basic data explore
"""
# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

# COMMAND ----------

# Create a DataFrame using SparkSession, there are 22 in total from  Feb 2020 until May 2022
df_2021_43 = spark.read.format("parquet").option("header","true").option("inferSchema","true").load("dbfs:/mnt/lsde/datasets/commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2021-43/subset=warc/")

# COMMAND ----------

df_2021_43_uk = df_2021_43.select("url_host_name","url", "warc_filename", "warc_record_offset", "warc_record_length", "warc_segment").filter(col("url_host_tld") == "uk")

# COMMAND ----------

df_2021_43_uk.head()

# COMMAND ----------

test = spark.read.format("text").load("dbfs:/mnt/lsde/datasets/commoncrawl/crawl-data/CC-MAIN-2021-43/segments/1634323587719.64/warc/CC-MAIN-20211025154225-20211025184225-00211.warc.gz")
test.count()

# COMMAND ----------

df_2021_43_uk_all_url = df_2021_43_uk.select("url", "warc_filename").filter(col('url').like("%covid%") | col('url').like("%coronavirus%") | col('url').like("%epidemic%")| col('url').like("%pneumonia%"))

# COMMAND ----------

df_2021_43_uk_all_url.count()

# COMMAND ----------

df_2021_43_uk_all_url.head(9)   #delete filter like("%epidemic%")

# COMMAND ----------

warc_path = "dbfs:/mnt/lsde/datasets/commoncrawl/crawl-data/CC-MAIN-2021-43/segments/1634323588244.55/warc/CC-MAIN-20211027212831-20211028002831-00148.warc.gz"
wet_path = warc_path.replace("/warc/", "/wet/").replace("warc.gz", "warc.wet.gz")

# COMMAND ----------

test = spark.read.format("text").load(wet_path)

# COMMAND ----------

wet_path

# COMMAND ----------

test.head(1000000)

# COMMAND ----------

df_2021_43_uk_all_url.head(10)
