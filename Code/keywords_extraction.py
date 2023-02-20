"""
Implement Keywords Extraction using !YAKE model.
"""

# Databricks notebook source
# MAGIC %pip install git+https://github.com/LIAAD/yake

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import MapType,StringType, StructType, StructField

import json as json
from urllib.parse import urljoin, urlparse

import pandas as pd

from warcio.archiveiterator import ArchiveIterator

from graphframes import *
import yake

from pyspark.sql import Row

# COMMAND ----------

language = "en" # 
max_ngram_size = 1 # N-grams
deduplication_thresold = 0.1 
deduplication_algo = 'seqm'
windowSize = 1
numOfKeywords = 1 

# define yake model
kw_extractor = yake.KeywordExtractor(lan=language, 
                                     n=max_ngram_size, 
                                     dedupLim=deduplication_thresold, 
                                     dedupFunc=deduplication_algo, 
                                     windowsSize=windowSize, 
                                     top=numOfKeywords)

def process_wet(records):
    url = records[0]
    path = "/dbfs/mnt/lsde/datasets/commoncrawl/"  + records[1].replace("/warc/", "/wet/").replace("warc.gz", "warc.wet.gz")
    with open(path, 'rb') as stream:
        try:
            for record in ArchiveIterator(stream):
                if record.rec_headers.get_header('WARC-Target-URI') == url:
                    wet_record = record.content_stream().read().decode("utf-8")                       
                    res = kw_extractor.extract_keywords(wet_record)
                    yield Row(url, res[0][0])
                    break
        except Exception as e: 
            print(e)   
          
        
# News web pages
def run(date):
    path = "dbfs:/mnt/lsde/datasets/commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-" + date + "/subset=warc/"
    df = spark.read.format("parquet").option("header","true").option("inferSchema","true").load(path)
    df = df.select("url", "warc_filename").filter(col("url_host_tld") == "uk").filter(lower(col('url')).like("%covid%")).filter(lower(col('url')).like('%/news/%'))
    
    input_data = df.rdd #.sample(False, 0.1, 0)
    output = input_data.flatMap(process_wet).toDF().withColumnRenamed("_1","url").withColumnRenamed("_2","keywords").cache()
    output.write.format("parquet").mode("overwrite").option("compression", "snappy").save("dbfs:/mnt/lsde/group16/" + date + "/news_keywords/")
#     output.cache()
    return output

# dates = ['2020-29', '2020-34', '2020-40', '2020-45', '2020-50', '2021-04', '2021-10', '2021-17', '2021-21']   # '2020_05', '2020_10', '2020_16',
# dates = [  ] #  date = "2021-31"
# for date in dates:
#     try:
#         output = run(date)
#         print(date, "is successfully")
#     except Exception as e:
#         print(date, e)
date = "2020-24"
output = run(date)


# COMMAND ----------


