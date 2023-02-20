"""
Implement sentiment analysis using T5 Transformer and Bert model.
"""
# Databricks notebook source
# MAGIC %pip install sparknlp

# COMMAND ----------

import pandas as pd
import numpy as np
import json
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from sparknlp.annotator import *
from sparknlp.base import *
import sparknlp
from sparknlp.pretrained import PretrainedPipeline
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import *
from pyspark.sql.types import MapType,StringType, StructType, StructField
from urllib.parse import urljoin, urlparse
from warcio.archiveiterator import ArchiveIterator

# COMMAND ----------

df_2020_10 = spark.read.format("parquet").option("header","true").option("inferSchema","true").load("dbfs:/mnt/lsde/datasets/commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2022-33/subset=warc/")
df_2020_10 = df_2020_10.select("url", "warc_filename").filter(col("url_host_tld") == "uk").filter(col('url').like("%cornavirus%") )            
pandas_df = df_2020_10.toPandas()
spark = sparknlp.start()

# COMMAND ----------

#summarization
document_assembler = DocumentAssembler()\
.setInputCol("text")\
.setOutputCol("documents")

# define T5Transformer model
t5 = T5Transformer() \
  .pretrained("t5_small", 'en') \
  .setTask("summarize:")\
  .setMaxOutputLength(200)\
  .setInputCols(["documents"]) \
  .setOutputCol("summaries")

summarizer_pp = Pipeline(stages=[
    document_assembler, t5
])
empty_df = spark.createDataFrame([['']]).toDF('text')
pipeline_model = summarizer_pp.fit(empty_df)
sum_lmodel = LightPipeline(pipeline_model)

# COMMAND ----------

#emotion model
model_emotion = "bert_sequence_classifier_emotion"
sequenceClassifier = BertForSequenceClassification.pretrained(model_emotion, 'en').setInputCols(['token', 'document']).setOutputCol('pred_class')

# COMMAND ----------

def process_wet(stream, url):    
    for record in ArchiveIterator(stream):
        if record.rec_headers.get_header('WARC-Target-URI') == url:
            wet_record = record.content_stream().read().decode("utf-8")
            return wet_record
            break
            
def run_pipeline(model, text):
    document_assembler = DocumentAssembler().setInputCol('text').setOutputCol('document')
    tokenizer = Tokenizer().setInputCols(['document']).setOutputCol('token')
    pipeline = Pipeline(stages=[document_assembler, tokenizer, sequenceClassifier])
    df = spark.createDataFrame(text, StringType()).toDF("text")
    results[model]=(pipeline.fit(df).transform(df))
    return results
results = {}
            

n = 0
sadness = 0
joy = 0
anger = 0
fear = 0
surprise = 0
love = 0
for index, row in pandas_df.iterrows():
    if n >= 10: 
        break 
    url = row[0]   
    path = "/dbfs/mnt/lsde/datasets/commoncrawl/" + row[1].replace("/warc/", "/wet/").replace("warc.gz", "warc.wet.gz")
    stream = open(path, 'rb')     #read the whole article
    article_text = process_wet(stream, url)   #select the WET which match to the url, article_text is the pure article text related to the covid 
    article_text = article_text.replace('/', ' ')   #the new version takes the text as uri wrongly, so we need this step
    result_of_summary = sum_lmodel.fullAnnotate(article_text)[0]

    
    #emotion below
    text_emotion = [result_of_summary['summaries'][0].result]   #use summary as the input of emotion model
    model_dict = {model_emotion: text_emotion}
    for model, text in zip(model_dict.keys(),model_dict.values()):  
        results = run_pipeline(model, text)
    for model_name, result in zip(results.keys(),results.values()):   #show the result of emotion model
        res = result.select(F.explode(F.arrays_zip(result.document.result, result.pred_class.result, result.pred_class.metadata)).alias("col")).select(F.expr("col['1']").alias("prediction")).first()[0]
        if (res == "sadness"):
            print(result_of_summary['summaries'][0].result, url)          
            sadness = sadness + 1  
        if (res == "joy"):
            joy = joy + 1
        if (res == "anger"):
            anger = anger + 1
            print(result_of_summary['summaries'][0].result, url)
        if (res == "fear"):
            print(result_of_summary['summaries'][0].result, url)
            fear = fear + 1
        if (res == "surprise"):
            surprise = surprise + 1
        if (res == "love"):
            love = love + 1
stream.close()
print("sadness", sadness)
print("joy", joy)
print("anger", anger)
print("fear", fear)
print("surprise", surprise)
print("love", love)