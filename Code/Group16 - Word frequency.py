"""
Compute Term Frequency that is the frequency of covid-related terms occuring on a web page
"""

# Databricks notebook source
from pyspark.sql.functions import *
from warcio.archiveiterator import ArchiveIterator

# COMMAND ----------

def process_wet(records):   
    url = records[0]
    path = records[1]
    
    path = "/dbfs/mnt/lsde/datasets/commoncrawl/" + path.replace("/warc/", "/wet/").replace("warc.gz", "warc.wet.gz")
    with open(path, 'rb') as stream:
        try:
            for record in ArchiveIterator(stream):
                if record.rec_headers.get_header('WARC-Target-URI') == url:
                    wet_record = record.content_stream().read().decode("utf-8")
                    word_count = wet_record.lower().count("coronavirus") + wet_record.lower().count("covid")
                    word_frequency = word_count / len(wet_record.split(' ')) 
                    yield (url, word_frequency)

                    break
        except Exception as e: 
            print(e)
            

def run(date):
    path = "dbfs:/mnt/lsde/datasets/commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-" + date + "/subset=warc/"
    df = spark.read.format("parquet").option("header","true").option("inferSchema","true").load(path)
    df = df.select("url", "warc_filename").filter(col("url_host_tld") == "uk").filter(lower(col('url')).like("%covid%")).filter(lower(col('url')).like('%/news/%'))
    print("The date is ", date, "; The number of News web pages is", df.count())
    
    input_data = df.rdd #.sample(False, 0.1, 0)
    output = input_data.flatMap(process_wet).toDF().withColumnRenamed("_1","url") \
    .withColumnRenamed("_2","word_frequency")
    
    output_path = "dbfs:/mnt/lsde/group16/" + date + "/news_word_frequency/"
    print("output the results to ", output_path)
    output.write.format("parquet").mode("overwrite").save(output_path)
    
#     return output

date = "2020-24"
run(date)
# dates = ['2020-29', '2020-34', '2020-40', '2020-45', '2020-50', '2021-04', '2021-10', '2021-17', '2021-21']
# for date in dates:
#     try:
#         run(date)
#         print(date, "is successfully")
#     except Exception as e:
#         print(date, e)


# COMMAND ----------



