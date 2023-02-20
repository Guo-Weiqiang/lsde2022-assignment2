"""
Further Data explore, see the detailed information about the common crawl datasets.
For example, the distribution of the number of news web pages related to Covid-19 in UK.
"""
# Databricks notebook source
from pyspark.sql.functions import *

def url_count_covid(date):
    path = "dbfs:/mnt/lsde/datasets/commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-"  + date + "/subset=warc/"
    df = spark.read.format("parquet").option("header","true").option("inferSchema","true").load(path)
    df_uk = df.select("url").filter(col("url_host_tld") == "uk").filter(lower(col('url')).like("%covid%")).filter(lower(col('url')).like('%/news/%')).cache()
    num = df_uk.count()
    print("News number is ", num, df_uk.head(10))
    return num

dates = ['2020-10', '2020-16', '2020-24', '2020-29', '2020-34', '2020-40', '2020-45', '2020-50', '2021-04', '2021-10', '2021-17', '2021-21', '2021-25', '2021-31', '2021-39', '2021-43', '2021-49', '2022-05', '2022-21', '2022-27', '2022-33']
news_nums = []
for date in dates:
    news_nums.append(url_count_covid(date))
print("The number of News web pages is {0}" % news_nums)
