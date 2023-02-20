"""
Visualise the keywords using wordcloud library.
"""

# Databricks notebook source
# MAGIC %pip install wordcloud

# COMMAND ----------

import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS
from collections import ChainMap
import pyspark.sql.functions as F
from pyspark.sql.functions import *

# COMMAND ----------

# dates = ['2020-10']
dates = ['2020-10', '2020-16', '2020-24', '2020-29', '2020-34', '2020-40', '2020-45', '2020-50', '2021-04', '2021-10', '2021-17', '2021-21', '2021-25', '2021-31', '2021-39', '2021-43', '2021-49', '2022-05', '2022-21', '2022-27', '2022-33']
for date in dates:
    path = "dbfs:/mnt/lsde/group16/" + date + "/news_keywords/"
    words = spark.read.format("parquet").option("header","true").option("inferSchema","true").load(path)
    # option 1: Dont take weight into account
#     df = words.select('url', lower('keywords')).withColumnRenamed("lower(keywords)", "keywords").groupBy('keywords').count().sort('count', ascending=False).withColumnRenamed("count", "importance")
    

    # option 2: Take weight into account
    WF = spark.read.format("parquet").option("header","true").option("inferSchema","true").load("dbfs:/mnt/lsde/group16/" + date + "/news_word_frequency/")
    df = words.select('url', lower('keywords')).withColumnRenamed("lower(keywords)", "keywords").join(WF, WF.url == words.url).groupBy('keywords').sum("word_frequency").withColumnRenamed("sum(word_frequency)", "importance") \
        .sort('importance', ascending=False)
    # tmp2 = df.join(words, df.keywords == lower(words.keywords)).sort('importance', ascending=False).select(df.keywords, 'url', 'importance').dropDuplicates(["keywords"]).sort('importance', ascending=False).limit(50) \
        # .withColumnRenamed("keywords", "name").withColumnRenamed("importance", "value")
    # output_path = output_path = "dbfs:/mnt/lsde/group16/"  + "/keywords/" + date
    # tmp = tmp2.select("name", "value", "url")
    # tmp.write.format("json").mode("overwrite").save(output_path)
    # words_dict = dict(ChainMap(*tmp.select(F.create_map('name', 'url')).rdd.map(lambda x: x[0]).collect()))
    # print(date, "----------------------------------")
    # print(words_dict)
    # output_path = output_path = "dbfs:/mnt/lsde/group16/"  + "/weight_urls/" + date + "/"
    # tmp.write.format("csv").mode("overwrite").save(output_path)
      
    wc = WordCloud(background_color="white", max_words=100, random_state=42) #  

    words_dict = dict(ChainMap(*df.select(F.create_map('keywords', 'importance')).rdd.map(lambda x: x[0]).collect()))
    wc = WordCloud(width = 3000, height = 2000, random_state=42, background_color='black', colormap='Pastel1', collocations=False, stopwords = STOPWORDS).generate_from_frequencies(words_dict)
 
    plt.imshow(wc.generate_from_frequencies(words_dict))
    plt.axis("off")
    plt.show()
#     plt.savefig('/dbfs/mnt/lsde/group16/' + date + '.svg', dpi=700, format="svg")

    wc.to_file('/dbfs/mnt/lsde/group16/wc-weighted/' + date + '.eps') # output the figure to the path




