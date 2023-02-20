"""
Implement PageRank Algorithm
"""

# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import MapType,StringType, StructType, StructField

import json as json
from urllib.parse import urljoin, urlparse

import pandas as pd

from warcio.archiveiterator import ArchiveIterator

from graphframes import *


# COMMAND ----------

def is_wat_json_record(record):
    """Return true if WARC record is a WAT record"""
    return (record.rec_type == 'metadata' and
            record.content_type == 'application/json')
    
def yield_link(src, target):
    yield src, target
    
def yield_links(src_url, base_url, links, url_attr, opt_attr=None):
    # base_url = urlparse(base)
    if not base_url:
        base_url = src_url
    has_links = False
    for l in links:
        link = None
        if url_attr in l:
            link = l[url_attr]
#         elif opt_attr in l and ExtractLinksJob.url_abs_pattern.match(l[opt_attr]):
#             link = l[opt_attr]
        else:
            continue
        # lurl = _url_join(base_url, urlparse(link)).geturl()
        try:
            lurl = urljoin(base_url, link)
        except ValueError:
            continue
        has_links = True
        yield src_url, lurl
    if not has_links:
        # ensure that every page is a node in the graph
        # even if it has not outgoing links
        yield src_url, src_url
        
def yield_http_header_links(url, headers):
    if 'Content-Location' in headers:
        yield url, headers['Content-Location']
            
def get_links(url, record):
    try:
        response_meta = record['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']
        if 'Headers' in response_meta:
            # extract links from HTTP header
            for l in yield_http_header_links(url, response_meta['Headers']):
                yield l
        if 'HTML-Metadata' not in response_meta:
            return

        html_meta = response_meta['HTML-Metadata']
        base = None
        if 'Head' in html_meta:
            head = html_meta['Head']
            if 'Base' in head:
                try:
                    base = urljoin(url, head['Base'])
                except ValueError:
                    pass
            if 'Link' in head:
                # <link ...>
                for l in yield_links(url, base, head['Link'], 'url'):
                    yield l
            if 'Scripts' in head:
                for l in yield_links(url, base, head['Scripts'], 'url'):
                    yield l
        if 'Links' in html_meta:
            for l in yield_links(url, base, html_meta['Links'],'url', 'href'):
                yield l
    except KeyError as e:
        print("error")

            
        

# COMMAND ----------

output_schema = StructType([
    StructField("src", StringType(), True),
    StructField("dst", StringType(), True)
])

def process_wat(records):
    url = records[0]
    path = "/dbfs/mnt/lsde/datasets/commoncrawl/" + records[1].replace("/warc/", "/wat/").replace("warc.gz", "warc.wat.gz")
    
    with open(path, 'rb') as stream:
        try: 
            for record in ArchiveIterator(stream):
                if record.rec_headers.get_header('WARC-Target-URI') == url:
                    wat_record = json.loads(record.content_stream().read())
                    warc_header = wat_record['Envelope']['WARC-Header-Metadata']
                    if warc_header['WARC-Type'] == 'response':
                        url = warc_header['WARC-Target-URI']
                        for link in get_links(url, wat_record):
                            yield link

                        break
        except Exception as e: 
            print(e)

    
def run(spark, date):
    path = "dbfs:/mnt/lsde/datasets/commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-" + date + "/subset=warc/"
    df = spark.read.format("parquet").option("header","true").option("inferSchema","true").load(path)
    df = df.select("url", "warc_filename").filter(col("url_host_tld") == "uk").filter(lower(col('url')).like("%covid%")).filter(lower(col('url')).like('%/news/%'))
    
    input_data = df.rdd.sample(False, 0.1, 0)
    output = input_data.flatMap(process_wat).toDF()

    output = output.withColumnRenamed("_1","src") \
    .withColumnRenamed("_2","dst")

    return output

# dates = ['2020-10', '2020-16', '2020-24', '2020-29', '2020-34', '2020-40', '2020-45', '2020-50', '2021-04', '2021-10', '2021-17', '2021-21', '2021-25', '2021-31', '2021-39', '2021-43', '2021-49', '2022-05', '2022-21', '2022-27', '2022-33']    
date = "2020-24"
output = run(spark, date)
output.cache()
print(output.count())

# COMMAND ----------

def remove_useless_links(links):
#     nodes = links.select("src").rdd.map(lambda x: x).collect().To
    links = links.filter(col("dst").isin(col("src")))
    links = links.dropDuplicates()
    return links

links = remove_useless_links(output)
links.cache()
links.head(10)

# COMMAND ----------

def pagerank(edges, links_path=None):
    output_schema = StructType([
        StructField("src", StringType(), True),
        StructField("dst", StringType(), True)
    ])
    
    
    source = edges.select("src")
    target = edges.select("dst")
    vertices = source.union(target).distinct().withColumnRenamed("src","id")
    
    g = GraphFrame(vertices, edges).cache()
    results = g.pageRank(resetProbability=0.15, maxIter=10).cache()
    
    return results

results = pagerank(links)
results.vertices.head(10) 
print('-----------------------')
results.edges.head(10)

# COMMAND ----------

x = results.vertices.select("pagerank").distinct()
x.head(10)

# COMMAND ----------

results.vertices.write.format("parquet").mode("overwrite").option("compression", "snappy").save("dbfs:/mnt/lsde/group16/" + date + "/pagerank/vertices/")
results.edges.write.format("parquet").mode("overwrite").option("compression", "snappy").save("dbfs:/mnt/lsde/group16/" + date + "/pagerank/edges/")


