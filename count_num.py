__author__ = 'yjxiong'

from pyspark import SparkContext, SparkConf

import urllib
import langid
import sys
import requests
import base64

conf = SparkConf().setAppName('count_num').setMaster("local[24]").set('spark.python.worker.memory','4g') \
    .set('spark.executor.memory','20g') \
    .set('spark.driver.memory','20g') \

sc = SparkContext(conf=conf)

name = sys.argv[1]

if len(sys.argv) == 2:
    print sc.textFile(name).count()
elif sys.argv[2] == 'filter':
    print sc.textFile(name).filter(lambda x: x.split('\t')[-1] != 'N/A').count()
