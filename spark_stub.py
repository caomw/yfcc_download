__author__ = 'yjxiong'


from pyspark import SparkContext, SparkConf

import urllib
import langid
import sys
import requests
import base64


conf = SparkConf().setAppName('yfcc').setMaster("local[256]")\
    .set('spark.executor.memory','4g')\

sc = SparkContext(conf=conf)