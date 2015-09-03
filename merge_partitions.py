__author__ = 'yjxiong'

from pyspark import SparkContext, SparkConf

import urllib
import langid
import sys

conf = SparkConf().setAppName('lang_analysis').setMaster("local[24]").set('spark.executor.memory','2g')

sc = SparkContext(conf=conf)

name = sys.argv[1]
n_part = int(sys.argv[2])

sc.textFile(name).coalesce(n_part).saveAsTextFile(name+'_compact')
