__author__ = 'yjxiong'

from pyspark import SparkContext, SparkConf

import urllib
import langid
import sys
import requests
import base64

conf = SparkConf().setAppName('count_num').setMaster("local[24]").set('spark.python.worker.memory','4g') \
    .set('spark.executor.memory','20g') \
    .set('spark.driver.memory','20g')
sc = SparkContext(conf=conf)

in_file = sc.textFile('/TMP/yjxiong/YFCC/temp_store/yfcc100m_rich_text_eng_dataset_images')

in_file.filter(lambda x:x.split('\t')[-1] != 'N/A').coalesce(2000)\
    .saveAsTextFile('/TMP/yjxiong/YFCC/temp_store/yfcc100m_rich_text_eng_dataset_success_images')
