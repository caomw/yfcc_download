__author__ = 'yjxiong'

from pyspark import SparkContext, SparkConf

import urllib
import langid
import sys
import requests
import base64

conf = SparkConf().setAppName('image_download').setMaster("local[256]")\
    .set('spark.executor.memory','4g')\

sc = SparkContext(conf=conf)


def download_image(text_line):
    info = text_line.split('\t')
    link = info[14]
    try:
        req = requests.get(link, allow_redirects=False, headers={'Connection':'close'})
        if req.status_code != 200:
            rst = 'N/A'
        else:
            rst = base64.b64encode(req.content)
    except requests.ConnectionError:
        rst = 'N/A'
    return '\t'.join(info + [rst])


link_data = sc.textFile('/TMP/yjxiong/YFCC/src_data/yfcc100m_dataset-8')

link_data.map(download_image).saveAsTextFile('/DATA/Datasets/YFCC100M/raw_data/yfcc100m_dataset-8_images')
