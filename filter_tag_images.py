__author__ = 'yjxiong'


from pyspark import SparkContext, SparkConf

import urllib
import langid
import sys
import requests
import base64

from nltk.stem import WordNetLemmatizer

conf = SparkConf().setAppName('image_download').setMaster("local[24]")\
    .set('spark.executor.memory','4g')\

hd_conf = {
    'fs.local.block.size': '134217728',
}

sc = SparkContext(conf=conf)


in_file = sc.textFile('/TMP/yjxiong/YFCC/temp_store/yfcc100m_rich_text_eng_dataset_success_images', 6000)

# in_file = sc.newAPIHadoopFile('/TMP/yjxiong/YFCC/temp_store/yfcc100m_rich_text_eng_dataset_success_images',
#                         'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
#                         'org.apache.hadoop.io.Text',
#                         'org.apache.hadoop.io.LongWritable',
#                         conf=hd_conf)

# first load the tag list
tag_list = [x.strip().split('\t')[0] for x in open('../results/tags/yfcc_freq_general_tags_cleaned.txt').readlines()]

tag_set = set(tag_list)

lt = WordNetLemmatizer()


def check_tag_existence(text_item, tag_set):
    tag_str = text_item[8]
    tag_str_unquoted = urllib.unquote(tag_str)
    tags = [lt.lemmatize(x) for x in tag_str_unquoted.replace('+',' ').split(',')]

    return len(tag_set.intersection(tags))>0


def build_machine_readable_tag_list(text_item, tag_list, tag_set):
    tag_str = text_item[8]
    tag_str_unquoted = urllib.unquote(tag_str)
    tags = [lt.lemmatize(x) for x in tag_str_unquoted.replace('+',' ').split(',')]

    useful_tags = tag_set.intersection(tags)

    tag_id_list = set()
    for t in useful_tags:
        tag_id_list.add(tag_list.index(t))

    out_tag_id_str = ','.join([str(x) for x in sorted(list(tag_id_list))])

    return [text_item[0], out_tag_id_str]


#take a try
# print in_file.map(lambda x: x[1].split('\t')).map(lambda x: build_machine_readable_tag_list(x, tag_list, tag_set)).take(3)

in_file.map(lambda x: x.split('\t')).filter(lambda x: check_tag_existence(x, tag_set))\
    .map(lambda x: build_machine_readable_tag_list(x, tag_list, tag_set))\
    .map(lambda x: '\t'.join(x)).coalesce(100).saveAsTextFile('/TMP/yjxiong/YFCC/temp_store/yfcc100m_rich_text_eng_dataset_success_tag_ids')
