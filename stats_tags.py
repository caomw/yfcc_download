__author__ = 'yjxiong'

from spark_stub import sc
import urllib
from operator import add
import cPickle

dataset = '/TMP/yjxiong/YFCC/temp_store/yfcc100m_rich_text_eng_dataset'
out_file = '../results/tags/tag_stat_naive.pc'

text_info = sc.textFile(dataset).map(lambda x: x.split('\t'))


def parse_tags(text_item):
    tags_str = text_item[8]
    tags_str_unquoted = urllib.unquote(tags_str)
    tags = tags_str_unquoted.split(',')
    return tags

tag_stats = text_info.flatMap(parse_tags).map(lambda x: (x, 1)).reduceByKey(add).sortBy(lambda x: x[1], False).collect()

cPickle.dump(tag_stats, open(out_file, 'wb'))