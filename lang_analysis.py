__author__ = 'yjxiong'


from pyspark import SparkContext, SparkConf

import urllib
import langid

conf = SparkConf().setAppName('lang_analysis').setMaster("local[24]").set('spark.executor.memory','2g')

sc = SparkContext(conf=conf)

text_info = sc.textFile("/TMP/yjxiong/YFCC/temp_store/yfcc100m_rich_text_dataset").map(lambda x: x.split('\t'))


def verify_lang(text_item, lang):
    for text in text_item[6:9]:
        blank_text = urllib.unquote(text)
        pred_lang = langid.classify(blank_text)[0]
        if pred_lang != lang:
            return False
    else:
        return True

text_info.filter(lambda x: verify_lang(x, 'en')).map(lambda x: '\t'.join(x)).saveAsTextFile("/TMP/yjxiong/YFCC/temp_store/yfcc100m_rich_text_eng_dataset")
