from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('demo').setMaster("local[24]")

sc = SparkContext(conf=conf)

text = sc.textFile("/TMP/yjxiong/YFCC/src_data/yfcc100m_dataset-*")

data = text.map(lambda x: x.split('\t'))

# print data.filter(lambda x: 'Canon' in x[5]).count()

# print text.first()

rich_info_items = data.filter(lambda x: x[6] != "" and x[7] != "" and x[8] != "")

rich_info_items.map(lambda x: '\t'.join(x)).repartition(10).saveAsTextFile("/TMP/yjxiong/YFCC/temp_store/yfcc100m_rich_text_dataset")