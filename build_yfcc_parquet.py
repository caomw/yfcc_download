__author__ = 'yjxiong'

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *

conf = SparkConf().setAppName('build_parquet').setMaster("local[24]")\
    .set('spark.executor.memory','4g')\


sc = SparkContext(conf=conf)
sqlc = SQLContext(sc)

# yfcc_field_list = open('yfcc_schema.txt').read().split()

yfcc_field_list = ['image_id', 'labels']

fields = [StructField(field_name, StringType(), True) for field_name in yfcc_field_list]

yfcc_schema = StructType(fields)

# yfcc_data = sc.textFile('/TMP/yjxiong/YFCC/temp_store/yfcc100m_rich_text_eng_dataset_success_images')\
#     .map(lambda x: x.split('\t')).coalesce(2000)

yfcc_data = sc.textFile('/TMP/yjxiong/YFCC/temp_store/yfcc100m_rich_text_eng_dataset_success_tag_ids')\
    .map(lambda x: x.split('\t')).coalesce(10)

yfcc_schema_df = sqlc.createDataFrame(yfcc_data, yfcc_schema)

# yfcc_schema_df.write.parquet('/TMP/yjxiong/YFCC/temp_store/yfcc100m_rich_text_eng_dataset_success_images_parquet')

yfcc_schema_df.write.parquet('/TMP/yjxiong/YFCC/temp_store/yfcc100m_rich_text_eng_dataset_success_tag_ids_parquet')

