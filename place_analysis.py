__author__ = 'yjxiong'

# from spark_stub import  sc
import cPickle
import nltk
import re
from nltk.corpus import stopwords


data = cPickle.load(open('../results/tags/tag_stat_naive.pc'))

city_info = cPickle.load(open('../temp_store/city_info.pc', 'rb'))
country_info = cPickle.load(open('../temp_store/country_info.pc', 'rb'))
admin_region_info = set(cPickle.load(open('../temp_store/admin_region_info.pc', 'rb')))

sw = stopwords.words('english')

place_tags = []
general_tags = []

for tag in data:
    tag_str = tag[0].replace('+', ' ').lower()
    tag_dry_str = ' '.join([i for i in tag_str.split() if i not in sw])
    if tag_str.lower() in city_info or tag_str.lower() in country_info or tag_str.lower() in admin_region_info:
        place_tags.append(tag)
    else:
        general_tags.append(tag)

open('../results/tags/place_tags_naive.txt', 'w').writelines([u'{} {}\n'.format(*x).encode('utf8') for x in place_tags])
open('../results/tags/general_tags_naive.txt', 'w').writelines([u'{} {}\n'.format(*x).encode('utf8') for x in general_tags])
