__author__ = 'yjxiong'

# from spark_stub import  sc
import cPickle
import nltk
import re
from nltk.corpus import stopwords


data = cPickle.load(open('../results/tags/tag_stat_naive.pc'))

place_data = set([x.strip() for x in open('../temp_store/place_name_list.txt').readlines()])
sw = stopwords.words('english')

place_tags = []
general_tags = []

for tag in data:
    tag_str = tag[0].replace('+', ' ').lower()
    tag_dry_str = ' '.join([i for i in tag_str.split() if i not in sw])
    if tag_str in place_data or tag_dry_str in place_data:
        place_tags.append(tag)
    else:
        general_tags.append(tag)

open('../results/tags/place_tags_naive.txt', 'w').writelines([u'{} {}\n'.format(*x).encode('utf8') for x in place_tags])
open('../results/tags/general_tags_naive.txt', 'w').writelines([u'{} {}\n'.format(*x).encode('utf8') for x in general_tags])
