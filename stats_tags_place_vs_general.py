__author__ = 'yjxiong'

from spark_stub import sc
import urllib
from operator import add
import cPickle
from geolib import geodist
import numpy as np

dataset = '/TMP/yjxiong/YFCC/temp_store/yfcc100m_rich_text_dataset'
out_file_template = '../results/tags/tag_stat_rich_text_{}.pc'

tag_type = 'place'

city_range = 100
city_check_pop_thresh = 400000

text_info = sc.textFile(dataset).map(lambda x: x.split('\t'))

city_info = cPickle.load(open('../temp_store/city_info.pc', 'rb'))
country_info = cPickle.load(open('../temp_store/country_info.pc', 'rb'))
admin_region_info = set(cPickle.load(open('../temp_store/admin_region_info.pc', 'rb')))


def check_place_tags(text_item):
    tags_str = text_item[8]
    tags_str_unquoted = urllib.unquote(tags_str)
    tags = tags_str_unquoted.replace('+',' ').split(',')

    cities, admins, countries = [], [], []
    places = []
    generals = []

    lon_lat = (float(text_item[10]), float(text_item[11])) if text_item[10] != '' and text_item[11] != '' else None

    for t in tags:
        if t in city_info:
            cities.append(t)
        elif t in admin_region_info:
            admins.append(t)
        elif t in country_info:
            countries.append(country_info[t])
        else:
            generals.append(t)


    for city_name in cities:
        cand = city_info[city_name]

        good_city = True

        city_id = np.argmax([c['population'] for c in cand])

        city_data = cand[city_id]
        if city_data['population'] < city_check_pop_thresh:
            # if the city has very few population, we may have to be sure it means this city
            if lon_lat is not None:
                city_dist = [geodist(lon_lat, c['lon_lat']) for c in cand]
                city_id = np.argmin(city_dist)
                city_data = cand[city_id]
                good_city = city_dist[city_id] <= city_range
            else:
                good_city = False

        if not good_city:
            # we need to check the country if not sure for the city name
            cty = [c['country'].lower() for c in cand]
            good_city = len(set(cty).intersection(countries)) != 0

            if good_city:
                city_data = filter(lambda x: x['country'].lower() in set(cty).intersection(countries), cand)[0]

        if good_city:
            places.append(city_data['name'])
        else:
            generals.append(city_name)

    places.extend(admins)
    places.extend(list(set(countries)))

    if tag_type == 'place':
        return places
    else:
        return generals


out_file = out_file_template.format(tag_type)

tag_stats = text_info.flatMap(check_place_tags).map(lambda x: (x, 1)).reduceByKey(add).sortBy(lambda x: x[1], False).collect()

cPickle.dump(tag_stats, open(out_file, 'wb'))
