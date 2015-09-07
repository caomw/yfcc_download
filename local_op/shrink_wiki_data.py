__author__ = 'yjxiong'

import json

json_data = json.load(open('../../temp_store/wiki_data_en.json'))

wiki_compact_data = []

cnt = 0
for item in json_data:
    compact_item = {
        'id': item['id'],
        'descriptions': item['descriptions']['value'] if item['descriptions'] is not None else None,
        'aliases': [x['value'] for x in item['aliases']] if item['aliases'] is not None else None,
        'labels': item['labels']['value']
    }
    wiki_compact_data.append(compact_item)
    cnt += 1
    if cnt % 100000 == 0:
        print cnt

json.dump(wiki_compact_data, open('../../temp_store/wiki_data_en_compact.json','w'))
