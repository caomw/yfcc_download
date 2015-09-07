
import json

f = open('../temp_store/wikidata-20150831-all.json')

# remove first line
f.readline()

wiki_data_en = []

cnt = 0
for line in f:
    try:
        item_json = json.loads(line[:-2])
    except:
        continue
    wiki_item = {
        'id':item_json['id'],
        'aliases':item_json['aliases']['en'] if 'en' in item_json['aliases'] else None,
        'labels':item_json['labels']['en'] if 'en' in item_json['labels'] else None,
        'descriptions':item_json['descriptions']['en'] if 'en' in item_json['descriptions'] else None
    }
    if wiki_item['labels'] is not None:
        wiki_data_en.append(wiki_item)
        cnt += 1
        if cnt % 50000 == 0:
            print "{} items parsed".format(cnt)
f.close()

json.dump(wiki_data_en, open('../temp_store/wiki_data_en.json', 'w'))

