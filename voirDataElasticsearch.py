from datetime import datetime
from elasticsearch import Elasticsearch
es = Elasticsearch()

res = es.search(index="speed-layer-twitter", body={"query": {"match_all": {}}})
print("Got %d Hits:" % res['hits']['total']['value'])
for hit in res['hits']['hits']:
    #print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])
    print("%(date)s %(hashtag)s" % hit["_source"])

#es.delete(index="test-index", id=1)
#es.delete(index="test-index", body={"query": {"match_all": {}}})
#es.indices.delete(index='speed-layer-twitter', ignore=[400, 404])