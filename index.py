
from elasticsearch import Elasticsearch
from elasticsearch import helpers

# constants
INDEX_NAME = 'test_index'
DOC_TYPE = 'pages'
URL_CACHE = {}
INDEX = []

# create ES client, create index
es = Elasticsearch()


def create_index():

    if es.indices.exists(INDEX_NAME):
        return

    # create a new index
    request_body = {
        "settings": {
            "index": {
                "store": {
                    "type": "default"
                },
                "number_of_shards": 1,
                "number_of_replicas": 0
            }
        }
    }

    es.indices.create(index=INDEX_NAME, body=request_body)

    # create a mapping for the index
    es.indices.put_mapping(
        index=INDEX_NAME,
        doc_type=DOC_TYPE,
        body={
            DOC_TYPE: {
                'properties': {
                    'url': {'type': 'string',
                            'store': True,
                            'index': 'not_analyzed'
                            },
                    'text': {'type': 'string',
                             'store': True,
                             'index': 'analyzed',
                             'term_vector': 'with_positions_offsets_payloads',
                             },
                    'title': {'type': 'string'},
                    'html': {'type': 'string'},
                    'inlinks': {'type': 'string'},
                    'outlinks': {'type': 'string'},
                    'author': {'type': 'string'},
                    'header': {'type': 'string'}
                }
            }
        }
    )
    return


def contains(url):
    if url in URL_CACHE:
        return True
    else:
        return False


def url_count():
    return len(URL_CACHE)


def get_url(idx):
    return URL_CACHE.keys()[idx]


def add_record(record):
    global INDEX
    url = record['url']
    action = {'index': {'_index': INDEX_NAME, '_type': DOC_TYPE, '_id': url}}

    INDEX.append(action)
    INDEX.append(record)
    URL_CACHE.update({url:''})

    if len(INDEX) >= 100:
        es.bulk(index=INDEX_NAME, body=INDEX)
        del INDEX[:]

    return


def update_record(record):
    global INDEX
    url = record['url']

    prev_inlinks = []
    try:
        query = {
          "query": {
            "term": {"_id": url}
          },
          "script_fields": {"_id": {"script": "doc['_id']"}},
          "fields": ["inlinks"]
        }
        res = es.search(index=INDEX_NAME, doc_type=DOC_TYPE, body=query)
        prev_inlinks = res['hits']['hits'][0]['fields']['inlinks']
    except:
        pass

    inlinks = list(set(prev_inlinks + record['inlinks']))

    action = {'update': {'_index': INDEX_NAME, '_type': DOC_TYPE, '_id': url}}
    body = {
            "script": "ctx._source.inlinks = new_links",
            "params": {"new_links" : inlinks}
    }
    INDEX.append(action)
    INDEX.append(body)

    if len(INDEX) >= 100:
        es.bulk(index=INDEX_NAME, body=INDEX)
        del INDEX[:]
    return


def fetch_cached_urls():
    try:
        query = {
            "query": {"match_all": {}},
            "fields": []
        }
        response = helpers.scan(client=es, query=query, index=INDEX_NAME, doc_type=DOC_TYPE)
        for hit in response:
            URL_CACHE.update({hit['_id']: ''})
    except:
        pass
    return



