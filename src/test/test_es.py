import jsonlines
import pytest
from elasticsearch import Elasticsearch

from src.etl.data_to_es import index_file, doc_generator


@pytest.fixture
def testfile():
    return'./testdoc.jsonl'

@pytest.fixture
def testidxname():
    return 'testidx'

@pytest.fixture
def es_index(testfile,testidxname):
    es = Elasticsearch([{'host': 'localhost', 'port': '9200', 'timeout': 300}])
    # testfile = '/mnt/c/Users/maaik/Documents/corpus2019/testdoc'
    # testidxname = 'testidx'
    index_file(es, testfile, testidxname)
    yield es
    es.indices.delete(testidxname)

@pytest.fixture
def jsonl_reader(testfile):

    reader = jsonlines.open(testfile)
    return reader


def test_index_created(es_index,testidxname):
    assert es_index.indices.exists(testidxname)



def test_doc_generator_index_name_updated(jsonl_reader,testidxname):
    docgen = doc_generator(jsonl_reader,testidxname)
    dictt = next(docgen)
    print(dictt)
    assert dictt['_index'] == testidxname