import jsonlines
import pytest
from elasticsearch import Elasticsearch

from src.etl.data_to_es import index_file, doc_generator

# todo: test for having initialized the featurestore
from src.interface.corpus import Corpus
from src.interface.features import FeatureEngineer


@pytest.fixture
def testfile():
    return './testdoc.jsonl'


@pytest.fixture
def testfile_smaller():
    return './testdoc2.jsonl'


@pytest.fixture
def testidxname():
    return 'testidx'


@pytest.fixture
def testidxname2():
    return 'testidx2'


@pytest.fixture
def es_index(testfile, testidxname):
    es = Elasticsearch([{'host': 'localhost', 'port': '9200', 'timeout': 300}])
    index_file(es, testfile, testidxname)
    yield es
    es.indices.delete(testidxname)


@pytest.fixture
def es_index_smaller(testfile_smaller, testidxname2):
    es = Elasticsearch([{'host': 'localhost', 'port': '9200', 'timeout': 300}])
    index_file(es, testfile_smaller, testidxname2)
    yield es
    es.indices.delete(testidxname2)


@pytest.fixture
def jsonl_reader(testfile):
    reader = jsonlines.open(testfile)
    return reader


@pytest.fixture
def featurequery(testidxname):
    fe = FeatureEngineer(Corpus(index=testidxname))
    return fe.query


@pytest.fixture
def featureengineer(testidxname):
    return FeatureEngineer(Corpus(index=testidxname), fquery='../../config/featurequery.json',
                           fconfig='../../config/features.json')


@pytest.fixture
def featureengineer(testidxname):
    return FeatureEngineer(Corpus(index=testidxname), fquery='../../config/featurequery.json',
                           fconfig='../../config/features.json')


@pytest.fixture
def featureengineer_smaller(testidxname2):
    return FeatureEngineer(Corpus(index=testidxname2), fquery='../../config/featurequery.json',
                           fconfig='../../config/features.json')


def test_index_created(es_index, testidxname):
    assert es_index.indices.exists(testidxname)


def test_doc_generator_index_name_updated(jsonl_reader, testidxname):
    docgen = doc_generator(jsonl_reader, testidxname)
    dictt = next(docgen)
    print(dictt)
    assert dictt['_index'] == testidxname


def test_compare_bm25_scores_fewer_and_more_fields(es_index, es_index_smaller, featureengineer,
                                                   featureengineer_smaller):
    features1 = featureengineer._FeatureEngineer__get_features("test", ["1"])
    features2 = featureengineer_smaller._FeatureEngineer__get_features("test", ["1"])
    assert features1.title_score[0] == features2.title_score[0]

# todo: test that all features are there, num columns == what we expect for example
# todo: test that 2019og, 2020og have correct size (46947044, ...

# todo: test for correct mapping after indexing

# todo: sanchecks: range of dates between 1900 and 2022, numcitations >= 0,