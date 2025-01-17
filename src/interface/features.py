import pandas as pd
from tqdm import tqdm

import src.utils.io as io


class FeatureEngineer():
    """Returns feature vectors for provided query-doc_ids pairs"""

    def __init__(self, corpus, fquery="./config/featurequery.json", fconfig='./config/features.json', init_ltr=True):
        self.corpus = corpus
        self.query = io.read_json(fquery)
        if init_ltr:
            corpus.init_ltr(fconfig)

    def __get_features(self, queryterm, doc_ids):
        self.query['query']['bool']['filter'][0]['terms']['_id'] = doc_ids
        self.query['query']['bool']['filter'][1]['sltr']['params']['keywords'] = queryterm
        docs = self.corpus.es.search(index=self.corpus.index, body=self.query, size=len(doc_ids))
        resp = self.__features_from_response(docs)
        resp['qlength'] = len(queryterm)
        return resp

    @property
    def log_field_name(self):
        return self.query["ext"]["ltr_log"]["log_specs"]["name"]

    def get_feature_mat(self, iohandler):
        print("Getting features...")
        tqdm.pandas()
        features = iohandler.get_query_seq().groupby('qid').progress_apply(
            lambda df: self.__get_features(df['query'].iloc[0], df['doc_id'].unique().tolist()))
        features.reset_index(inplace=True, level=0)
        return (features)

    def __features_from_response(self, docs):
        docs = docs['hits']['hits']
        features = [doc['fields']['_ltrlog'][0][self.log_field_name] for doc in docs]
        ids = [doc['_id'] for doc in docs]
        result = []
        for i, vec in enumerate(features):
            vec = {el['name']: el['value'] for el in vec}
            vec['doc_id'] = ids[i]
            result.append(vec)
        return pd.DataFrame.from_dict(result)
