from src.interface.corpus import Corpus
from src.interface.features import FeatureEngineer
from src.interface.iohandler import InputOutputHandler

corpus2019 = Corpus(index='semanticscholar2019')
ft2019 = FeatureEngineer(corpus2019,
                         fquery='/mnt/c/Users/maaik/Documents/thesis/resources/elasticsearch-ltr-config/featurequery_bonart.json',
                         fconfig='/mnt/c/Users/maaik/Documents/thesis/resources/elasticsearch-ltr-config/features_bonart.json')

corpus2019og = Corpus()
ft2019og = FeatureEngineer(corpus2019og)

q = "./training/fair-TREC-training-sample-cleaned.json"
seq = "./training/training-sequence.tsv"


input2019 = InputOutputHandler(corpus2019,
                               fsequence=seq,
                               fquery=q)

input2019og = InputOutputHandler(corpus2019og,
                                 fsequence=seq,
                                 fquery=q)

features2019 = ft2019.get_feature_mat(input2019)
features20192 = ft2019.get_feature_mat(input2019)
features2019og = ft2019og.get_feature_mat(input2019og)
features2019og2 = ft2019og.get_feature_mat(input2019og)
features2019