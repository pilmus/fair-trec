import glob
import os

import tqdm

from elasticsearch import Elasticsearch
from elasticsearch import helpers
import jsonlines
from pathlib import Path
import gzip
import src.utils.logger as logger
import re
import pandas as pd



def doc_generator(reader):
    for doc in reader.iter(type=dict, skip_invalid=True):
        author_names = []
        author_ids = []
        for obj in doc.get('authors'):
            author_ids.extend(obj.get('ids'))
            author_names.append(obj.get('name'))

        yield {
            "_index": 'semanticscholar2019og',
            "_type": "document",
            "_id": doc.get('id'),
            "title": doc.get('title'),
            "paperAbstract": doc.get("paperAbstract"),
            "entities": doc.get("entities"),
            "author_names": author_names,
            "author_ids": author_ids,
            "inCitations": len(doc.get("inCitations")),
            "outCitations": len(doc.get("outCitations")),
            "year": doc.get("year"),
            "venue": doc.get('venue'),
            "journalName": doc.get('journalName'),
            "journalVolume": doc.get('journalVolume'),
            "sources": doc.get('sources'),
            "doi": doc.get('doi')
        }


def already_indexed(indexed_files):
    if not os.path.exists(indexed_files):
        with open(indexed_files, "w"):
            pass
    with open(indexed_files, "r") as fp:
        return fp.read().splitlines()


es = Elasticsearch([{'host': 'localhost', 'port': '9200', 'timeout': 300}])

logdir = "log/"





rawspath = f'/mnt/c/Users/maaik/Documents/corpus2019/*'
raw_files = glob.glob(rawspath)

print(f"Raw files: {raw_files}.")

indexed_filepath = os.path.join(logdir, f'indexed_files_2019og.txt')
indexed_files = already_indexed(indexed_filepath)

print(f"Already indexed: {indexed_files}.")
for raw in raw_files:
    if raw not in indexed_files:
        print(f"Indexing contents of {raw}.")
        with jsonlines.open(raw) as reader:
            progress = tqdm.tqdm(unit="docs", total=1000000)
            successes = 0
            for ok, action in helpers.streaming_bulk(es, doc_generator(reader), chunk_size=100):
                progress.update(1)
                successes += ok

        with open(indexed_filepath, "a") as fp:
            fp.write(f"{raw}\n")
            print(f"Indexed contents of {raw}.")
#
# for file in path.iterdir():
#     if file.suffix == ".gz":
#         if file.name not in processed:
#             log.info("process file %s", file)
#             with gzip.open(str(file)) as f:
#                 reader = jsonlines.Reader(f)
#                 for success, info in helpers.parallel_bulk(es, doc_generator(reader),
#                                                            chunk_size=100, max_chunk_bytes=1000 * 1000 * 25,
#                                                            request_timeout=30):
#                     if not success: log.errror('doc failed', info)
