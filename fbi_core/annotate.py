import datetime
import hashlib
import os
import re
from collections import defaultdict

import elasticsearch
from elasticsearch import Elasticsearch
import requests

from conf import load_config

api_key, host_es, es_index, es_annotation = load_config()

if api_key:
    es = Elasticsearch(host_es, headers={"x-api-key": api_key})
else:
    es = Elasticsearch()

if es_annotation:
    indexname = es_annotation
else:
    indexname = "fbi-annotations"


def get_moles_records():
    """get moles info from catalogue"""
    obs_url = "https://catalogue.ceda.ac.uk/api/v2/observations.json"
    obs_url += "/?fields=uuid,title,result_field,status,observationcollection_set"
    obs_url += "&limit=20000"
    r = requests.get(obs_url, timeout=300)

    # map collections to paths
    observation_records = {}
    collections_paths = defaultdict(list)
    for ob_record in r.json()["results"]:
        # skip records with no results field
        if "result_field" not in ob_record or ob_record["result_field"] is None:
            continue
        # skip records with bad data path
        if "dataPath" not in ob_record["result_field"] or not ob_record["result_field"][
            "dataPath"
        ].startswith("/"):
            continue
        data_path = ob_record["result_field"]["dataPath"]

        # description records are observation records mostly
        observation_records[data_path] = ob_record

        for collection_url in ob_record["observationcollection_set"]:
            coll_id = int(re.search(r"/(\d+)\.json$", collection_url).group(1))
            collections_paths[coll_id].append(data_path)

    # find collections common paths
    collections_common_paths = {}
    for coll_id, paths in collections_paths.items():
        collections_common_paths[coll_id] = os.path.commonpath(paths)

    # invert the dict
    common_paths_collections = defaultdict(list)
    for coll_id, common_path in collections_common_paths.items():
        common_paths_collections[common_path].append(coll_id)

    # find unambiguous common paths to map a common path to single collection
    collections_by_path = {}
    for common_path, collections_list in common_paths_collections.items():
        if len(collections_list) == 1:
            collections_by_path[common_path] = collections_list[0]

    # grab collections records
    coll_url = "https://catalogue.ceda.ac.uk/api/v2/observationcollections.json"
    coll_url += "/?fields=ob_id,uuid,title,publicationState"
    coll_url += "&limit=10000"
    r = requests.get(coll_url, timeout=200)
    
    collection_records_by_obid = {}
    for collection_rec in r.json()["results"]:
        collection_records_by_obid[collection_rec["ob_id"]] = collection_rec

    #  swap collection id for full collection record
    for path, coll_id in collections_by_path.items():
        collections_by_path[path] = collection_records_by_obid[coll_id]

    return observation_records, collections_by_path


def insert_annotation(path, annotation_type, record, process=None):
    """Insert record by replaceing it"""
    path = os.path.normpath(path)
    key = path + "|" + annotation_type
    record_id = hashlib.sha1(key.encode()).hexdigest()
    annotation_record = {
        "added_date": datetime.datetime.now().isoformat(),
        "under": path,
        "annotation": {annotation_type: record},
    }
    if isinstance(process, str):
        annotation_record["added_process"] = process

    try:
        es.delete(index=indexname, id=record_id)
    except elasticsearch.exceptions.NotFoundError:
        pass
    es.index(index=indexname, id=record_id, body=annotation_record, request_timeout=100)


def get_fbi_annotations(path):
    pass


def lookup(path):

    path = os.path.normpath(path)
    matches = []
    while path != "/":
        print(path)
        if path in records:
            matches.insert(0, records[path])
        path = os.path.dirname(path)

    combined_rec = {}
    for rec in matches:
        combined_rec.update(rec)
    return combined_rec


def grab_moles():

    obs, collections = get_moles_records()
    for ob_path, ob_rec in obs.items():
        print(ob_path, ob_rec)
        insert_annotation(ob_path, "observation", ob_rec)

    for coll_path, coll_rec in collections.items():
        print(coll_path, coll_rec)
        insert_annotation(coll_path, "collection", coll_rec)
