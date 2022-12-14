from datetime import datetime
from ceda_es_client import CEDAElasticsearchClient
import elasticsearch
from elasticsearch.helpers import scan
import os
import hashlib
import json
from functools import lru_cache
from .conf import APIKEY

if APIKEY:
    es = CEDAElasticsearchClient(headers={'x-api-key': APIKEY})
else:
    es = CEDAElasticsearchClient()
    
indexname = "fbi-2022"

def fbi_records(after="/", stop="~", fetch_size=10000, exclude_phenomena=False, item_type=None):
    """FBI record iterator in path order"""
    n = 0
    current_stop = stop
    while True:
        query = {
            "sort" : [{ "path.keyword": "asc" }],
            "query": {"bool": {"must": [{"range": {"path.keyword": {"gt": after, "lte": current_stop}}}],
                               "must_not": [{"exists": {"field": "removed"}}] }},
            "size": fetch_size}
        if exclude_phenomena:
            query["_source"] = {"exclude": ["phenomena"]}
        if item_type:
            query["query"]["bool"]["must"].append({"term": {"type": {"value": item_type}}})
        result = es.search(index=indexname, body=query, request_timeout=900)
        nfound = len(result["hits"]["hits"])
        n += nfound

        if nfound == 0 and current_stop >= stop:
            break

        if nfound == 0:
            current_stop = min(os.path.dirname(current_stop) + "~", stop)
        else:
            after = result["hits"]["hits"][-1]["_source"]["path"]
            current_stop = min(os.path.dirname(after) + "~", stop)
            for record in result["hits"]["hits"]:
                yield record["_source"]


def where_is(name, fetch_size=10000, removed=False):
    """retrun records for items named"""
    query = all_under_query("/", name_regex=name, include_removed=removed)
    query["size"] = 10000
    results = es.search(index=indexname, body=query)
    files = []
    for r in results["hits"]["hits"]:
        files.append(r["_source"])
    return files


def ls_query(path, location=None, name_regex=None, 
             item_type=None, include_removed=False,):
    """ls for fbi"""
    query = all_under_query(path, location=location, name_regex=name_regex, 
                            include_removed=include_removed, item_type=item_type)
    
    query["size"] = 10000
    results = es.search(index=indexname, body=query)
    files = []
    for r in results["hits"]["hits"]:
        files.append(r["_source"])

    return files

def fbi_count(after="/", stop="~", item_type=None):
    """FBI record counter"""
    query = {"query": {"bool": {"must": [{"range": {"path.keyword": {"gt": after, "lte": stop}}}],
                               "must_not": [{"exists": {"field": "removed"}}] }}}
    if item_type:
        query["query"]["bool"]["must"].append({"term": {"type": {"value": item_type}}})
    count = es.count(index=indexname, body=query, request_timeout=900)["count"]
    return count

@lru_cache(maxsize=1024)
def fbi_count_in_dir(directory, item_type=None):
    return fbi_count(after=directory, stop=directory + "/~", item_type=item_type)

def all_under_query(path, location=None, name_regex=None, 
                    include_removed=False, item_type=None):
    if path == "/":
        must = [{"match_all": {}}]
    else:
        must = [{"term": {"directory.tree": {"value": path}}}]

    if include_removed:
        must_not = []
    else:
        must_not = [{"exists": {"field": "removed"}}]

    if item_type is not None:
        must.append({"term": {"type": {"value": item_type}}})

    if name_regex is not None:
        must.append({"regexp": {"name.keyword": {"value": name_regex, "flags": "ALL",
                    "max_determinized_states": 1000, "rewrite": "constant_score"}}})

    if location is not None:
        must.append({"term": {"location": location}})

    return {"query": {"bool": {"must": must, "must_not": must_not }}}    

@lru_cache(maxsize=1024)
def fbi_count_in_dir2(directory, item_type=None):
    """FBI record counter"""
    query = all_under_query(directory, item_type=item_type)
    count = es.count(index=indexname, body=query, request_timeout=900)["count"]
    return count


def archive_summary(path, max_types=5, max_vars=1000, max_exts=10, location=None, 
                    name_regex=None, include_removed=False, item_type=None):
    """find summary info for the archive below a path."""
    query = all_under_query(path, location=location, name_regex=name_regex, 
                            include_removed=include_removed, item_type=item_type)

    query["size"] = 0
    query["aggs"] = {"size_stats":{"stats":{"field":"size"}},
                      "types": {"terms": {"field": "type", "size": max_types}},
                      "exts": {"terms": {"field": "ext", "size": max_exts}},
                      "vars": {"terms": {"field": "phenomena.best_name.keyword", "size": max_vars}}}

    result = es.search(index=indexname, body=query, request_timeout=900)
    aggs = result["aggregations"]
    ret = {"size_stats": aggs["size_stats"]}

    for agg_name in ("types", "exts", "vars"):
        agg_list = []
        for bucket in aggs[agg_name]["buckets"]:
            agg_list.append((bucket["key"], bucket["doc_count"]))
        ret[agg_name] = agg_list

    return ret


def count_from(directory, from_dir):
    count = 0
    for record in fbi_listdir(directory, dirs_only=True): 
        subdir = record["path"]     
        if subdir > from_dir:
            continue
        count += fbi_count_in_dir2(subdir)
    return count

def next_dir(directory):
    parent = os.path.dirname(directory)
    for record in fbi_listdir(parent):
        subdir = record["path"]
        if subdir > directory: 
            return subdir
    return next_dir(parent)         


def split(splitlist, batch_size):
    new_splits = []
    for directory, count in splitlist:
        if count > batch_size:
            subdirs = fbi_listdir(directory, dirs_only=True)
            for subdir in subdirs:
                subdir_path = subdir["path"]
                sub_count = fbi_count_in_dir2(subdir_path)
                new_splits.append((subdir_path, sub_count))
                count -= sub_count
        new_splits.append((directory, count))
    return new_splits

def splits(batch_size=10000000):
    splits = [("/", fbi_count_in_dir2("/"))]
    while True:
        new_splits = split(splits, batch_size=batch_size)
        if len(splits) == len(new_splits):
            break
        splits = new_splits
 
    splits.sort()
    merged = []
    count = 0
    after = "/"
    for d, c in splits:
        if count + c > batch_size:
            merged.append((after, d, count))
            after = d
            count = 0
        count += c
    merged.append((after, "~", count))

    for d, s, c in merged:
        print(d, s, c)
    return merged

def make_dirs(directory):
    while True:
        try:
            fbi_rec = get_record(directory)
            break
        except elasticsearch.exceptions.NotFoundError:
            parent, name = os.path.split(directory)
            
            last_modified = datetime.now()
            record = {"path": directory, "directory": parent, "name": name, "last_modified": last_modified , "type": "dir"}
            print(f"Adding: {record['path']}")
            update_item(record)
            directory = parent


def fbi_listdir(directory, fetch_size=10000, dirs_only=False, removed=False, hidden=True):
    """FBI record iterator for a directory"""
    must = [{"term": {"directory.keyword": {"value": directory}}}]
    if dirs_only:
        must.append({"term": {"type": {"value": "dir"}}})
    
    must_not = []
    if not removed:
        must_not.append({"exists": {"field": "removed"}})
    if not hidden:
        must_not.append({"regexp": {"name.keyword": "[.].*"}})              

    query = {"bool": {"must": must, "must_not": must_not}}
    result = []
    for item in scan(es, index=indexname, size=fetch_size, query={"query":query}, request_timeout=900):
        result.append(item["_source"])

    result.sort(key=lambda q: q["name"])    
    return result


def insert_item(record):
    """Insert record by replaceing it"""
    record_id = _create_id(record["path"])
    try: 
        es.delete(index=indexname, id=record_id)
    except elasticsearch.exceptions.NotFoundError:
        pass 
    es.index(index=indexname, id=record_id, body=record, request_timeout=100)    

def update_item(record):
    """Update a single document - overwrite feilds in record suplied."""
    document = {'doc': record, 'doc_as_upsert': True}
    es.update(index=indexname, id=_create_id(record["path"]), body=document, request_timeout=100)

def flag_removed(record):
    fbi_rec = get_record(record["path"])
    if fbi_rec is None:
        return
    fbi_rec["removed"] = record["last_modified"]
    document = {'doc': fbi_rec, 'doc_as_upsert': True}
    es.update(index=indexname, id=_create_id(record["path"]), body=document)

def get_record(path):
    try: 
        record = es.get_source(index=indexname, id=_create_id(path), request_timeout=100)
    except elasticsearch.exceptions.NotFoundError:
        return None
    return dict(record)    

def _create_id(path):
    return hashlib.sha1(path.encode()).hexdigest()

def bulk_update(records):
    """Update a list of records"""
    body = ''
    for record in records:
        body += json.dumps({"update": {"_index": indexname, "_id": _create_id(record["path"])}}) + "\n"
        body += json.dumps({"doc": record, "doc_as_upsert": True}) + "\n"
        #add_item(record)
    print(body)
    print(es.bulk(index=indexname, body=body, refresh=True))

def nla_dirs(after="/", stop="/~", fetch_size=10000):
    """FBI record iterator for nla directories"""
    n = 0
    current_stop = stop
    while True:
        query = {"sort" : [{ "path.keyword": "asc" }],
                 "query": {"bool": {"must": [
                    {"range": {"path.keyword": {"gt": after, "lte": current_stop}}},
                    {"term": {"name.keyword": {"value": "00FILES_ON_TAPE"}}}
                 ]}},
                "size": fetch_size}

        result = es.search(index=indexname, body=query, request_timeout=900)
        nfound = len(result["hits"]["hits"])
        n += nfound

        print(nfound, current_stop, stop)
        if nfound == 0 and current_stop >= stop:
            break

        if nfound == 0:
            current_stop = min(os.path.dirname(current_stop) + "~", stop)
        else:
            after = result["hits"]["hits"][-1]["_source"]["path"]
            current_stop = min(os.path.dirname(after) + "~", stop)
            for record in result["hits"]["hits"]:
                yield record["_source"]["directory"]

if __name__ == "__main__":   
     splits()

