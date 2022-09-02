from datetime import datetime
from ceda_es_client import CEDAElasticsearchClient
import elasticsearch
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


def ls_query(path, location=None, name_regex=None):
    """ls for fbi"""
    q4p = {"query": {"bool": {
            "must": [{"term": {"directory.tree": path}}],
            "must_not": [{"exists": {"field": "removed"}}]
            }}}

    if name_regex is not None:
        q4p["query"]["bool"]["must"].append({"regexp": {"name.keyword": {
                "value": name_regex, "flags": "ALL",
                "max_determinized_states": 1000, "rewrite": "constant_score"}}})
    q4p["size"] = 10000
    if location is not None:
        q4p["query"]["bool"]["must"].append({"term": {"location": location}})

    results = es.search(index=indexname, body=q4p)

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

@lru_cache(maxsize=1024)
def fbi_count_in_dir2(directory):
    """FBI record counter"""
    if directory == "/":
        must_query = {"match_all": {}}
    else:
        must_query = {"term": {"directory.tree": {"value": directory}}}
    query = {"query": {"bool": {"must": [must_query], "must_not": [{"exists": {"field": "removed"}}] }}}
    count = es.count(index=indexname, body=query, request_timeout=900)["count"]
    return count


def archive_summary(path, max_types=5, max_vars=1000, max_exts=10, location=None, name_regex=None, include_removed=False):
    """find summary info for the archive below a path."""
    if path == "/":
        must = [{"match_all": {}}]
    else:
        must = [{"term": {"directory.tree": {"value": path}}}]

    if include_removed:
        must_not = []
    else:
        must_not = [{"exists": {"field": "removed"}}]

    if name_regex is not None:
        must.append({"regexp": {"name.keyword": {"value": name_regex, "flags": "ALL",
                    "max_determinized_states": 1000, "rewrite": "constant_score"}}})

    if location is not None:
        must.append({"term": {"location": location}})

    query = {"query": {"bool": {"must": must, "must_not": must_not }}}

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


def split(directory, after, batch_size):
    print(directory, after)
    # list dir
    subdirs = []

    for record in fbi_listdir(directory, dirs_only=True):
        path = record["path"]
        if path + "~" > after: 
            subdirs.append(path)

    # for each sub dir count from after to end sub dir:
    count = fbi_count(after=after, stop = directory)
    for subdir in subdirs:        
        count +=  fbi_count_in_dir(subdir)
        print("- ", subdir, count)    
        #  if too much then split that dir
        if count > batch_size: 
            print(f'recurse subdir {subdir}')
            return split(subdir, after, batch_size) 
        #  if enough then return split
        if count > batch_size * 0.5:
            print(f'Good batch {count}, {subdir + "~"}')
            return subdir + "~", count
 
    # if still not found enoough return whole directory
    count = fbi_count(after=after, stop = directory + "~") 
    print(f'return as could not find enough {directory + "~", count}')
    return directory + "~", count


def splits(batch_size=10000000):
    after = "/"
    splitlist = []
    while True:
        stop, count = split("/", after, batch_size=batch_size)
        splitlist.append((after, stop, count))
        print(f" +++++++ BATCH {after} -> {stop} [{count}]")
        after = stop
        if after == "/~": break
    return splitlist          


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
    n = 0
    after = ""
    sort = [{"name.keyword": "asc"}]
    while True:
        must = [{"term": {"directory.keyword": {"value": directory}}}, 
                {"range": {"name.keyword": {"gt": after}}}]
        if dirs_only:
            must.append({"term": {"type": {"value": "dir"}}})
        
        must_not = []
        if not removed:
            must_not.append({"exists": {"field": "removed"}})
        if not hidden:
            must_not.append({"regexp": {"name.keyword": "[.].*"}})              

        query = {"bool": {"must": must, "must_not": must_not}}

        result = es.search(index=indexname, sort=sort, size=fetch_size, query=query, request_timeout=900)
        nfound = len(result["hits"]["hits"])
        n += nfound
        if nfound == 0:
            break
        else:
            after = result["hits"]["hits"][-1]["_source"]["name"]
            for record in result["hits"]["hits"]:
                yield record["_source"]


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

