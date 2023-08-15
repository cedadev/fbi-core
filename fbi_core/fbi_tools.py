from datetime import datetime
from ceda_es_client import CEDAElasticsearchClient
import elasticsearch
from elasticsearch.helpers import scan
import os
import hashlib
import json
import re
from functools import lru_cache
from .conf import APIKEY

if APIKEY:
    es = CEDAElasticsearchClient(headers={'x-api-key': APIKEY})
else:
    es = CEDAElasticsearchClient()
    
indexname = "fbi-2022"

def fbi_records(after="/", stop="~", fetch_size=10000, exclude_phenomena=False, item_type=None):
    """FBI record iterator in path order. 
    
    :param str after: paths after are iterated over. Defaults to "/"
    """
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


def fbi_records_under(path, fetch_size=10000, exclude_phenomena=False, search_after="/", search_stop="/~", **kwargs):
    """FBI record iterator in path order"""
    n = 0
    search_after = search_after
    path = os.path.commonpath((search_after, search_stop))
    query = all_under_query(path, **kwargs)
    if exclude_phenomena:
        query["_source"] = {"exclude": ["phenomena"]}
    query["sort"] = [{ "path.keyword": "asc" }]
    query["size"] = fetch_size
    query["query"]["bool"]["must"].append({"range": {"path.keyword": {"gt": search_after, "lte": search_stop}}})

    for i, record in enumerate(scan(es, index=indexname, size=fetch_size, query=query, request_timeout=900)):
        if i % 1000 == 0:
            print(i, record["_source"]["path"])
        yield record["_source"]


def where_is(name, fetch_size=10000, removed=False):
    """retrun records for items named"""
    query = all_under_query("/", name_regex=name, include_removed=removed)
    query["size"] = fetch_size
    results = es.search(index=indexname, body=query)
    files = []
    for r in results["hits"]["hits"]:
        files.append(r["_source"])
    return files


def ls_query(path, include_removed=False, size=10000, **kwargs):
    """ls for fbi
    
    :param str path:
    :param **kwrargs: Any options from all_under_query
    :return list[dict]: FBI records.
    """
    query = all_under_query(path, include_removed=include_removed, **kwargs)
    
    query["size"] = size
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

def count(path, after=None, stop=None, **kwargs):
    query = all_under_query("/", **kwargs)
    if after and stop:
        query["query"]["bool"]["must"].append({"range": {"path.keyword": {"gt": after, "lte": stop}}})
    elif after:
        query["query"]["bool"]["must"].append({"range": {"path.keyword": {"gt": after}}})
    elif stop:
        query["query"]["bool"]["must"].append({"range": {"path.keyword": {"lte": stop}}})
    return es.count(index=indexname, body=query, request_timeout=900)["count"]

def all_under_query(path, location=None, name_regex=None, 
                    include_removed=False, item_type=None, ext=None,
                    since=None, before=None, 
                    audited_since=None, audited_before=None, 
                    corrupt_since=None, corrupt_before=None, 
                    with_field=None, without=None, blank=None, 
                    maxsize=None, minsize=None):
    """
    Make elastic search query for FBI records. 

    :param str path: The path to search under.
    :param str path: Media location, either on_disk or on_tape
    :param str name_regex: A regular expression to match against the file or directory name.
    :param bool include_removed: Flag to include removed items in the search.
    :param str item_type: Item type for the record. Either "file", "dir" or "link".
    :param str ext: Search on extention type. e.g. ".nc".
    :param str since: Search for items modified since this iso formated datetime. 
    :param str before: Search for items modified before this iso formated datetime. 
    :param str audited_since: Search for items audited since this iso formated datetime. 
    :param str audited_before: Search for items audited before this iso formated datetime. 
    :param str corrupt_since: Search for items corrupt since this iso formated datetime. 
    :param str corrupt_before: Search for items corrupt before this iso formated datetime. 
    :param str with_field: Search for items where this field exists.
    :param str without: Search for items where this field does not exist.
    :param str blank: Search for items where this field is an empty string.
    :param int maxsize: Search for items smaller than this size in bytes.
    :param int maxsize: Search for items larger than this size in bytes.
    :return dict: Elasticsearch query which could be used by the elacticsearch client. 
    """
    if path == "/":
        must = [{"match_all": {}}]
    else:
        path = path.rstrip("/")
        must = [{"term": {"directory.tree": {"value": path}}}]

    if include_removed:
        must_not = []
    else:
        must_not = [{"exists": {"field": "removed"}}]

    if without:
        must_not.append({"exists": {"field": without}})
    if with_field:
        must.append({"exists": {"field": with_field}})
    if blank:
        must.append({"term": {blank: {"value": ""}}})

    must_not.append({"term": {"name.keyword": {"value": ".ftpaccess" }}})
    must_not.append({"term": {"name.keyword": {"value": "00README_catalogue_and_licence.txt" }}})

    if item_type is not None:
        must.append({"term": {"type": {"value": item_type}}})

    if ext is not None:
        must.append({"term": {"ext": {"value": ext}}})

    if maxsize is not None:
        must.append({"range": {"size": {"lte": maxsize}}})

    if minsize is not None:
        must.append({"range": {"size": {"gte": minsize}}})

    if name_regex is not None:
        must.append({"regexp": {"name.keyword": {"value": name_regex, "flags": "ALL",
                    "max_determinized_states": 1000, "rewrite": "constant_score"}}})

    if since is not None:
        must.append({"range": {"last_modified": {"gte": since}}})

    if before is not None:
        must.append({"range": {"last_modified": {"lte": before}}})

    if audited_since is not None:
        must.append({"range": {"last_audit": {"gte": audited_since}}})

    if audited_before is not None:
        must.append({"range": {"last_audit": {"lte": audited_before}}})

    if corrupt_since is not None:
        must.append({"range": {"corrupted": {"gte": corrupt_since}}})

    if corrupt_before is not None:
        must.append({"range": {"corrupted": {"lte": corrupt_before}}})


    if location is not None:
        must.append({"term": {"location": location}})

    return {"query": {"bool": {"must": must, "must_not": must_not }}}    

def lastest_file(directory):
    """latest file record of last updated file under a path.
    
    :param str directory: path to search for last updated file
    :return dict or None: Record for the last updated file.
    """
    query = all_under_query(directory, item_type="file")
    query["sort"] = [{"last_modified": {"order": "desc"}}]
    query["size"] = 1
    result = es.search(index=indexname, body=query, request_timeout=900)
    if len(result["hits"]["hits"]) == 0:
        return None
    last_record = result["hits"]["hits"][0]["_source"]
    if "last_modified" in last_record:
        return last_record
    else:
        return None

def convert2datetime(d):
    """Convert a str or int to a datetime"""
    if isinstance(d, int):
        return datetime.fromtimestamp(d)
    else:
        return datetime.fromisoformat(d)   

def last_updated(directory):  
    lfile = lastest_file(directory)
    if lfile is None:
        return None
    return convert2datetime(lfile["last_modified"])

@lru_cache(maxsize=1024)
def fbi_count_in_dir2(directory, item_type=None):
    """FBI record counter"""
    query = all_under_query(directory, item_type=item_type)
    count = es.count(index=indexname, body=query, request_timeout=900)["count"]
    return count

def get_random_records(path, number, **kwargs):   
    query = all_under_query(path, **kwargs)
    # print(json.dumps(query, indent=4))
    query["random_score"] = {}
    query["boost_mode"] = "replace"
    query = {"query": {"function_score": query}, "size": number}
    results = es.search(index=indexname, body=query, request_timeout=900)
    recs = []
    for r in results["hits"]["hits"]:
        recs.append(r["_source"])
    recs.sort(key=lambda x: x["path"])
    return recs

def get_random(path, number, **kwargs): 
    recs = get_random_records(path, number, **kwargs)
    return list(map(lambda x: x["path"], recs))

def archive_summary(path, max_types=5, max_vars=1000, max_exts=10,  
                    include_removed=False, **kwargs):
    """find summary info for the archive below a path."""
    query = all_under_query(path, include_removed=include_removed, **kwargs)

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

def random_splits(path, nbatchs=20, **kwargs):
    paths = get_random(path, nbatchs, **kwargs)
    paths.append("/~~")
    paths.insert(0, "/")
    splits = []
    for i, path in enumerate(paths[:-1]):
        c = count(path, after=path, stop=paths[i+1], **kwargs)
        print(c, path)
        splits.append((path, paths[i+1], c))

    return splits

def splits(batch_size=10000000, root_path="/"):
    splits = [(root_path, fbi_count_in_dir2(root_path))]
    while True:
        new_splits = split(splits, batch_size=batch_size)
        if len(splits) == len(new_splits):
            break
        splits = new_splits
 
    splits.sort()
    merged = []
    count = 0
    after = root_path
    for d, c in splits:
        if count + c > batch_size:
            merged.append((after, d, count))
            after = d
            count = 0
        count += c
    merged.append((after, root_path+"~", count))

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

def dir_annotations(path):
    query = {"query": { "bool": {"must":   
                [{"term": {"type": {"value": "dir_annotation"}}}],
                "should": [], "minimum_should_match": 1}}}
 
    while path != "/":
        query["query"]["bool"]["should"].append({"term": {"path.keyword":   {"value": path}}})
        path = os.path.dirname(path)

    results = es.search(index=indexname, body=query, request_timeout=90)
    records = []
    for r in results["hits"]["hits"]:
        records.append(r["_source"])
    return records


def insert_annotation(record_id, record):
    """Insert annotation record by replaceing it"""
    try: 
        es.delete(index=indexname, id=record_id)
    except elasticsearch.exceptions.NotFoundError:
        pass 
    es.index(index=indexname, id=record_id, body=record, request_timeout=100)   

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

def get_record_attr(path, attr):
    """return a single attribute value from the record of a path"""
    rec = get_record(path)
    return rec.get(attr)

def get_records_by_content(md5, filename=None, under=None, include_removed=False):
    """Get records with content that matches an md5. 
    Optionaly make it match a filename and a parent directory."""
    if under == "/" or under is None:
        must = [{"match_all": {}}]
    else:
        under = under.rstrip("/")
        must = [{"term": {"directory.tree": {"value": under}}}]

    must.append({"term": {"md5": {"value": md5}}})

    if include_removed:
        must_not = []
    else:
        must_not = [{"exists": {"field": "removed"}}]
    
    if filename is not None:
        must.append({"term": {"name.keyword": { "value": filename}}})

    query = {"query": {"bool": {"must": must, "must_not": must_not}}}
    results = es.search(index=indexname, body=query, request_timeout=90)
    records = []
    for r in results["hits"]["hits"]:
        records.append(r["_source"])
    return records

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

def parameters(directory, removed=False, hidden=False):
    """return list of unique parameters under a directory"""
    must = [{"term": {"directory.tree": {"value": directory}}}]
    must_not = []
    if not removed:
        must_not.append({"exists": {"field": "removed"}})
    if not hidden:
        must_not.append({"regexp": {"name.keyword": "[.].*"}})              
    query = {"bool": {"must": must, "must_not": must_not}}

    aggs = {"parameters": {
              "terms": {"field": "phenomena.agg_string.keyword", "size": 10000},
              "aggs": {"object_src": {"top_hits": {"size": 1}}}
            }}
 
    query = {"bool": {"must": must, "must_not": must_not}}
    body = {"query": query, "aggs": aggs, "size": 0}
    result = es.search(index=indexname, body=body, request_timeout=900)

    parameter_aggs = result["aggregations"]["parameters"]

    parameter_dict = {}
    cf_units_pattern = re.compile("(.*?)\s+[Ss]ince")
    for bucket in parameter_aggs["buckets"]:
        agg_string = bucket["key"]
        phenomena = bucket["object_src"]["hits"]["hits"][0]["_source"]["phenomena"]
        for phen in phenomena:
            if phen["agg_string"] == agg_string:
                var_id = phen.get("var_id")
                standard_name = phen.get("standard_name")
                long_name = phen.get("long_name")
                units = phen.get("units")

                if units is not None:
                    m = cf_units_pattern.match(units)
                    if  m:
                        units = m.group(1)

                para_key = f"{var_id} | {standard_name} | {long_name} | {units}"
                if para_key not in parameter_dict:
                    parameter_dict[para_key] = {"file_count": 0}
                if var_id is not None: parameter_dict[para_key]["var_id"] = var_id
                if standard_name is not None: parameter_dict[para_key]["standard_name"] = standard_name
                if long_name is not None: parameter_dict[para_key]["long_name"] =long_name
                if units is not None: parameter_dict[para_key]["units"] = units
                parameter_dict[para_key]["file_count"] += bucket["doc_count"]
              
                break

    return list(parameter_dict.values())

if __name__ == "__main__":   
     parameters("/badc/cmip5")

