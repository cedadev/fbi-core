from datetime import datetime
from ceda_es_client import CEDAElasticsearchClient
import elasticsearch
from elasticsearch.helpers import scan, bulk
import os
import hashlib
import json
import re
from .conf import APIKEY, indexname

if APIKEY:
    es = CEDAElasticsearchClient(headers={'x-api-key': APIKEY})
else:
    es = CEDAElasticsearchClient()


def fbi_records(after="/", stop="~", fetch_size=10000, exclude_phenomena=False, item_type=None, **kwargs):
    """
    FBI record iterator. The is implicitly in path order. 
    
    :param str after: paths after this are iterated over. Defaults to "/"
    :param str stop: iteration stops when the path is greater than or equal to this. Defaults to "~" 
    :param int fetch_size: The number of records to request from elasticsearch at a time.
    :param bool exclude_phenomena: remove the bulky phenomena attribute from the record. Default is False.
    :param str item_type: Item type for the records. Either "file", "dir" or "link". Defaults to all types.

    :return iterator[dict]: Yeilds FBI records as dictionaries. 
    """
    n = 0
    current_stop = stop
    while True:
        sort = [{ "path.keyword": "asc" }]
        query = {"bool": {"must": [{"range": {"path.keyword": {"gt": after, "lte": current_stop}}}],
                               "must_not": [{"exists": {"field": "removed"}}] }}
        if exclude_phenomena:
            query["_source"] = {"exclude": ["phenomena"]}
        if item_type:
            query["bool"]["must"].append({"term": {"type": {"value": item_type}}})
        result = es.search(index=indexname, query=query, sort=sort, size=fetch_size, request_timeout=900)
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


def fbi_records_under(path="/", fetch_size=10000, exclude_phenomena=False, **kwargs):
    """FBI record iterator in path order"""
    n = 0
    search_after = [path]
    current_scope = path
    #path = os.path.commonpath((search_after, search_stop))
    query = all_under_query(path, **kwargs)
    if exclude_phenomena:
        query["_source"] = {"exclude": ["phenomena"]}
    sort = [{ "path.keyword": "asc" }]

    while True:
        result = es.search(index=indexname, query=query, size=fetch_size, sort=sort, request_timeout=900, search_after=search_after)
        nfound = len(result["hits"]["hits"])
        if nfound == 0 and current_scope == path:
            break
        if nfound == 0: 
            # expand scope
            current_scope = max(os.path.dirname(os.path.dirname(current_scope)), path)
        if nfound > 9000:
            # narrow scope
            lastpath = result["hits"]["hits"][-1]["_source"]["path"]
            current_scope_depth = len(current_scope.split("/"))
            current_scope = "/".join(lastpath.split("/")[:current_scope_depth+1])
        query = all_under_query(current_scope, **kwargs)   
        n += nfound
        if len(result["hits"]["hits"]) > 0:
            search_after = result["hits"]["hits"][-1]["sort"]

        for record in result["hits"]["hits"]:
            yield record["_source"]


def where_is(name, fetch_size=10000, removed=False):
    """retrun records for items named"""
    query = all_under_query("/", name_regex=name, include_removed=removed)
    results = es.search(index=indexname, query=query, size=fetch_size)
    files = []
    for r in results["hits"]["hits"]:
        files.append(r["_source"])
    return files


def ls_query(path, size=10000, **kwargs):
    """ls for fbi
    
    :param str path:
    :param **kwrargs: Any options from all_under_query
    :return list[dict]: FBI records.
    """
    for i, rec in enumerate(fbi_records_under(path, **kwargs)):
        yield rec
        if i > size: break

def count(path, **kwargs):
    query = all_under_query(path, **kwargs)
    return es.count(index=indexname, query=query, request_timeout=900)["count"]

def all_under_query(path, location=None, name_regex=None, 
                    include_removed=False, item_type=None, ext=None,
                    since=None, before=None, 
                    audited_since=None, audited_before=None, 
                    corrupt_since=None, corrupt_before=None, 
                    regex_date_since=None, regex_date_before=None, 
                    with_field=None, without=None, blank=None, 
                    maxsize=None, minsize=None, 
                    fileset=None,
                    after=None, stop=None):
    """
    Make elastic search query for FBI records. 

    :param str path: The path to search under.
    :param str location: Media location, either on_disk or on_tape. Default is all locations.
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
    :param int minsize: Search for items larger than this size in bytes.
    :param str fileset: Search for items in a fileset.
    :param str after: Search items where path is lexically after this.
    :param str stop: Search items where path is lexically before this.

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

    #must_not.append({"term": {"name.keyword": {"value": ".ftpaccess" }}})
    #must_not.append({"term": {"name.keyword": {"value": "00README_catalogue_and_licence.txt" }}})

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

    if regex_date_since is not None:
        must.append({"range": {"regex_date": {"gte": regex_date_since}}})

    if regex_date_before is not None:
        must.append({"range": {"regex_date": {"lte": regex_date_before}}})

    if after and stop:
        must.append({"range": {"path.keyword": {"gt": after, "lte": stop}}})
    elif after:
        must.append({"range": {"path.keyword": {"gt": after}}})
    elif stop:
        must.append({"range": {"path.keyword": {"lte": stop}}})

    if location is not None:
        must.append({"term": {"location": location}})

    if fileset is not None:
        must.append({"term": {"fileset": fileset}})

    return {"bool": {"must": must, "must_not": must_not }}  

def lastest_file(directory):
    """latest file record of last updated file under a path.
    
    :param str directory: path to search for last updated file
    :return dict or None: Record for the last updated file.
    """
    return top_file(directory, "last_modified")

# miss spelt
latest_file = lastest_file

def first_file(directory):
    """First file record in ES order of logical path under a path.
    
    :param str directory: path to search.
    :return dict or None: File Record. 
    """
    return top_file(directory, "path.keyword", order="asc")

def last_file(directory):
    """Last file record in ES order of logical path under a path.
    
    :param str directory: path to search.
    :return dict or None: File Record. 
    """
    return top_file(directory, "path.keyword")

def top_file(directory, order_by, order="desc"):
    """First file record in ES order of logical path under a path.
    
    :param str directory: path to search.
    :param str order_by: attribute to order on.
    :param str directory: 
    :return dict or None: Record for the first file.
    """
    query = all_under_query(directory, item_type="file")
    sort = [{order_by: {"order": order}}]
    result = es.search(index=indexname, query=query, sort=sort, size=1, request_timeout=900)
    if len(result["hits"]["hits"]) == 0:
        return None
    last_record = result["hits"]["hits"][0]["_source"]

    order_by = order_by.replace(".keyword", "")
    if order_by in last_record:
        return last_record
    else:
        return None


def links_to(target):
    """return list of links to an archive target."""
    sort = [{ "path.keyword": "asc" }]
    query = {"bool": {"must": [
                   {"term": {"target": {"value": target}}},
                   {"term": {"type": {"value": "link"}}}],
                       "must_not": [{"exists": {"field": "removed" }}]}}
    result = es.search(index=indexname, query=query, size=10000, sort=sort, request_timeout=900)

    links = []
    for r in result["hits"]["hits"]:
        links.append(r["_source"]["path"])
    return links

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

def get_random_records(path, number, **kwargs):  
    """Get a set of random records that match critiria.
    
    :param str path: path to pick from.
    :param int number: Number of records to return.
    :param **kwrargs: Any options from all_under_query

    :return list[dict]: FBI Records.
    """

    query = all_under_query(path, **kwargs)
    # print(json.dumps(query, indent=4))
    query = {"function_score": {"query": query, "random_score": {}, "boost_mode": "replace"}}
    results = es.search(index=indexname, query=query, size=number, request_timeout=900)
    recs = []
    for r in results["hits"]["hits"]:
        recs.append(r["_source"])
    recs.sort(key=lambda x: x["path"])
    return recs

def get_random(path, number, **kwargs): 
    """Get a set of random paths that match critiria.
    
    :param str path: path to pick from.
    :param int number: Number of records to return.
    :param **kwrargs: Any options from all_under_query

    :return list[str]: Archive paths.
    """
    recs = get_random_records(path, number, **kwargs)
    return list(map(lambda x: x["path"], recs))

def archive_summary(path, max_types=5, max_vars=1000, max_exts=10,  
                    include_removed=False, **kwargs):
    """find summary info for the archive below a path."""
    query = all_under_query(path, include_removed=include_removed, **kwargs)
    aggs = {"size_stats":{"stats":{"field":"size"}},
            "types": {"terms": {"field": "type", "size": max_types}},
            "exts": {"terms": {"field": "ext", "size": max_exts}},
            "vars": {"terms": {"field": "phenomena.best_name.keyword", "size": max_vars}},
            "dates": {"stats":{"field":"regex_date"}}}

    # print(json.dumps(query, indent=4))
    result = es.search(index=indexname, query=query, size=0, aggs=aggs, request_timeout=900)
    aggs = result["aggregations"]
    ret = {"size_stats": aggs["size_stats"]}
    if "min_as_string" in aggs["dates"]:
        ret["regex_date_range"] = (aggs["dates"]["min_as_string"], aggs["dates"]["max_as_string"])
    else:
        ret["regex_date_range"] = None

    for agg_name in ("types", "exts", "vars"):
        agg_list = []
        for bucket in aggs[agg_name]["buckets"]:
            agg_list.append((bucket["key"], bucket["doc_count"]))
        ret[agg_name] = agg_list
    return ret


def _split(splitlist, batch_size, **kwargs):
    """
    Divide a list of directories into by adding subdirectories if there are too many items in a directory.

    :param list splitlist: A list of tuples containing a directory name and an item count. 
                           e.g. [("/x/y", 100)] may expand to [("/x/y/a", 50), ("/x/y/b", 10), ("/x/y/c", 40),]
    """
    new_splits = []
    for directory, split_count in splitlist:
        if split_count > batch_size:
            subdirs = fbi_listdir(directory, dirs_only=True)
            for subdir in subdirs:
                subdir_path = subdir["path"]
                sub_count = count(subdir_path, **kwargs)
                new_splits.append((subdir_path, sub_count))
                split_count -= sub_count
        new_splits.append((directory, split_count))
    return new_splits

def splits(path, batch_size=10000000, **kwargs):
    splits = [(path, count(path, **kwargs))]
    while True:
        new_splits = _split(splits, batch_size=batch_size, **kwargs)
        if len(splits) == len(new_splits):
            break
        splits = new_splits

    splits.sort()
    merged = []
    batch_count = 0
    after = path
    for d, c in splits:
        if batch_count + c > batch_size:
            merged.append((after, d, batch_count))
            after = d
            batch_count = 0
        batch_count += c
    merged.append((after, path+"~", batch_count))

    return merged

def make_dirs(directory):
    """
    Make FBI records for a diretory and any missing parent directories.

    :param str directory: The directory to add. 
    """
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
    """FBI record list for a directory"""
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
    es.index(index=indexname, id=record_id, document=record, request_timeout=100)    

def update_item(record):
    """Update a single document - overwrite feilds in record suplied."""
    es.update(index=indexname, id=_create_id(record["path"]), doc=record, doc_as_upsert=True, request_timeout=100)

def flag_removed(record):
    """Mark a file as removed by adding a removed date."""
    fbi_rec = get_record(record["path"])
    if fbi_rec is None:
        return
    fbi_rec["removed"] = record["last_modified"]
    document = {'doc': fbi_rec, 'doc_as_upsert': True}
    es.update(index=indexname, id=_create_id(record["path"]), doc=document)

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

    query = {"bool": {"must": must, "must_not": must_not}}
    results = es.search(index=indexname, query=query, request_timeout=90)
    records = []
    for r in results["hits"]["hits"]:
        records.append(r["_source"])
    return records

def _create_id(path):
    return hashlib.sha1(path.encode()).hexdigest()

def bulk_update(records):
    raise DeprecationWarning("Do not use bulk_update")
    raise Exception("Stop for now")
    """Update a list of records"""
    body = ''
    for record in records:
        body += json.dumps({"update": {"_index": indexname, "_id": _create_id(record["path"])}}) + "\n"
        body += json.dumps({"doc": record, "doc_as_upsert": True}) + "\n"
        #add_item(record)
    print(body)
    bulk(es, body)


def update_file_location(path_list, location): 
    """Mark list of paths as on media type. This is for the NLA system to update media
    movements.

    :param list pathlist: A list of paths to mark up.
    :param str location: "on_disk", "on_tape" or "on_obstore"."""
    assert location in ("on_disk", "on_tape", "on_obstore", "on_cache")

    for path in path_list:
        rec = get_record(path)
        if rec is None:
            raise(ValueError(f"No FBI record for path {path} so can't change to {location}"))
        if rec.get("type") != "file":
            continue
        if rec.get("location") != location:
            rec["location"] = location
            update_item(rec)

def nla_dirs(after="/", stop="/~", fetch_size=10000):
    """FBI record iterator for nla directories"""
    n = 0
    current_stop = stop
    while True:
        sort = [{ "path.keyword": "asc" }]
        query = {"bool": {"must": [
                    {"range": {"path.keyword": {"gt": after, "lte": current_stop}}},
                    {"term": {"name.keyword": {"value": "00FILES_ON_TAPE"}}}
                 ]}}

        result = es.search(index=indexname, query=query, sort=sort, size=fetch_size, request_timeout=900)
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
    result = es.search(index=indexname, query=query, aggs=aggs, size=0, request_timeout=900)

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

