import os
from requests import get, codes

def load_config():
    try:
        conf_file = os.path.join(os.environ["HOME"], ".fbi.yml") # for prod in Linux based environment
    except:
        conf_file = os.path.join("./home/", ".fbi.yml") # For testing in Windows environment, won't work in Linux/ Mac OS as "HOME" env variable exists. So another solution for debugging would be required.
    if os.path.exists(conf_file):
        conf = yaml.load(open(conf_file), Loader=yaml.Loader)
        api_key = conf["ES"]["api_key"]
        host_es = conf["ES"]["host"]

        if "index_fbi" in conf["ES"]:
            es_index = conf["ES"]["index_fbi"]
        else:
            es_index = None

        if "index_fbi_annotation" in conf["ES"]:
            es_annotation = conf["ES"]["index_fbi_annotation"]
        else:
            es_annotation = None
    else:
        api_key = None
        host_es = None
        es_index = None
        es_annotation = None


    # Get spotlist on storage-d
    spotlist_url = "https://cedaarchiveapp.ceda.ac.uk/storage-d/spotlist"
    spots_page = get(spotlist_url)

    if spots_page.status_code != codes['✓']:

        print(spots_page.status_code + " Warning, error obtaining spotlist on storage-d  from https://cedaarchiveapp.ceda.ac.uk/storage-d/spotlist, using cached values from 05-Dec-2025")
        try:
            spotlist_file = os.path.join(os.environ["HOME"], "spotlist-cache.yml") # for prod in Linux based environment
        except:
            spotlist_file = os.path.join("./home/", "spotlist-cache.yml") # For testing in Windows environment, won't work in Linux/ Mac OS as "HOME" env variable exists. So another solution for debugging would be required.
        if os.path.exists(spotlist_file):
            spotlist = yaml.load(open(spotlist_file), Loader=yaml.Loader)
            spotlist = spotlist["spotlist_cache"]

        else:
            spotlist = None
    else:
        spotlist = spots_page.text.splitlines()

    return api_key, host_es, es_index, es_annotation, spotlist

