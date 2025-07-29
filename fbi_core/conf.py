import yaml
import os


conf_file = os.path.join(os.environ["HOME"], ".fbi.yml")

def load_config():
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

    return api_key, host_es, es_index, es_annotation

