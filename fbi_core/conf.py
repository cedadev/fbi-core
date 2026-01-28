import os

import yaml

conf_file = os.path.join(os.environ["HOME"], ".fbi.yml")

if os.path.exists(conf_file):
    conf = yaml.load(open(conf_file), Loader=yaml.Loader)
    APIKEY = conf["ES"]["api_key"]
    ES_HOSTS = conf["ES"].get("hosts", ["https://elasticsearch.ceda.ac.uk:443"])
else:
    APIKEY = None
    ES_HOSTS = ["https://elasticsearch.ceda.ac.uk:443"]
