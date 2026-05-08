import os

import yaml

conf_file = os.path.join(os.environ["HOME"], ".fbi.yml")

if os.path.exists(conf_file):
    conf = yaml.load(open(conf_file), Loader=yaml.Loader)
    USERNAME = conf["ES"]["username"]
    PASSWORD = conf["ES"]["password"]
    ES_HOSTS = conf["ES"].get("hosts", ["https://elasticsearch.ceda.ac.uk:443"])
else:
    USERNAME = None
    PASSWORD = None
    ES_HOSTS = ["https://elasticsearch.ceda.ac.uk:443"]
