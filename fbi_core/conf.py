import yaml
import os


conf_file = os.path.join(os.environ["HOME"], ".fbi.yml")
if os.path.exists(conf_file):
    conf = yaml.load(open(conf_file), Loader=yaml.Loader)
    APIKEY = conf["ES"]["api_key"]
    USERNAME = conf["ES"]["username"]
    PASSWORD = conf["ES"]["password"]
else:
    APIKEY = None
    USERNAME = None
    PASSWORD = None

