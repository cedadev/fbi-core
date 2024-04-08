import yaml
import os

operational_indexname = "fbi-2022"
indexname = operational_indexname 
test_indexname = "fbi-test"

conf_file = os.path.join(os.environ["HOME"], ".fbi.yml")
if os.path.exists(conf_file):
    conf = yaml.load(open(conf_file), Loader=yaml.Loader)
    APIKEY = conf["ES"]["api_key"]
    if "use_test_index" in conf["ES"] and conf["ES"]["use_test_index"]:
        indexname = test_indexname
else:
    APIKEY = None

