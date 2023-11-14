# test making fileset "chutes". Make the best file sets to collect data in.

import fbi_core

path = "/badc"


def agg_info(path):
    summary = fbi_core.archive_summary(path, since="2023-10-01")
    item_types = {"file":0, "link":0, "dir":0}
    item_types.update(dict(summary["types"]))
    files = item_types["file"]
    size = summary["size_stats"]["sum"]
    return files, size

def closeable(path):
    depth = path.count("/")
    if depth < 3: 
            return False
    files, size = agg_info(path)
    return files < 4

def busyness(path):
    files, size = agg_info(path)
    depth = path.count("/")
    if depth < 2:
        return 2
    if depth > 6:
        return 0
    
    sizebusyness = size * 1e-9 * 4**(-depth)
    numberbusyness = files * 1e-2 * 2**(-depth)
    return sizebusyness + numberbusyness
    


            
def findactive(path):
    result = []
    b = busyness(path)
    if closeable(path):
        #print(f"close {path}")
        return []
    elif b > 1:
        print(f"BUSY: {b} {path}")
        result.append(path)
        nbusy = 0
        recs = fbi_core.fbi_listdir(path, dirs_only=True)
        for rec in recs:
            subpath = rec["path"]
            result += findactive(subpath)
        return result
    else:
        #print(f"----: {b} {path}") 
        return []

        

chutes = findactive(path)
print(chutes)
last_files = 0
last_path = ""
remove = [] 
for c in chutes:
    files, size = agg_info(c)
    if files < last_files + 10 and c.startswith(last_path):
        remove.append(last_path)
    last_path = c
    last_files = files

chutes = list(set(chutes) - set(remove))   
chutes.sort()
for c in chutes:
    print(c)