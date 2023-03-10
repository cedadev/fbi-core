from typing import DefaultDict
import click
import json
import tabulate
from .format_utils import sizeof_fmt
from .fbi_tools import es, indexname, get_record, archive_summary, ls_query, parameters, lastest_file, convert2datetime, get_random


@click.command()
@click.argument("paths", nargs=-1)
@click.option("--location", help="size only for this media type.")
@click.option("--name_regex", help="Only count files that match a regex.")
def ls(paths, location, name_regex):
    for path in paths:
        files = ls_query(path, location=location, name_regex=name_regex)
        for f in files:
            print(f["path"])



def agg_info(path, maxtypes=3, location=None, name_regex=None):
    info = archive_summary(path, max_types=maxtypes, max_exts=maxtypes, location=location, name_regex=name_regex)

    exts = {}
    for key, value in info["exts"]:
        if key == "File without extension.": key = 'No_ext'
        if key.startswith("."): key = key[1:]
        if key == "": key = 'No_ext'
        exts[key] = value

    item_types = {"file":0, "link":0, "dir":0}
    item_types.update(dict(info["types"]))
    return info["size_stats"], item_types, exts


@click.command()
@click.argument("paths", nargs=-1)
@click.option("--maxtypes", help="Max number of common types to display.", default=3)
@click.option("--location", help="size only for this media type.")
@click.option("--name_regex", help="Only count files that match a regex.")
def summary(paths, maxtypes, location, name_regex):
    table = []
    headers = ["Path", "Files", "Dirs", "links", "Size", "Min", "Max", "Avg", "exts"]
    for path in paths:
        size_stats, item_types, exts = agg_info(path, maxtypes=maxtypes, location=location, 
                                                name_regex=name_regex)

        ext_str = ""
        for ext, number in exts.items():
            ext_str += f"{ext}:{number} " 

        table.append([path, item_types['file'], item_types['dir'],item_types['link'],
                      sizeof_fmt(size_stats["sum"]),
                      sizeof_fmt(size_stats["min"]), sizeof_fmt(size_stats["max"]), 
                      sizeof_fmt(size_stats["avg"]), exts])

    print(tabulate.tabulate(table, headers))


@click.command()
@click.argument("paths", nargs=-1)
@click.option("--phenomena/--no-phenomena", help="include phenonmena.", default=False)
def show_record(paths, phenomena):
    for path in paths:
        record = get_record(path)
        if not phenomena and record is not None and "phenomena" in record: 
            del record["phenomena"]
        print(json.dumps(record, indent=4)) 


@click.command()
@click.argument("paths", nargs=-1)
def show_parameters(paths):
    for path in paths:
        print(f" ===== {path} =====")
        print(json.dumps(parameters(path), indent=4))
   

@click.command()
@click.argument("paths", nargs=-1)
@click.option("-f", "--filenames", help="Show file names of latest files", is_flag=True)
@click.option("--record", help="Show complete FBI record of latest files", is_flag=True)
def show_last_updated(paths, filenames, record):
    for path in paths:
        rec = lastest_file(path)
        if rec is None:
            print(f"{path}: No file found.")
        else:
            if filenames:
                print(f'{path}: {convert2datetime(rec["last_modified"])}   [{rec["path"]}]')
            else:
                print(f'{path}: {convert2datetime(rec["last_modified"])}')
            if record:
                print(json.dumps(rec, indent=4))


@click.command()
@click.argument("path")
@click.option("-n", "--number", help="Pick N paths. Max 10000", type=int, default=20)
@click.option("-f", "--files", help="only pick files", is_flag=True)
@click.option("-d", "--dirs", help="only pick dirs", is_flag=True)
@click.option("-l", "--links", help="only pick links", is_flag=True)
@click.option("-e", "--ext", help="only pick files with extention")
def random_paths(path, number, files, dirs, links, ext):
    if files or ext is not None:
        item_type = "file"
    elif dirs:
        item_type = "dir"
    elif links:
        item_type = "link"
    else:
        item_type = None 

    paths = get_random(path, number, item_type=item_type, ext=ext)
    for path in paths:
        print(f"{path}")


if __name__ == "__main__":
    show_record()

