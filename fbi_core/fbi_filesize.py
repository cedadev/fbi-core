from typing import DefaultDict
import click
import json
import tabulate
from .format_utils import sizeof_fmt
from .fbi_tools import es, indexname, get_record, archive_summary, ls_query


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
    print(info["size_stats"], item_types, exts)
    return info["size_stats"], item_types, exts


@click.command()
@click.argument("paths", nargs=-1)
@click.option("--maxtypes", help="Max number of common types to display.", default=3)
@click.option("--location", help="size only for this media type.")
@click.option("--name_regex", help="Only count files that match a regex.")
def main(paths, maxtypes, location, name_regex):
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


if __name__ == "__main__":
    show_record()

