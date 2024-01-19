from typing import DefaultDict
import click
import json
import sys
import os
import tabulate
import colorama
from .format_utils import sizeof_fmt
from .fbi_tools import get_record, archive_summary, ls_query, parameters, lastest_file, convert2datetime, get_random,  links_to, fbi_records, fbi_records_under, get_records_by_content, splits
import time

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

class FilterCommand(click.Command):
    """class to add standard options to command line tools"""
    # standard options are: item_type, location, ext, since, before, name_regex, without, maxsize, minsize
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        options = [click.core.Option(("-t", "--item-type"), 
                                     type=click.Choice(['file', 'dir', 'link'], case_sensitive=False), 
                                     help="only pick items of this type"),
            click.core.Option(("--location",), type=click.Choice(['on_disk', 'on_tape'], case_sensitive=False), 
                                     help="only pick files stored on disk or tape"),
            click.core.Option(("-e", "--ext"), metavar="EXT", help="only pick files with extention"),
            click.core.Option(("--since",), type=click.DateTime(), help="only pick files modified since a date"),
            click.core.Option(("--before",), type=click.DateTime(), help="only pick files modified before a date"),
            click.core.Option(("--audited-since",), type=click.DateTime(), help="only pick files audited since a date"),
            click.core.Option(("--audited-before",), type=click.DateTime(), help="only pick files audited before a date"),
            click.core.Option(("--corrupt-since",), type=click.DateTime(), help="only pick files corrupted since a date"),
            click.core.Option(("--corrupt-before",), type=click.DateTime(), help="only pick files corrupted before a date"),
            click.core.Option(("--name_regex",), metavar="REGEX", help="Only pick files that match a regex."),
            click.core.Option(("--without",), metavar="FIELD", help="Only pick files without this field in the record."),
            click.core.Option(("--blank",), metavar="FIELD", help="Only pick files where this field is an empty string."),
            click.core.Option(("--with-field",), metavar="FIELD", help="Only pick files with this field in the record."),
            click.core.Option(("--maxsize",), type=int, metavar='SIZE', help="Only pick files with size less then SIZE."),
            click.core.Option(("--minsize",), type=int, metavar='SIZE', help="Only pick files with size greater then SIZE."),
            click.core.Option(("--fileset",), metavar='FILESET', help="Only pick items in this fileset."),
            click.core.Option(("--include-removed",), is_flag=True, show_default=True, help="inculde removed items."),
            click.core.Option(("--after",), metavar="PATH", help="Records where path is lexically after this."),
            click.core.Option(("--stop",), metavar="PATH", help="Records where path is lexically before this."),]
        for o in reversed(options):
            self.params.insert(0, o)


@click.command(cls=FilterCommand)
@click.argument("paths", nargs=-1)
def ls2(paths, **kwargs):
    t0 =  time.time()
    t00 = time.time()
    for path in paths:
        for i, f in enumerate(fbi_records_under(path,  **kwargs)):
        #for f in fbi_records_under(path, **kwargs):
            if i % 10000 == 0:
                rate = 10000/(time.time() - t0)
                trate = i/(time.time() - t00)
                t0 = time.time()
                print(f'{i}: [{int(trate)} {int(rate)}]{f["path"]}')
            print(f["path"])

@click.command(cls=FilterCommand)
@click.argument("paths", nargs=-1)
def ls(paths, **kwargs):
    for path in paths:
        for f in fbi_records_under(path,  **kwargs):
            print(f["path"])

@click.command(cls=FilterCommand)
@click.argument("paths", nargs=-1)
def md5sum(paths, **kwargs):
    """Make a list of MD5 checksums from FBI"""
    for path in paths:
        files = fbi_records_under(path, **kwargs)
        for f in files:
            if "md5" in f:
                print(f'{f["md5"]} {f["path"]}')


@click.command()
@click.option("-l", '--filelist', help='file listing. Defaults to sdtin', type=click.File('r'), default=sys.stdin)
@click.option("--limit-path", help="Limit the search to files under this path")
@click.option("-r", "--include-removed", help="Include files that have been removed from the archive.", is_flag=True)
@click.option("-m", "--match-filename", help="Only match if the filename in the archive is the same.", is_flag=True)
@click.option("-v", "--verbose", help="Print each line", is_flag=True)
def check_archive_by_checksum(filelist, limit_path, include_removed, match_filename, verbose):
    """Check files in archive against md5sum output."""
    matched = 0
    unmatched = 0
    duplicates = []
    for line in filelist:
        md5, path = line.strip().split()
        if match_filename:
            filename = os.path.basename(path)
        else:
            filename = None
        records = get_records_by_content(md5, include_removed=include_removed, under=limit_path, filename=filename)
        n_records = len(records)
        if n_records == 1:
            matched += 1
            if verbose: print(f"SINGLE MATCH {md5}  {path} == {records[0]['path']}")
        elif n_records == 0:
            unmatched += 1
            if verbose: print(f"NO MATCH     {md5} {path}")
        else:
            duplicates.append(path)
            if verbose: 
                print(f"DUPLICATES   {n_records} * {md5} {path}") 
                for record in records: print(record["path"])
     

    print(f"Found single matching file:     {matched}")
    print(f"Not found:                      {unmatched}")
    print(f"Duplicate files:                {len(duplicates)}")

def agg_info(path, maxtypes=3, **kwargs):
    info = archive_summary(path, max_types=maxtypes, max_exts=maxtypes, **kwargs)

    exts = {}
    for key, value in info["exts"]:
        if key == "File without extension.": key = 'No_ext'
        if key.startswith("."): key = key[1:]
        if key == "": key = 'No_ext'
        exts[key] = value

    item_types = {"file":0, "link":0, "dir":0}
    item_types.update(dict(info["types"]))
    return info["size_stats"], item_types, exts


@click.command(cls=FilterCommand, context_settings=CONTEXT_SETTINGS)
@click.argument("paths", nargs=-1)
@click.option("--maxtypes", help="Max number of common types to display.", default=3)
def summary(paths, maxtypes, **kwargs):
    table = []
    headers = ["Path", "Files", "Dirs", "links", "Size", "Min", "Max", "Avg", "exts"]
    total_files = 0
    total_dirs = 0
    total_links = 0
    total_size = 0
    for path in paths:
        size_stats, item_types, exts = agg_info(path, maxtypes=maxtypes, **kwargs)

        ext_str = ", ".join(map(lambda x: f"{x[0]}: {x[1]}" , exts.items()))
        if item_types['file'] + item_types['dir'] + item_types['link'] > 0:
            table.append([path, item_types['file'], item_types['dir'],item_types['link'],
                          sizeof_fmt(size_stats["sum"]),
                          sizeof_fmt(size_stats["min"]), sizeof_fmt(size_stats["max"]), 
                          sizeof_fmt(size_stats["avg"]), ext_str])
        total_files += item_types['file']
        total_dirs += item_types['dir']
        total_links += item_types['link']
        total_size += size_stats["sum"]
    table.append(["TOTALS", total_files, total_dirs, total_links, sizeof_fmt(total_size),
                          "", "", "", ""])
    table_str = tabulate.tabulate(table, headers)
    unit_highligths = (("PiB", colorama.Fore.RED), ("TiB", colorama.Fore.YELLOW))
    for unit, colour in unit_highligths:
        table_str = table_str.replace(unit, f"{colour}{unit}{colorama.Style.RESET_ALL}")
    print(table_str)


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


@click.command(cls=FilterCommand, context_settings=CONTEXT_SETTINGS)
@click.argument("path")
@click.option("-n", "--number", metavar="N", help="Pick N paths. Max 10000", type=int, default=20)
def random_paths(path, number, **kwargs):
    """Pick a set of random files from the CEDA Archive using the FBI."""

    paths = get_random(path, number, **kwargs)
    for path in paths:
        print(f"{path}")


@click.command(cls=FilterCommand, context_settings=CONTEXT_SETTINGS)
@click.argument("path")
@click.option("-n", "--number", metavar="N", help="Pick N paths. Max 10000", type=int, default=10000000)
def find_splits(path, number, **kwargs):
    print("ff", number, f"{kwargs}")
    sss = splits(path, batch_size=number, **kwargs)
    for start, stop, count in sss:
        print(f"{start}, {stop}, {count}")

@click.command()
@click.argument("paths", nargs=-1)
def find_links_to(paths):
    for path in paths:
        for link in links_to(path):
            print(link)
        for link in links_to(path + "/"):
            print(link)

if __name__ == "__main__":
    show_record()

