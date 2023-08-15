import threading
from queue import Queue
from elasticsearch import Elasticsearch
import click
from .fbi_tools import es, indexname, fbi_records_under, splits, fbi_records
import json
import os
import zipfile


class SavePoint:

    def __init__(self, dir_name, split_batches):
        self.name = dir_name
        self.split_batches = split_batches
        if os.path.exists(dir_name):
            # load
            


# Worker function to extract records and dump them to files
def worker(queue, ithread, es_retrive_size, output_dir):
    batch_num = 0
    print(f"starting tread {ithread}")
    while True:
        print("GET")
        batch = queue.get()
        if batch is None:
            print(f"stop thread {ithread}")
            break
        after, stop, size = batch
        print(f"thread {ithread} pick up batch {batch}")

        # Fetch records from Elasticsearch and add them to the queue 
        label = after.strip("/").replace("/", "_")
        output_file = f"{output_dir}/{label}.json"
        with open(output_file, 'w') as fh:
            for i, record in enumerate(fbi_records_under("/", search_after=after, search_stop=stop, fetch_size=es_retrive_size)):
                if i % 1000 == 0: print(ithread, batch_num, i, record["path"])
                fh.write(json.dumps(record) + "\n")

        #queue.task_done()
        batch_num += 1


@click.command()
@click.option('--num-threads', default=4, help='Number of threads to use')
@click.option('--es-batch-size', default=10000, help='Number of records to fetch per batch')
@click.option('--records-per-file', default=100000, help='Number of records to fetch per batch')
@click.option('--path', help='Root path to dump', default="/")
@click.option('--output-dir', help='path for output', default="./dump_output")

def main(num_threads, es_batch_size, records_per_file, path, output_dir):

    split_batches = splits(batch_size=records_per_file, root_path=path)
    print(split_batches)

    # Create a queue to hold the batches of records
    queue = Queue()

    # Create and start worker threads
    threads = []
    for ithread in range(num_threads):
        thread = threading.Thread(target=worker, args=(queue, ithread, es_batch_size, output_dir))
        thread.start()
        threads.append(thread)

    # make output dir
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    for batch in split_batches:
        queue.put(batch)

    # Stop worker threads
    for _ in range(num_threads):
        queue.put(None)

    for thread in threads:
        thread.join()

    print("Records dumped successfully.")

if __name__ == '__main__':
    main()