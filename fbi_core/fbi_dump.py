import threading
from queue import Queue
from elasticsearch import Elasticsearch
import click
from .fbi_tools import es, indexname, fbi_records_under, splits


# Output directory
output_dir = '/path/to/output/directory'



# Worker function to extract records and dump them to files
def worker(queue, ithread, es_retrive_size):
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
        for i, record in enumerate(fbi_records_under("/", search_after=after, search_stop=stop, fetch_size=es_retrive_size)):
            print(ithread, batch_num, i, record["path"])

        ''' # Process the batch of records and dump them to a file
        output_file = f"{output_dir}/{threading.current_thread().name}.txt"
        with open(output_file, 'w') as file:
            for record in batch:
                file.write(f"{record}\n")'''

        #queue.task_done()
        batch_num += 1

@click.command()
@click.option('--num-threads', default=4, help='Number of threads to use')
@click.option('--es-batch-size', default=10000, help='Number of records to fetch per batch')
@click.option('--records-per-file', default=10000, help='Number of records to fetch per batch')
@click.option('--path', help='Root path to dump', default="/")
def main(num_threads, es_batch_size, records_per_file, path):

    split_batches = splits(batch_size=records_per_file, root_path=path)
    print(split_batches)

    # Create a queue to hold the batches of records
    queue = Queue()

    # Create and start worker threads
    threads = []
    for ithread in range(num_threads):
        thread = threading.Thread(target=worker, args=(queue, ithread, es_batch_size))
        thread.start()
        threads.append(thread)

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