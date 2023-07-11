import threading
from queue import Queue
from elasticsearch import Elasticsearch
import click
from .fbi_tools import es, indexname, get_record, archive_summary, ls_query, parameters, lastest_file, convert2datetime, get_random, splits


# Output directory
output_dir = '/path/to/output/directory'



# Worker function to extract records and dump them to files
def worker():
    while True:
        batch = queue.get()
        if batch is None:
            break

        # Process the batch of records and dump them to a file
        output_file = f"{output_dir}/{threading.current_thread().name}.txt"
        with open(output_file, 'w') as file:
            for record in batch:
                file.write(f"{record}\n")

        queue.task_done()

@click.command()
@click.option('--num-threads', default=4, help='Number of threads to use')
@click.option('--batch-size', default=100, help='Number of records to fetch per batch')
def main(num_threads, batch_size):


    split_batches = splits(batch_size=1000000)
    print(split_batches)
    xxx
    # Create a queue to hold the batches of records
    queue = Queue()

    # Create and start worker threads
    threads = []
    for _ in range(num_threads):
        thread = threading.Thread(target=worker)
        thread.start()
        threads.append(thread)

    # Fetch records from Elasticsearch and add them to the queue
    scroll_id = None
    while True:
        response = es.search(
            index=index_name,
            doc_type=doc_type,
            scroll='2m',
            size=batch_size,
            body={
                'query': {'match_all': {}}
            },
            sort=['_doc'],
            scroll_id=scroll_id
        )

        records = [hit['_source'] for hit in response['hits']['hits']]
        queue.put(records)

        scroll_id = response['_scroll_id']
        if len(records) < batch_size:
            break

    # Wait for all tasks to be processed
    queue.join()

    # Stop worker threads
    for _ in range(num_threads):
        queue.put(None)

    for thread in threads:
        thread.join()

    print("Records dumped successfully.")

if __name__ == '__main__':
    main()