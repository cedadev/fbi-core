import threading
from queue import Queue
from elasticsearch import Elasticsearch
import click
from .fbi_tools import es, indexname, fbi_records_under, splits, fbi_records
import json
import os
import zipfile
import tabulate
import time

class FBIBatch:
    def __init__(self, run_dir, number, after=None, stop=None, start_count=None) -> None:
        self.run_dir = run_dir
        self.number = number
        self.after = after
        self.current = after
        self.stop = stop
        self.start_count = start_count
        self.summary = {}   # an dict to store any summary info for batch.
        if os.path.exists(self.batch_savefile):
            state = json.load(open(self.batch_savefile))
            self.after = state["after"]
            self.current = state["current"]
            self.stop = state["stop"]
            self.start_count = state["start_count"]
        else:
            self.save()

    def save(self):
        tmp_file = self.batch_savefile + ".tmp"
        with open(tmp_file, "w") as f:
            batch_state = {"after": self.after, 
                           "stop": self.stop, 
                           "start_count": self.start_count, 
                           "number": self.number,
                           "run_dir": self.run_dir,
                           "current": self.current,
                           "summary": self.summary}
            json.dump(batch_state, f, indent=4)
        os.rename(tmp_file, self.batch_savefile)

    def load(self):
        state = json.load(open(self.batch_savefile))
        self.after = state["after"]
        self.current = state["current"]
        self.stop = state["stop"]
        self.start_count = state["start_count"]
        self.number = state["number"]
        self.run_dir = state["run_dir"]
        self.summary = state["summary"]

    def is_complete(self):
        return self.current >= self.stop

    @property
    def batch_savefile(self):
        return os.path.join(self.run.name, f"batch{self.number}.json")

    def 


class FBIBatchRun:

    def __init__(self, dir_name, batch_size=1000000):
        self.dir_name = dir_name
        self.batch_size = batch_size
        self.n_batches = 0
        self.queue = Queue()
        # add batches if have not made the state file yet
        if not os.path.exists(self.statefile):
            self.make_new_batches(batch_size)
        else:
            self.load()

    @property
    def statefile(self):
        return os.path.join(self.name, "state.json")
    
    def make_new_batches(self, batch_size):
        os.makedirs(self.dir_name, exist_ok=True)
        batches = splits(batch_size=batch_size)
        self.n_batches = len(batches)
        for i, after, stop, count in enumerate(batches):
            FBIBatch(self.dir_name, i, after=after, stop=stop, start_count=count)      
        self.save() 

    def save(self):
        with open(self.statefile, "w") as f:
            json.dump({"batches": self.batches, "run": self.name}, f)



    def is_complete(self):
        return any(self.batch_is_complete(b["number"]) for b in self.batches)

    def load(self):
        with open(self.statefile) as f:
            loaded = json.load(f)
        self.batches = loaded["batches"]

    def show(self, show_current):
        print(f"RUN: {self.run}")
        header = ["Batch", "After", "State", "% Done", "Done", "FBI Batch Size"]
        table = []
        total = 0
        grand_total = 0
        grand_batch = 0
        for b in self.batches:
            current, total, active = self.batch_info(b["number"])
            fbi_start_count = max(b["fbi_start_count"],1) 
            grand_batch += fbi_start_count
            grand_total += total
            percent = 100*total/fbi_start_count
            done = ""
            if active: done = "Running"
            if current == "~": done += " Complete"
            table.append([b["number"], b['after'], done, f'{percent:5.1f}', f"{total}", f"{fbi_start_count}"])
        grand_percent = 100 * grand_total / grand_batch
        table.append(["", "***** Total / -> /~", "", f'{grand_percent:5.1f}', f"{grand_total}", f"{grand_batch}"])
        print(tabulate.tabulate(table, header))

    def batch_savefile(self, n):
        return os.path.join(self.name, f"batch{n}.json")
            
    def batch_info(self, n):
        savefile = self.batch_savefile(n)
        if not os.path.exists(savefile):
            return "NOT STARTED", 0, False
        active = time.time() - os.path.getmtime(savefile) < 30      
        with open(savefile) as f:
            info = json.load(f)
        return info["current"], info["total"], active

    # Worker function to extract records and dump them to files
    def worker(self, queue, ithread, run_dir):
        print(f"starting tread {ithread}")
        while True:
            print("GET")
            batch_number = queue.get()
            if batch_number is None:
                print(f"stop thread {ithread}")
                break
            
            print(f"thread {ithread} pick up batch {batch_number}")
            batch = FBIBatch(run_dir, batch_number)
            dump(batch)
            #queue.task_done()

def dump(batch: FBIBatch):
    # Fetch records from Elasticsearch and add them to the queue 
    label = batch.after.strip("/").replace("/", "_")
    output_file = f"{output_dir}/{label}.json"
    with open(output_file, 'w') as fh:
        for i, record in enumerate(fbi_records_under("/", search_after=after, search_stop=stop, fetch_size=es_retrive_size)):
            if i % 1000 == 0: print(ithread, batch_num, i, record["path"])
            fh.write(json.dumps(record) + "\n")




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