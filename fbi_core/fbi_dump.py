import threading
from queue import Queue
import click
from .fbi_tools import es, indexname, fbi_records_under, splits
import json
import os
import zipfile
import tabulate
import time
from typing import NamedTuple
from collections import namedtuple
from .fbi_filesize import FilterCommand, CONTEXT_SETTINGS
import signal
import importlib
import subprocess

FBIBatchState = namedtuple('FBIBatchState', ['number', 'after', 'stop', "start_count", 'current', 'summary', 'number_processed', "processing_pid"])


def process_exists(pid):
    """Helper function to detect if a process is running"""
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


def load_function(func_str):
    """Helper functions to return a function from a string"""
    module_components = func_str.split(".")
    if len(module_components) > 1:
        module_name = ".".join(module_components[:-1])
        function_name = module_components[-1]
        module = importlib.import_module(module_name)
        return getattr(module, function_name)
    else:
        function_name = module_components[-1]
        return getattr(globals()["__builtins__"], function_name)


class FBIBatch:
    def __init__(self, run, batch_number) -> None:
        self.run = run
        self.after = None
        self.stop = None
        self.start_count = None
        self.current = None
        self.summary = None
        self.number_processed = 0
        self.processing_pid = None
        self.batch_number = batch_number
        self.is_setup = False
        if os.path.exists(self.batch_savefile):
            self.load()

    def setup(self, after, stop, start_count):
        self.after = after
        self.stop = stop
        self.start_count = start_count
        self.current = after
        self.is_setup = True
        self.save()

    def load(self):
        state = json.load(open(self.batch_savefile))
        self.after = state["after"]
        self.stop = state["stop"]
        self.start_count = state["start_count"]
        self.current = state["current"]
        self.summary = state["summary"]
        self.number_processed = state["number_processed"]
        self.processing_pid = state["processing_pid"]
        self.is_setup = True     

    def save(self):
        assert self.is_setup, "Can't save batch until setup."
        tmp_file = self.batch_savefile + ".tmp"
        with open(tmp_file, "w") as f:
            state = {
                "after": self.after,
                "stop": self.stop,
                "start_count": self.start_count,
                "current": self.current,
                "summary": self.summary,
                "number_processed": self.number_processed,
                "processing_pid": self.processing_pid
            }
            json.dump(state, f, indent=4)
        os.rename(tmp_file, self.batch_savefile)

    def is_complete(self):
        return self.current and self.stop and self.current >= self.stop
    
    def is_running(self):
        return self.processing_pid and process_exists(self.processing_pid)

    def kill(self):
        if self.processing_pid and process_exists(self.processing_pid):
            os.kill(self.processing_pid, signal.SIGINT)
        self.processing_pid = None
        self.save()

    @property
    def batch_savefile(self):
        return self.run.batch_savefile(self.batch_number)

    def set_summary(self, summary):
        s = self.state
        self.state = FBIBatchState(s.number, s.after, s.stop, s.start_count, s.current, summary, s.number_processed, s.processing_pid)

    def records(self):
        # Fetch records from Elasticsearch and add them to the queue 
        self.processing_pid = os.getpid()
        self.save()
        query_args = self.run.base_query.copy()
        query_args["after"] = self.current
        query_args["stop"] = self.stop
        for record in fbi_records_under("/", **query_args):
            self.current = record["path"]
            self.number_processed += 1
            if self.number_processed % self.run.batch_state_save_frequency == 0: 
                self.save()
            yield record
        self.current = self.stop
        self.processing_pid = None
        self.save()

def dump(batch):
    # Fetch records from Elasticsearch and add them to the queue 
    label = batch.after.strip("/").replace("/", "_")
    label = batch.batch_number
    output_dir = "./output"
    os.makedirs(output_dir, exist_ok=True)
    output_file = f"{output_dir}/{label}.jsonl"
    with open(output_file, 'a') as fh:
        for record in batch.records():
            fh.write(json.dumps(record) + "\n")



class FBIBatchRun:

    def __init__(self, dir_name, function_name=None, parallel_processes=4, batch_size=1000000, batch_state_save_frequency=1000, **kwargs):
        self.dir_name = dir_name
        self.parallel_processes=parallel_processes
        self.batch_state_save_frequency = batch_state_save_frequency
        self.base_query = kwargs
        self.batch_size = batch_size
        self.function_name = function_name
        self.number_of_batchs = 0
        self.batches = []
        if os.path.exists(self.statefile):
            self.active = False
            self.active_run_pid = None
            self.load()
        else: 
            self.active = True
            self.active_run_pid = os.getpid()
            self.save()
        self.func = load_function(self.function_name)

    def load(self):
        with open(self.statefile) as f:
            loaded = json.load(f)
        self.batches = []
        self.batch_size = loaded["batch_size"]
        self.parallel_processes = loaded["parallel_processes"]
        self.base_query = loaded["base_query"]
        self.batch_state_save_frequency = loaded["batch_state_save_frequency"]
        self.function_name = loaded["function_name"]
        self.active_run_pid = loaded["active_run_pid"]
        self.number_of_batchs = loaded["number_of_batchs"]

        for batch_number in range(self.number_of_batchs):
            self.batches.append(FBIBatch(self, batch_number))

        if process_exists(self.active_run_pid):
            self.active = False
        else: 
            self.active = True
            self.active_run_pid = os.getpid()
            self.save()

    def save(self):
        """Save the state of the run."""
        if not self.active:
            return
        os.makedirs(self.dir_name, exist_ok=True)
        with open(self.statefile, "w") as f:
            json.dump({"batch_size": self.batch_size, 
                       "base_query": self.base_query, 
                       "parallel_processes": self.parallel_processes,
                       "batch_state_save_frequency": self.batch_state_save_frequency,
                       "active_run_pid": self.active_run_pid,
                       "function_name": self.function_name,
                       "number_of_batchs": self.number_of_batchs}, f, indent=4)

    @property
    def n_batches(self):
        """The total number of batches in the run"""
        return len(self.batches)

    @property
    def statefile(self):
        """The file name for the run state info"""
        return os.path.join(self.dir_name, "state.json")

    def batch_savefile(self, n):
        return os.path.join(self.run.name, f"batch{n}.json")

    def kill_active(self):
        """kill the active run process if not this instance."""
        if not self.active and process_exists(self.active_run_pid):
            os.kill(self.active_run_pid, signal.SIGINT)
            self.save()
        for b in self.batches:
            b.kill()

    def make_new_batches(self, path, batch_size=1000000, **kwargs):
        if len(self.batches) > 0:
            raise ValueError("batches already exist")
        batches = splits(path, batch_size=batch_size, **kwargs)
        self.base_query = kwargs
        for i, b in enumerate(batches):
            after, stop, count = b
            print(i, b, after, stop, count)
            batch = FBIBatch(self, i)
            batch.setup(after, stop, count)
            batch.save() 
        self.number_of_batchs = len(batches)        
        self.save() 

    def is_complete(self):
        for b in self.batches:
            if not b.is_complete():
                return False
        return True

    def show(self):
        print(f"RUN: {self.dir_name}")
        header = ["Batch", "After", "State", "% Done", "Done", "FBI Batch Size"]
        table = []
        total = 0
        grand_total = 0
        grand_batch = 0
        for b in self.batches:
            b.load()
            start_count = max(b.start_count, 1) 
            grand_batch += b.start_count
            grand_total += b.number_processed
            total = b.number_processed
            percent = 100 * total / start_count
            done = ""
            if b.is_running(): done = "Running"
            if b.is_complete(): done += " Complete"
            table.append([b.batch_number, b.after, done, f'{percent:5.1f}', f"{total}", f"{start_count}"])
        grand_percent = 100 * grand_total / grand_batch
        table.append(["", "***** Total / -> /~", "", f'{grand_percent:5.1f}', f"{grand_total}", f"{grand_batch}"])
        print(tabulate.tabulate(table, header))

    def batch_savefile(self, n):
        return os.path.join(self.dir_name, f"batch{n}.json")

    def process(self):
        print("Starting process for run")
        while True:
            number_to_start = self.parallel_processes
            for batch in self.batches:
                batch.load()
                if not batch.is_running() and not batch.is_complete() and number_to_start > 0:
                    number_to_start -= 1
                    subprocess.Popen(["fbi_batch_run", self.dir_name, str(batch.batch_number)])
                    
                    print(f"+++ Starting batch {batch.batch_number}")
                time.sleep(0.02)
                
            if self.is_complete():
                print("Break as complete")
                break
            time.sleep(5)        


@click.command(cls=FilterCommand, context_settings=CONTEXT_SETTINGS)
@click.argument('run_name')
@click.argument('function_name')
@click.option('--parallel-processes', default=4, help='Number of concurent batches to run')
@click.option('--records-per-batch', default=100000, help='Number of records to process per batch')
@click.option('--batch-state-save-frequency', default=1000, help='Number of records to process before saving the batch state.')
@click.option('--path', help='Root path to dump', default="/")
@click.option("--setup-only", help="Do not start the launcher", is_flag=True)
def setup_run(run_name, function_name, parallel_processes, records_per_batch, batch_state_save_frequency, path, setup_only, **kwargs):
    print()
    run = FBIBatchRun(run_name, function_name, parallel_processes=parallel_processes, batch_state_save_frequency=batch_state_save_frequency)
    run.make_new_batches(path, batch_size=records_per_batch, **kwargs) 
    if not setup_only:
        os.system(f"fbi_launch_run {run.dir_name} &")


@click.command()
@click.argument('run_name')
@click.option("--kill", help="Kill all batch processes and the launcher", is_flag=True)
@click.option("-i", "--inspect", help="Just look at run info.", is_flag=True)
def launch_run(run_name, kill, inspect):
    run = FBIBatchRun(run_name)
    if not run.active:
        inspect = True
    if kill:
        run.kill_active()
        return
    elif inspect:
        print(f"Inspect run: {run}")
        run.show()
    else:
        print("Start processing")
        run.process()
    
@click.command()
@click.argument('run_name')
@click.argument('batch_number', type=int)
def batch_run(run_name, batch_number):
    run = FBIBatchRun(run_name)
    batch =  FBIBatch(run, batch_number)
    if not batch.is_running(): 
        run.func(batch)
    else:
        print("Tried to start batch that was already running.")
