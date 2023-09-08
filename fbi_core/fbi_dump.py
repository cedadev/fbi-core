import threading
from queue import Queue
import click
from .fbi_tools import es, indexname, fbi_records_under, splits, fbi_records
import json
import os
import zipfile
import tabulate
import time
from typing import NamedTuple
from collections import namedtuple
from .fbi_filesize import FilterCommand, CONTEXT_SETTINGS
import signal
import glob
import importlib

FBIBatchState = namedtuple('FBIBatchState', ['number', 'after', 'stop', "start_count", 'current', 'summary', 'number_processed', "processing_pid"])


def process_exists(pid):
    """Helper function to detect if a process is running"""
    try:
        os.kill(pid, signal.SIG_IGN)
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
        self.state = None 
        self.batch_number = batch_number
        if os.path.exists(self.batch_savefile):
            self.load()
        else:
            raise ValueError("No batch save file.")

    def load(self):
        state = json.load(open(self.batch_savefile))
        self.state = FBIBatchState(*state)        

    def save(self):
        tmp_file = self.batch_savefile + ".tmp"
        with open(tmp_file, "w") as f:
            json.dump(self.state, f, indent=4)
        os.rename(tmp_file, self.batch_savefile)

    def is_complete(self):
        return self.state.current >= self.state.stop
    
    def is_running(self):
        return self.state.processing_pid and process_exists(self.state.processing_pid)

    @property
    def batch_savefile(self):
        return self.run.batch_savefile(self.batch_number)

    def set_summary(self, summary):
        s = self.state
        self.state = FBIBatchState(s.number, s.after, s.stop, s.start_count, s.current, summary, s.number_processed, s.processing_pid)

    def records(self):
        # Fetch records from Elasticsearch and add them to the queue 
        s = self.state
        self.state = FBIBatchState(s.number, s.after, s.stop, s.start_count, s.current, s.summary, s.number_processed, os.getpid())
        self.save()
        for i, record in enumerate(fbi_records_under("/", search_after=self.state.current, search_stop=self.state.stop)):
                if i % self.run.batch_state_save_frequency == 0: 
                    print(self.batch_number, i, record["path"])
                    s = self.state
                    self.state = FBIBatchState(s.number, s.after, s.stop, s.start_count, record["path"], s.summary, i, s.processing_pid)
                    self.save()
                yield record
        s = self.state
        self.state = FBIBatchState(s.number, s.after, s.stop, s.start_count, record["path"], s.summary, i, s.processing_pid)
        self.save()

def dump(batch):
    # Fetch records from Elasticsearch and add them to the queue 
    label = batch.state.after.strip("/").replace("/", "_")
    output_dir = "./output"
    os.makedirs(output_dir, exist_ok=True)
    output_file = f"{output_dir}/{label}.json"
    with open(output_file, 'w') as fh:
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
        print(self.function_name, "77777")
        self.batches = []
        if os.path.exists(self.statefile):
            self.active = False
            self.active_run_pid = None
            print(self.function_name, "77777")
            self.load()
            print(self.function_name, "77777")
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
        for b in self.batches:
            batch_state = self.batch_state(b.number)
            if process_exists(batch_state.pid):
                os.kill(batch_state.pid, signal.SIGINT)

    def make_new_batches(self, path, batch_size=1000000, **kwargs):
        if len(self.batches) > 0:
            raise ValueError("batches already exist")
        batches = splits(path, batch_size=batch_size, **kwargs)
        self.base_query = kwargs
        for i, b in enumerate(batches):
            after, stop, count = b
            print(i, b, after, stop, count)
            batch_state = FBIBatchState(i, after, stop, count, after, {}, 0, None)
            json.dump(batch_state, open(self.batch_savefile(i), "w"), indent=4)
            self.batches.append(FBIBatch(self, i))  
        self.number_of_batchs = len(batches)        
        self.save() 

    def is_complete(self):
        return any(b.is_complete() for b in self.batches)

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
        return os.path.join(self.dir_name, f"batch{n}.json")

    def process(self):
        print("Starting process for run")
        while True:
            number_to_start = self.parallel_processes
            for batch in self.batches:
                if not batch.is_running() and not batch.is_complete() and number_to_start > 0:
                    number_to_start -= 1
                    os.system(f"fbi_batch_run {self.dir_name} {batch.batch_number} &")
                    print(f"Starting batch {batch.batch_number}")
            if self.is_complete():
                break
            time.sleep(10)
            print("....")         


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
        os.system(f"fbi_batch_run {run.dir_name} &")


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
 

    
