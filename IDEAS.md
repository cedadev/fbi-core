


# General pick a set of records and do something pattern

 - should use the same options to pick records
 - divide the records into mutually exclusive sets
 - launcher programme controls running of batches and is the only way to run batches
 - laucnher programme sits in the background looking for which processes to start next
 - if the launcher fails it knows where to start off again
 - if the lanucher fails it knows which batches are still running.
 - if a batch fails the launcher can tell
 - if a batch fails repeatedly quickly then the lancher flags it as a bad batch
 - There can be only one launcher for a batch at any one time.
 - An inspector shows the state of the batches and the launcher
 - the only thing writing the run state file is the launcher
 - baches are started squientally 


 - a batch run is independent of the launcher. Only sharing a batch sstate file. 
 - the only process writing a batch state file is the batch processor
 - 

batch attributes
- start path 
- stop path
- batch size starting estimate
- current path
- current number done
- summary info
- pid
- finished


run attributes
- bad batches (ones that keep crashing)
- options for selection
- report frequency - number of records to do before writing to the batch file.
- function called for each record
 

## files

All info for a run is in a run directory

There is a single state.json file with the run?

There is a launcher lock file to make sure there is only one launcher.

## operation

create run 
```bash
$ make_run run_dir 'fbi_core.dump' --report N --launch [archive selection-options] 

43 Batches made.
...
Launching

```

start launcher
No output
```bash
$ launcher run_dir  
```

start batch - only done by launcher
```bash
$ start_batch run_dir batch_num   
```

Inspect state
```bash
$ inspect run_dir

Run run_dir
Batches: ...

```
