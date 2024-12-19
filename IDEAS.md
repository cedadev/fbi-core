


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




# splits revisit

Start at directory A with a target number of items no bigger than T and no smaller than kT.
The `count` function has after and stop args that count number of item between them.
The `next_sib` function gets the next sibling directory for A (e.g /z/a next_sib("/z/a") = "/z/b") if there 
are no direct siblings then use parent 
A has perent P.
A has sibling dirs S1, S2, ... Sn after A

Start at A and use next sibling dir as a stop. CA = count(A, S1)
This counts all items under the directory A + any files bettwen A and S1. 

stop = S1
C = count(A, S1)

while not selected: 
  
  If C > T there is too much in A need to narrow scope.
    stop = first_subdir(A)
   
  else if C < kT there is not enough so move up to P - next stop point is sibling of P 
    stop = next_sib(A)

  else
    This is a good selection  
    selected == C < T and C > kT or reached the end


# An FBI API

function that give a list of records:

fbi_records
fbi_records_under
where_is
ls_query
all_under_query
links_to
get_random_records
fbi_listdir
def get_records_by_content
nla_dirs

stats of records:
count
lastest_file
last_updated
archive_summary
parameters

utils:
convert2datetime
splits
_create_id

Write records:
make_dirs
insert_item
update_item
flag_removed
bulk_update
update_file_location

Get single record:
get_record, get_record_attr

