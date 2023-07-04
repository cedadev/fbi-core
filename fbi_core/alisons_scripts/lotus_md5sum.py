# To calculate md5sum's from a list of files, using lotus
#
# A. Waterfall   11/12/2015
#############################################

################
def run_lotus_chksum(outdir, nfiles):
    import os, subprocess

    # string= 'bsub -q short-serial -W 10:00 -J "md5[1-'+str(nfiles)+']" -i '+outdir+'/inputfiles_%I -o '+outdir+'/lotus_output_%I.o -e '+outdir+'/lotus_output_%I.e ./get_md5sum.sh '+outdir
    command = 'sbatch -p short-serial --mem 5 -t 01:00:00 -W --job-name "md5" '
    command += (
        f"--array 1-{nfiles} -i {outdir}/inputfiles_%a -o {outdir}/lotus_output_%a.o "
    )
    command += "-e {outdir}/lotus_output_%a.e ./get_md5sum.sh {outdir}"
    #  print(string)
    subprocess.check_call(command, shell=True)
    # nb. would it be better to have the job id in the returned string?
    return


##############
# main code
# inputs: indir = directory containing input files
# outdir: directory for outputs
# n_nodes:  number of nodes to run on.
def lotus_md5sum(indir, outdir, n_nodes):
    import os, math, glob, time

    # inputs
    # indir='/neodc/esacci_sst/data/lt/Analysis/L4/v01.1/'
    # outdir='/home/users/amwaterfall/sst_test'
    # n_nodes=100  #how many lotus nodes to make use of

    # --------
    # get a list of files
    print("Getting filelist")
    #   filetype = '*LST*'
    #   find_string = 'find -L '+indir + ' -type f' +' -name ' + filetype + '*'
    find_string = "find -L " + indir + " -type f"
    filelist_obj = os.popen(find_string)
    filelist = filelist_obj.readlines()
    # filelist = [x.rstrip() for x in filelist] # remove any end of line characters

    # remove any files starting with . (e.g. .ftpaccess files)
    filelist[:] = [x for x in filelist if (x.split("/")[-1])[0] != "."]

    # write out the input filelist
    # check if the output directory exists
    if os.path.isdir(outdir) == False:
        os.makedirs(outdir)
    infil = open(outdir + "/all_infiles.txt", "w")
    infil.writelines(filelist)
    infil.close()

    # --------
    # write separate input files for lotus
    print("writing files")

    nfiles = len(filelist)  # number of files
    nlines = int(math.ceil(nfiles / float(n_nodes)))

    nstart = 0
    nend = nlines
    loop_id = 0

    # loop over number of nodes and write out a file for each node.
    while nstart < nfiles:
        # write out the file
        loop_id = loop_id + 1
        filename = outdir + "/inputfiles_" + str(loop_id)
        f = open(filename, "w")
        f.writelines(filelist[nstart:nend])
        f.close()
        print(nstart, nend)
        nstart = nstart + nlines
        nend = nend + nlines
        if nend > nfiles:
            nend = nfiles

    # --------
    # run lotus
    run_lotus_chksum(outdir, loop_id)

    # --------
    # check for all lotus scripts having run

    # NB. this won't work if there are files in the directory - would be better to use the jobid somewhere....

    #   finish=0
    #   print(loop_id)
    #   while finish !=1:
    # check there are the right number of output files
    #      o_files = glob.glob(outdir+'/lotus_output*.o')
    #      e_files = glob.glob(outdir+'/lotus_output*.e')
    #      output_files=glob.glob(outdir+'/output*')#
    #
    #      if len(o_files) == loop_id:
    #         finish=1
    #    #wait for a bit.....
    #      time.sleep(60)
    #      print('waiting')

    #   if len(e_files) != loop_id: print('No of error files is wrong')
    #   if len(output_files) != loop_id: print('No of output files is wrong:'+str(len(output_files)))

    # check for no error messages - under SLURM always returns an error file, so omitting
    #   for e in e_files:
    #      f=open(e,'r')
    #      x = f.read()
    #      f.close()
    #      if len(x) != 0:
    #         print('Error message in : '+e)
    #         print(e)

    # check for successful completion message - not given in SLURM?
    #   bad_file=[]
    #   for o in o_files:
    #      f =open(o,'r')
    #      x = f.read()
    #      f.close()
    #      if 'Successfully completed' not in x:
    #         bad_file.append(o)
    #
    #   if bad_file:
    #      print('Not all files successfully completed:')
    #      print(bad_file)
    #   else:
    #      print('All files successfully completed')

    # aggregate output results into one file
    # want to retain correct order
    of = open(outdir + "/all_output.txt", "w")  # file to output all results in
    out_count = 0
    for i in range(1, loop_id + 1):
        fl = outdir + "/output_" + str(i)
        for line in open(fl, "r"):
            of.writelines(line)
            out_count += 1

    of.close()

    # check length of files is as expected from the input file list - otherwise there's been a problem
    if out_count != nfiles:
        print("ERROR:  Some files have not been processed")

    return
