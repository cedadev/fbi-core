import os
import subprocess
import math

# To calculate md5sum's from a list of files, using lotus
#
# A. Waterfall   11/12/2015
#############################################

################
def run_lotus_chksum(outdir, nfiles):
    command = 'sbatch -p short-serial --mem 5 -t 01:00:00 -W --job-name "md5" '
    command += (
        f"--array 1-{nfiles} -i {outdir}/inputfiles_%a -o {outdir}/lotus_output_%a.o "
    )
    command += "-e {outdir}/lotus_output_%a.e ./get_md5sum.sh {outdir}"
    subprocess.check_call(command, shell=True)
    # nb. would it be better to have the job id in the returned string?
    return


##############
# main code
# inputs: indir = directory containing input files
# outdir: directory for outputs
# n_nodes:  number of nodes to run on.
def lotus_md5sum(indir, outdir, n_nodes):

    # inputs
    # indir='/neodc/esacci_sst/data/lt/Analysis/L4/v01.1/'
    # outdir='/home/users/amwaterfall/sst_test'
    # n_nodes=100  #how many lotus nodes to make use of

    print("Getting filelist")
    find_string = "find -L " + indir + " -type f"
    filelist_obj = os.popen(find_string)
    filelist = filelist_obj.readlines()
    # filelist = [x.rstrip() for x in filelist] # remove any end of line characters

    # remove any files starting with . (e.g. .ftpaccess files)
    filelist[:] = [x for x in filelist if not os.path.basename(x).startswith('.')]

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

    # aggregate output results into one file
    # want to retain correct order
    with open(outdir + "/all_output.txt", "w") as of: # file to output all results in
        out_count = 0
        for i in range(1, loop_id + 1):
            fl = outdir + "/output_" + str(i)
            for line in open(fl, "r"):
                of.writelines(line)
                out_count += 1


    # check length of files is as expected from the input file list - otherwise there's been a problem
    if out_count != nfiles:
        print("ERROR:  Some files have not been processed")

    return
