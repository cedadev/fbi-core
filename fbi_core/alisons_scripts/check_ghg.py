#Example to check ghg cci data 

import lotus_md5sum, compare_md5files_quick

#1) check old ghg data in the archive against old files in the group workspace

#calculate the md5 sums in the archive
indir1 = '/neodc/esacci/ghg/data/cci_plus/CO2_OC2_FOCA/v9.0/'
outdir1 = '/home/users/amwaterfall/ghg_check/archive/'

n_nodes=100
lotus_md5sum.lotus_md5sum(indir1,outdir1,n_nodes)


#check the input data on the group workspace
indir2='/gws/nopw/j04/esacci_portal/ghg/temp/'
outdir2='/home/users/amwaterfall/ghg_check/gws/'
nodes=100
lotus_md5sum.lotus_md5sum(indir2,outdir2,n_nodes)

#now compare the output and check if identical
compare_md5files_quick.compare_md5files_quick(outdir2+'all_output.txt',outdir1+'all_output.txt')