#!/bin/bash
#BSUB -o %J.o
#BSUB -e %J.e
#BSUB -q lotus

outdir=$1
while read f; do
    md5sum $f >> ${outdir}/output_${SLURM_ARRAY_TASK_ID}
done
echo Successfully completed

