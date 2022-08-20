data_folder=$(realpath ${1:-.})
cmd=${2:-auto}
singularity exec --bind $data_folder:/data spsp-uploader.sif bash -c "cd /data; cp /transfer.sh .; ./transfer.sh $cmd"
