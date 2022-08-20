data_folder=$(realpath ${1:-.})
cmd=${2:-auto}
docker run -it --network host -v $data_folder:/data spsp-uploader bash -c "cd /data; cp /transfer.sh .; ./transfer.sh $cmd"
