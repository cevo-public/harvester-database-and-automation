# This is a wrapper script to run import_sequences.py on Euler
echo "Loading python 3.7.1 and R modules"
module load new gcc/6.3.0 python/3.7.1 r/3.6.0

# echo "Have you already run this script once and installed the necessary packages? (y/n)"
# read ALREADY_INSTALLED_PACKAGES
LD_LIBRARY_PATH=/cluster/apps/gcc-6.3.0/udunits2-2.2.24-xfcugdo3ilck7cvwckfxvd2xlsw6wbk7/lib/:$LD_LIBRARY_PATH
R_LIBS_USER=${HOME}/R/x86_64-slackware-linux-gnu-library/3.6/

Rscript database/R/install_packages.R check || {
        echo "Installing requirements from install_packages.R"
        mkdir -p $R_LIBS_USER
        bsub -I Rscript database/R/install_packages.R
}

pip install --user -r requirements_euler.txt

echo "Submitting interactive batch job"
DATE=`date +"%Y-%m-%d_%H:%M:%S"`
export DB_USER=harvester
export DB_HOST=localhost  # we establish port forwarding below
export DB_NAME=sars_cov_2

bsub -J upload-spsp -G es_beere -Is <<"EOF"

# setup port forwarding to database
SERVER=[USER]@[SERVER].ethz.ch
CONTROL_FILE=$(mktemp -u)
ssh -f -N -M -S $CONTROL_FILE -L 9999:localhost:5432 $SERVER
# ensure shotdown of port forwarding
trap 'echo -n shutdown ssh tunel; ssh -S $CONTROL_FILE -O exit $SERVER;' exit

python -u database/python/import_sequences.py --euler 2>&1 | tee logs/${DATE}.log
EOF
