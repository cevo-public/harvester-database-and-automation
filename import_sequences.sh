# This is a wrapper script to run import_sequences.py on Euler
echo "Loading python 3.7.1 and R modules"
module load new gcc/4.8.2 python/3.7.1
module load new gcc/4.8.2 r/3.6.0 

# echo "Have you already run this script once and installed the necessary packages? (y/n)"
# read ALREADY_INSTALLED_PACKAGES

ALREADY_INSTALLED_PACKAGES=y

if [[ "$ALREADY_INSTALLED_PACKAGES" == "y" ]]; then
        echo "Not installing packages."
        elif [ "$ALREADY_INSTALLED_PACKAGES" = "n" ]
then

        echo "Installing requirements from requirements.txt"
        pip install --user -r requirements_euler.txt

        echo "Installing requirements from install_packages.R"
        R_LIBS_USER=${HOME}/R/x86_64-slackware-linux-gnu-library/3.6/
        mkdir -p $R_LIBS_USER
        bsub -I Rscript database/R/install_packages.R
fi

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

python database/python/import_sequences.py --euler 2>&1 | tee logs/${DATE}.log
EOF
