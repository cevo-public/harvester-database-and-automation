# Transfer files to SPSP.

timestamp=`date +%Y/%m/%d-%H:%M:%S`
echo "[$timestamp] Beginning transfer to SPSP. MAKE SURE YOU ARE ON THE VPN! "

timestamp=`date +%Y/%m/%d-%H:%M:%S`
echo "[$timestamp] Checking for files to submit."
FILES=( /app/outdir/for_submission/viruses/*/*.fasta.gz )
if (( ${#FILES[@]} )); then
    timestamp=`date +%Y/%m/%d-%H:%M:%S`
    echo "[$timestamp] Found sequences to submit."
    else
      timestamp=`date +%Y/%m/%d-%H:%M:%S`
      echo "[$timestamp] No sequences to submit. Exiting."
      exit 0
fi


timestamp=`date +%Y/%m/%d-%H:%M:%S`
echo "[$timestamp] Copying files to transfer-tool directory."
cp -r /app/outdir/for_submission/viruses/ /app/transfer-tool
cd /app/transfer-tool/

# Configure the .env file for SPSP transfer tool
timestamp=`date +%Y/%m/%d-%H:%M:%S`
echo "[$timestamp] Configuring the .env file for SPSP transfer tool."
echo 'ID=$SPSP_LAB_CODE' > .env
echo 'HOST=spsp.sib.swiss' >> .env
echo 'SFTP_URL=${ID}@${HOST}:/data' >> .env

# Import the SPSP public key for the transfer tool
timestamp=`date +%Y/%m/%d-%H:%M:%S`
echo "[$timestamp] Importing the SPSP public key for the transfer tool."
gpg -q --import --fingerprint .pub

# Add the SPSP SFTP server to the list of known hosts for SSH
timestamp=`date +%Y/%m/%d-%H:%M:%S`
echo "[$timestamp] Adding the SPSP SFTP server to the list of known hosts for SSH."
ssh-keyscan spsp.sib.swiss > /root/.ssh/known_hosts
ssh-keyscan -t ecdsa $SPSP_SERVER_IP >> /root/.ssh/known_hosts  # this gets rid of a warning but I don't know why

# Print the SPSP transfer tool version
timestamp=`date +%Y/%m/%d-%H:%M:%S`
echo "[$timestamp] Testing transfer tool script spsp with command './spsp version'"
./spsp version

# Check SSH connection to SPSP SFTP server
#timestamp=`date +%Y/%m/%d-%H:%M:%S`
#echo "[$timestamp] Checking SSH connection with command 'ssh -T $SPSP_LAB_CODE@spsp.sib.swiss'"
#ssh -v -T $SPSP_LAB_CODE@spsp.sib.swiss  # This is useful and should return "This service allows sftp connections only." However, it prevents subsequent commands from running and I don't know why

# See what IP address the files will be sent from
timestamp=`date +%Y/%m/%d-%H:%M:%S`
echo "[$timestamp] Checking IP address data will be sent from with command 'curl ifconfig.io'"
curl ifconfig.io

# Run the transfer tool
timestamp=`date +%Y/%m/%d-%H:%M:%S`
echo "[$timestamp] Running transfer tool script with command './spsp auto'"
./spsp auto
