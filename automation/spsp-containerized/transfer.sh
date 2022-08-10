#!/bin/bash
set -euo pipefail

# ------------------ TRANSFER FILES TO SPSP ------------------

function ts {
    date +%Y/%m/%d-%H:%M:%S
}

echo "[$(ts)] Beginning transfer to SPSP. MAKE SURE YOU ARE ON THE VPN! "

# Configure the .env file for SPSP transfer tool
echo "[$(ts)] Configuring the .env file for SPSP transfer tool."
echo 'ID=$LAB_CODE' > .env
echo 'HOST=spsp.sib.swiss' >> .env
echo 'SFTP_URL=${ID}@${HOST}:/data' >> .env

# Import the SPSP public key for the transfer tool
echo "[$(ts)] Importing the SPSP public key for the transfer tool."
gpg -q --import --fingerprint .pub

# Add the SPSP SFTP server to the list of known hosts for SSH
echo "[$(ts)] Adding the SPSP SFTP server to the list of known hosts for SSH."

mkdir -p $HOME/.ssh
if ! ssh-keyscan spsp.sib.swiss > $HOME/.ssh/known_hosts 2>/dev/null; then
	echo "[$(ts)] ssh-keyscan spsp.sib.swiss failed, are you in BSSE vpn?"
	exit 1
fi

ssh-keyscan -t ecdsa $SPSP_SERVER_IP >> $HOME/.ssh/known_hosts 2>/dev/null # this gets rid of a warning but I don't know why

# Print the SPSP transfer tool version
echo "[$(ts)] Testing transfer tool script spsp with command './spsp version'"
./spsp version

# See what IP address the files will be sent from
echo "[$(ts)] Checking IP address data will be sent from with command 'curl ifconfig.io'"
curl ifconfig.io

# Run the transfer tool
cmd=${1:-auto}
echo "[$(ts)] Running transfer tool script with command $cmd"

export SHELLOPTS
./spsp $cmd
