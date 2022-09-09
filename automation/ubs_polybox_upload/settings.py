#!/usr/bin/env python

import os
from datetime import timedelta

BASEFOLDER = "/cluster/project/pangolin/working"
EULER_UPLOADS_FOLDER = f"{BASEFOLDER}/uploads"
EULER_SAMPLES_FOLDER = f"{BASEFOLDER}/samples"

# polybox and euler user
USER = "[USER]"

PASSWORD = os.environ["PASSWORD"]

CLUSTER = "euler.ethz.ch"
KEYFILE = "./id_ed25519_euler"

POLYBOX_ROOT = f"https://polybox.ethz.ch/remote.php/dav/files/{USER}/Shared"

POLYBOX_UPLOAD_FOLDER = f"{POLYBOX_ROOT}/ETHZ-USB/sequencing_data"
POLYBOX_USB_METADATA = f"{POLYBOX_ROOT}/ETHZ-USB/metadata"
POLYBOX_UPLOADED_IDS = f"{POLYBOX_ROOT}/ETHZ-USB/uploaded_ids.txt"

# cleanup polybox folders:
EXPIRATION_INTERVAL = timedelta(days=14)
