#!/usr/bin/env python

import csv
import io
import os
import re
import subprocess
from collections import defaultdict
from datetime import date
from xml.dom import minidom

import requests

from settings import (
    CLUSTER,
    EULER_SAMPLES_FOLDER,
    EULER_UPLOADS_FOLDER,
    KEYFILE,
    PASSWORD,
    POLYBOX_UPLOAD_FOLDER,
    POLYBOX_UPLOADED_IDS,
    POLYBOX_USB_METADATA,
    USER,
)


def ssh(*args, echo=False):
    try:
        response = subprocess.check_output(
            ["ssh", "-i", KEYFILE, f"{USER}@{CLUSTER}", *args],
            stderr=subprocess.STDOUT,
            text=True,
        )
        if echo and response:
            print(response)
        return response
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "command '{}' returned with error (code {}): {}".format(
                e.cmd, e.returncode, e.output
            )
        )


def create_polybox_folder_if_not_exists(folder):
    result = requests.request("MKCOL", folder, auth=(USER, PASSWORD))
    if result.status_code in (201, 405):
        return
    if result.status_code == 401:
        raise IOError("polybox authorization failed.")
    raise IOError(f"creating polybox folder {folder} failed.")


def fetch_existing_uploads():
    result = requests.get(POLYBOX_UPLOADED_IDS, auth=(USER, PASSWORD))
    if result.status_code == 404:
        return
    lines = result.text.split("\n")
    for line in lines:
        yield tuple(line.split(" ", 1))


def update_uploaded(ids):
    to_upload = [
        f"{ethid} {batch}" for (ethid, batch) in sorted(fetch_existing_uploads()) + ids
    ]
    data = io.StringIO("\n".join(to_upload))
    result = requests.put(POLYBOX_UPLOADED_IDS, auth=(USER, PASSWORD), data=data)
    result.raise_for_status()


def fetch_usb_meta_data_files():
    result = requests.request("PROPFIND", POLYBOX_USB_METADATA, auth=(USER, PASSWORD))
    dom = minidom.parseString(result.text.encode("utf-8", "xmlcharrefreplace"))
    for element in dom.getElementsByTagName("d:href"):
        file_name = os.path.basename(element.childNodes[0].wholeText)
        if re.match(r"sars-cov-2_samples_gfb_[0-9]{8}.csv", file_name):
            yield file_name


def eth_ids_from_usb_meta_data_file(file_name):
    iter = csv.reader(
        requests.get(
            os.path.join(POLYBOX_USB_METADATA, file_name),
            auth=(USER, PASSWORD),
        ).text.split("\n"),
        delimiter=",",
    )
    # skip header
    next(iter)
    for line in iter:
        if not line:
            continue
        ethid = line[0]
        yield ethid


def fetch_usb_eth_ids():
    return [
        (ethid)
        for p in fetch_usb_meta_data_files()
        for (ethid) in eth_ids_from_usb_meta_data_file(p)
    ]


def fetch_processed_euler_samples():
    for line in ssh("list_uploads", EULER_UPLOADS_FOLDER).split("\n"):
        folder = line.strip()
        g = re.match(r"(.+?(_.+?){4})-(.+)$", folder)
        g2 = re.match(r"(.+?)-(.+)$", folder)
        if g:
            sample_name = g.group(1)
            ethid = sample_name.split("_", 1)[0]
            batch = g.group(3)
            yield sample_name, ethid, batch
        elif g2:
            sample_name = g2.group(1)
            ethid = sample_name.split("_", 1)[0]
            batch = g2.group(2)
            yield sample_name, ethid, batch

def trigger_upload_single_file_polybox_from_euler(
    sample_name, batch, rel_path, polybox_folder
):
    ssh(
        "upload_polybox",
        EULER_SAMPLES_FOLDER,
        sample_name,
        batch,
        rel_path,
        f"{USER}:{PASSWORD}",
        polybox_folder,
        echo=True,
    )


def upload_polybox(sample_name, batch):
    dated_upload_folder = f"{POLYBOX_UPLOAD_FOLDER}/{date.today():%Y-%m-%d}"
    create_polybox_folder_if_not_exists(dated_upload_folder)

    polybox_folder = os.path.join(dated_upload_folder, ethid)
    create_polybox_folder_if_not_exists(polybox_folder)
    for p in (
        "raw_data/*_R1.fastq.gz",
        "raw_data/*_R2.fastq.gz",
        "references/consensus_ambig.bcftools.fasta",
        "references/consensus.bcftools.fasta",
        "alignments/REF_aln.bam",
        "alignments/REF_aln.bam.bai",
    ):
        trigger_upload_single_file_polybox_from_euler(
            sample_name, batch, p, polybox_folder
        )


create_polybox_folder_if_not_exists(POLYBOX_UPLOAD_FOLDER)

print("fetch existing uploads")
existing_samples = set(fetch_existing_uploads())

print("fetch euler samples")
euler_samples = list(fetch_processed_euler_samples())


euler_samples_not_uploaded_yet = (
    set((ethid, batch) for (_, ethid, batch) in euler_samples) - existing_samples
)

usb_identifiers = fetch_usb_eth_ids()

euler_samples_batches = defaultdict(list)
for sample_name, ethid, batch in euler_samples:
    euler_samples_batches[ethid].append((sample_name, batch))

uploaded = []
not_uploaded = set()

for ethid in usb_identifiers:
    if ethid not in euler_samples_batches:
        print("no euler sample for", ethid)
        not_uploaded.add(ethid)
        continue
    sample_name, batch = euler_samples_batches[ethid][-1]
    if (ethid, batch) in euler_samples_not_uploaded_yet:
        print("upload sample", sample_name, batch)
        upload_polybox(sample_name, batch)
        uploaded.append((ethid, batch))
    else:
        print("skip already uploaded sample", ethid, batch)


print(f"uploaded {len(uploaded)} samples out of {len(not_uploaded) + len(uploaded)}")

if len(uploaded) != 0:
    update_uploaded(uploaded)

