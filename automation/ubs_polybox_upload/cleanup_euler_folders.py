#!/usr/bin/env python

import os
from datetime import datetime
from xml.dom import minidom

import requests

from settings import EXPIRATION_INTERVAL, PASSWORD, POLYBOX_UPLOAD_FOLDER, USER


def list_outdated_date_folders_polybox(time_delta):
    result = requests.request("PROPFIND", POLYBOX_UPLOAD_FOLDER, auth=(USER, PASSWORD))
    dom = minidom.parseString(result.text.encode("utf-8", "xmlcharrefreplace"))
    for element in dom.getElementsByTagName("d:collection"):
        for (
            node
        ) in element.parentNode.parentNode.parentNode.parentNode.getElementsByTagName(
            "d:href"
        ):
            folder = os.path.basename(node.childNodes[0].wholeText.rstrip("/"))
            try:
                timestamp = datetime.strptime(folder, "%Y-%m-%d")
            except ValueError:
                continue
            if datetime.now() - timestamp > time_delta:
                yield folder


def delete_folder(p):
    result = requests.request(
        "DELETE", p, auth=(USER, PASSWORD), headers=dict(Depth="infinite")
    )
    if result.status_code != 204:
        raise IOError(f"deleting {p} failed with status code {result.status_code}.")


for folder in list_outdated_date_folders_polybox(EXPIRATION_INTERVAL):
    print(f"delete folder for {folder}.")
    delete_folder(POLYBOX_UPLOAD_FOLDER + "/" + folder)
