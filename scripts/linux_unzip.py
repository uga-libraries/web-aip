"""Purpose: 7zip sometimes has errors with unzipping the downloaded gzipped WARCs due to a bug in PC zip programs.
When this happens, use this script to unzip the WARC in a Linux environment.

In this case (8-2022 batch), we need to re-download so the script input is web_aip_batch.py after web.download_warcs().
This means the metadata folder has Archive-It metadata reports and the objects folder has zipped WARCs.
To finish, run web_aip_batch.py without web.download_metadata() and web.download_warcs().

The MD5 for each WARC is double-checked before it is unzipped and the seed folder is bagged to detect errors
from moving the files between Windows and Linux environments."""

# usage: python linux_unzip.py aips_directory
# aips_directory is the parent directory of the AIP folders

import bagit
import csv
import datetime
import os
import re
import requests
import subprocess
import sys
import configuration as c

# Makes the aips_directory the current directory.
os.chdir(sys.argv[1])

# Makes a log, which will have WARC-level information.
# Includes column headers.
log = open("warc_unzip_log.csv", "w", newline="")
log_write = csv.writer(log)
log_write.writerow(["AIP", "WARC", "Fixity", "Unzipping"])

# Processes each AIP.
for aip in os.listdir("."):

    # Skips the log.
    if aip == "warc_unzip_log.csv":
        continue

    # Prints the script progress.
    print(f"Starting on {aip}")

    # Processes each WARC, which are in the AIP's objects folder.
    objects_path = os.path.join(aip, "objects")
    for warc in os.listdir(objects_path):

        # Starts a list with log information.
        log_row = [aip, warc]

        # Gets the WARC MD5 from Archive-It using WASAPI.
        warc_data = requests.get(f'{c.wasapi}?filename={warc}', auth=(c.username, c.password))
        if not warc_data.status_code == 200:
            log_row.append(f"API error {warc_data.status_code}: can't get MD5 for WARC")
            log_row.append("n/a: fixity error")
            log_write.writerow(log_row)
            continue
        py_warc = warc_data.json()
        warc_md5 = py_warc["files"][0]["checksums"]["md5"]

        # Calculates the md5 for the downloaded (zipped) WARC with md5deep.
        warc_path = os.path.join(objects_path, warc)
        md5deep_output = subprocess.run(f'"{c.MD5DEEP}" "{warc_path}"', stdout=subprocess.PIPE, shell=True)
        try:
            regex_md5 = re.match("b['|\"]([a-z0-9]*) ", str(md5deep_output.stdout))
            downloaded_warc_md5 = regex_md5.group(1)
        except AttributeError:
            log_row.append(f"Fixity for {warc} cannot be extracted from md5deep output: {md5deep_output.stdout}")
            log_row.append("n/a: fixity error")
            log_write.writerow(log_row)
            continue

        # Compares the md5 of the downloaded (zipped) WARC to Archive-It metadata.
        if not warc_md5 == downloaded_warc_md5:
            log_row.append(f"Fixity changed: AIT {warc_md5}, Downloaded {downloaded_warc_md5}")
            log_row.append("n/a: fixity error")
            log_write.writerow(log_row)
            continue
        else:
            log_row.append(f"Successfully verified fixity on {datetime.datetime.now()}")

        # # If the fixity matched in the previous step, unzips the WARC.
        # # The zipped WARC is automatically deleted. Work on a copy in case there is a problem.
        # # TODO: this hasn't been tested
        # unzip_output = subprocess.run(f"gunzip {warc_path}", stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, shell=True)
        # if not unzip_output == b'':
        #     log_row.append(f"Error while unzipping: {unzip_output.stderr.decode('utf-8')}")
        # else:
        #     log_row.append(f"Unzipped on {datetime.datetime.now()} with no error detected")
        log_write.writerow(log_row)

    # Bags the AIP and renames the folder to add "_bag".
    bagit.make_bag(aip, checksums=["md5", "sha256"])
    new_aip_name = f"{aip}_bag"
    os.replace(aip, new_aip_name)

    # Validates the bag.
    # Prints to the terminal if there is a validation error, since that is rare.
    new_bag = bagit.Bag(new_aip_name)
    try:
        new_bag.validate()
    except bagit.BagValidationError as errors:
        print(f"ERROR: Bag for {aip} is not valid.")
        print(errors)

# Closes the log.
log.close()
