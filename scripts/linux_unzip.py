"""Purpose: 7zip sometimes has errors with unzipping the downloaded gzipped WARCs due to a bug in PC zip programs.
When this happens, use this script to unzip the WARC in a Linux environment.

In this case (8-2022 batch), we need to re-download so the script input is web_aip_batch.py after web.download_warcs().
This means the metadata folder has Archive-It metadata reports and the objects folder has zipped WARCs.
To finish, run web_aip_batch.py without web.download_metadata() and web.download_warcs().

The MD5 for each WARC is double-checked before it is unzipped
and the MD5 for each unzipped WARC is saved to the log for checking before the AIP is made
to detect errors from moving the files between Windows and Linux environments."""

# usage: python linux_unzip.py aips_directory
# aips_directory is the parent directory of the AIP folders. It should only contain AIP folders.

import csv
import datetime
import hashlib
import os
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
log_write.writerow(["AIP", "WARC", "Fixity", "Unzipping", "Unzip_MD5"])

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

        # # Gets the WARC MD5 from Archive-It using WASAPI.
        # # Didn't end up using this because of the switch to MD5DEEP
        # warc_data = requests.get(f'{c.wasapi}?filename={warc}', auth=(c.username, c.password))
        # if not warc_data.status_code == 200:
        #     log_row.append(f"API error {warc_data.status_code}: can't get MD5 for WARC")
        #     log_write.writerow(log_row)
        #     continue
        # py_warc = warc_data.json()
        # warc_md5 = py_warc["files"][0]["checksums"]["md5"]

        # Calculates the MD5 for the downloaded (zipped) WARC.
        # Switched to MD5DEEP because of memory errors.
        warc_path = os.path.join(objects_path, warc)
        output = subprocess.run(f"md5deep {warc_path}", stdout=subprocess.PIPE, shell=True)
        log_row.append(output.stdout.decode('utf-8'))
        # with open(warc_path, "rb") as file:
        #     file_read = file.read()
        #     downloaded_warc_md5 = hashlib.md5(file_read).hexdigest()

        # # Compares the md5 of the downloaded (zipped) WARC to Archive-It metadata.
        # # Didn't end up using this because of the switch to MD5DEEP. Need to parse the output better to compare.
        # if not warc_md5 == downloaded_warc_md5:
        #     log_row.append(f"Fixity changed: AIT {warc_md5}, Downloaded {downloaded_warc_md5}")
        #     log_write.writerow(log_row)
        #     continue
        # else:
        #     log_row.append(f"Successfully verified fixity on {datetime.datetime.now()}")

        # The zipped WARC is automatically deleted. Work on a copy in case there is a problem.
        unzip_output = subprocess.run(f"gunzip {warc_path}", stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, shell=True)
        if not unzip_output.stderr == b'':
            log_row.append(f"Error while unzipping: {unzip_output.stderr.decode('utf-8')}")
            log_write.writerow(log_row)
            continue
        else:
            log_row.append(f"Unzipped on {datetime.datetime.now()} with no error detected")

        # Calculates the MD5 of the unzipped WARC so it can be tested before making the AIP.
        # The path to the unzipped WARC is the warc_path without the .gz extension.
        # Switched to MD5DEEP because of memory errors.
        output = subprocess.run(f"md5deep {warc_path[:-3]}", stdout=subprocess.PIPE, shell=True)
        log_row.append(output.stdout.decode('utf-8'))
        # with open(warc_path[:-3], "rb") as file:
        #     file_read = file.read()
        #     md5 = hashlib.md5(file_read).hexdigest()
        #     log_row.append(md5)

        # Saves the log information for any WARC that successfully unzipped.
        log_write.writerow(log_row)

# Closes the log.
log.close()
