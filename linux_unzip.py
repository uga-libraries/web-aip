"""Purpose: 7zip sometimes has errors with unzipping the downloaded gzipped WARCs due to a bug in PC zip programs.
When this happens, use this script to unzip the WARC in a Linux environment.

The web_aip_download.py script moves the AIP to an error folder after web.download_warcs() if it didn't unzip.
This means the metadata folder has Archive-It metadata reports and the objects folder has zipped WARCs.
To finish making AIPs after this script, run aip_from_download.py.

The MD5 for each WARC is double-checked before it is unzipped
and the MD5 for each unzipped WARC is saved to the log for checking before the AIP is made
to detect errors from moving the files between Windows and Linux environments.

Initially tried to use hashlib to calculate MD5 but got memory errors and switched to md5deep."""

# usage: python linux_unzip.py aips_directory
# aips_directory is the parent directory of the AIP folders. It should only contain AIP folders.

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
log_write.writerow(["AIP", "WARC", "Zip_MD5", "Fixity_Comparison", "Unzipping_Result", "Unzip_MD5"])

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

        # Calculates the MD5 for the downloaded (zipped) WARC.
        warc_path = os.path.join(objects_path, warc)
        output = subprocess.run(f"md5deep {warc_path}", stdout=subprocess.PIPE, shell=True)
        try:
            regex_md5 = re.match("b['|\"]([a-z0-9]*) ", str(output.stdout))
            downloaded_warc_md5 = regex_md5.group(1)
        except AttributeError:
            downloaded_warc_md5 = output.stdout.decode('utf-8')
        log_row.append(downloaded_warc_md5)

        # Gets the MD5 for the WARC from Archive-It.
        warc_data = requests.get(f'{c.wasapi}?filename={warc}', auth=(c.username, c.password))
        if not warc_data.status_code == 200:
            print("API error getting WARC fixity. Skipping unzip for WARC:", warc)
            continue
        py_warc = warc_data.json()
        warc_md5 = py_warc["files"][0]["checksums"]["md5"]

        # Compares the expected MD5 from Archive-It to the downloaded (zipped) WARC.
        # Stops processing this WARC if they do not match.
        if not warc_md5 == downloaded_warc_md5:
            log_row.append(f"Fixity changed: AIT {warc_md5}, Downloaded {downloaded_warc_md5}")
            log_write.writerow(log_row)
            continue
        else:
            log_row.append(f"Successfully verified fixity on {datetime.datetime.now()}")

        # Unzips the WARC.
        # The zipped WARC is automatically deleted. Work on a copy in case there is a problem.
        unzip_output = subprocess.run(f"gunzip -f {warc_path}", stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, shell=True)
        if not unzip_output.stderr == b'':
            log_row.append(f"Error while unzipping: {unzip_output.stderr.decode('utf-8')}")
            log_write.writerow(log_row)
            continue
        else:
            log_row.append(f"Unzipped on {datetime.datetime.now()} with no error detected")

        # Calculates the MD5 of the unzipped WARC, so it can be tested after making the AIP.
        # The path to the unzipped WARC is the warc_path without the .gz extension (last 3 characters).
        output = subprocess.run(f"md5deep {warc_path[:-3]}", stdout=subprocess.PIPE, shell=True)
        log_row.append(output.stdout.decode('utf-8'))

        # Saves the log information for any WARC that successfully unzipped.
        log_write.writerow(log_row)

# Closes the log.
log.close()
