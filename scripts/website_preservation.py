"""
Purpose: Downloads archived web content and associated metadata from Archive-It.org using their APIs and converts it into aips that are ready to ingest into the UGA Libarries' digital preservation system (ARCHive).

There will be one aip per seed, even if that seed was crawled multiple times in the same quarter. A seed may have multiple aips in the system, as a new aip is made for every quarter's crawls.

Dependencies: 
    * Python libraries: requests, python-dateutil
    * Tools: bagit.py, fits, md5deep, saxon, xmllint

The script is run automatically every three months using crontab on our Linux server. Prior to the scheduled download, all seed metadata should be entered into Archive-It. Running this script will call all the other scripts needed for this workflow.
"""

import datetime
import os
import re
import subprocess

from seed_data import seed_data, seed_status
from warc_data import py_warcs, warc_status
from web_variables import webpres, web_scripts, aip_scripts, move_error


# Announces the start of the script.
# Including the date and time it starts to check how long it takes to run.

print(f'Starting web preservation script on {datetime.datetime.today()}.')


# Confirms the api calls in seed_data.py and warc_data.py had no errors.
# If there were errors, quits the script.

if warc_status == 'error' or seed_status == 'error':
    print('Error in one or both API calls. Check Archive-It availability and run the script again.')
    exit()


# Makes a folder for aips within the webpres folder, 
#     a designated place on the local machine for web archiving documents).
# The folder name includes today's date to keep it separate from previous downloads.
# Changes current directory to the aips folder.

if not os.path.exists(f'{webpres}/aips_{datetime.date.today()}'):
    os.makedirs(f'{webpres}/aips_{datetime.date.today()}')

os.chdir(f'{webpres}/aips_{datetime.date.today()}')


# ******************************************************************
# PART ONE: DOWNLOAD WARCS AND METADATA INTO AIP DIRECTORY STRUCTURE.
# This section is unique for web archives.
# ******************************************************************

# Starts counts for tracking script progress.
# Some processes are time consuming, so this shows the script is still working.

total_warcs = 0
for warc in py_warcs['files']:
    total_warcs += 1

current_warc = 0


# Iterates through data about each warc.
# Data was saved to a python object py_warcs in warc_data.py

for data in py_warcs['files']:

    #Updates the current warc number and displays the script progress.

    current_warc += 1
    print(f"\n>>>Processing {data['filename']} ({current_warc} of {total_warcs}).")


    # Calculates seed id, which is a portion of the filename between "-SEED" and "-".
    # Stops processing this warc and starts the next if the filename doesn't match expected pattern.

    try:
        regex = re.match('^.*-SEED(\d+)-', data['filename'])
        seed_id = regex.group(1)

    except AttributeError:
        print(f"Cannot calculate seed id for {data['filename']}")
        print('This warc will not be downloaded.')
        continue


    # Gets data from warc metadata that other scripts need and saves to variables.
    # These are given to the scripts as arguments.

    filename = data['filename']
    warc_url = data['locations'][0]
    warc_md5 = data['checksums']['md5']
    ait_collection = data['collection']


    # Gets data from seed metadata that other scripts need and saves to variables.
    # Data was saved to dictionary seed_data in seed_data.py.
    # These are given to the scripts as arguments.

    aip_id = seed_data[seed_id][0]
    aip_title = seed_data[seed_id][1]
    crawl_def = seed_data[seed_id][2]


    # Makes the aip directory for the seed's aip (aip folder with metadata and objects subfolders).

    subprocess.run(f'python3 {web_scripts}/aip_directory.py "{aip_id}" "{aip_title}"', shell=True)


    # Downloads the seed metadata from Archive-It into the seed's metadata folder.

    subprocess.run(f'python3 {web_scripts}/download_metadata.py "{aip_id}" "{aip_title}" "{filename}" "{ait_collection}" "{crawl_def}" "{seed_id}"', shell=True)


    # Downloads the warc(s) from Archive-It into the seed's objects folder.
    # Checks if the aip folder is present in case it was moved due to errors from downloading metadata.

    if f'{aip_id}_{aip_title}' in os.listdir('.'):
        subprocess.run(f'python3 {web_scripts}/download_warcs.py "{aip_id}" "{aip_title}" "{filename}" "{warc_url}" "{warc_md5}"', shell=True)


# Checks for empty metadata or objects folders in the aips.
# These would be the result of a download error that the script did not catch.

subprocess.run(f'python3 {web_scripts}/check_aip_directory.py', shell=True)


# ******************************************************************
# PART TWO: CREATE AIPS THAT ARE READY FOR INGEST INTO ARCHIVE
# This section is the same as the general aip workflow and uses those scripts.
# ******************************************************************

# Makes directories used to store script outputs, if they aren't already there.
# These are made in the same parent folder as the aips directory.

if not os.path.exists('../fits-xml'):
    os.makedirs('../fits-xml')

if not os.path.exists('../master-xml'):
    os.makedirs('../master-xml')

if not os.path.exists('../aips-to-ingest'):
    os.makedirs('../aips-to-ingest')


# Starts a count for tracking script progress.
# The current directory only contains aip folders, so the number of items equals the number of aips.
# Some processes are time consuming, so this shows the script is still working.

total_aips = len(os.listdir('.'))
current_aip = 0


# Runs the scripts for each step of making an aip, one aip at a time.
# Checks if the aip is still present before running each script, in case it was moved due to an error in the previous script.

for aip in os.listdir('.'):

    #Updates the current aip number and displays the script progress.

    current_aip += 1
    print(f'\n>>>Processing {aip} ({current_aip} of {total_aips}).')


    # Extracts the aip id, department, and aip title from the folder name and saves them to variables.
    #     (1) The aip id is before the first underscore and is only lowercase letters, numbers, or dashes.
    #     (2) The department is indicated by the first part of the aip id, either 'harg' or 'rbrl'.
    #     (3) The aip title is everything after the first underscore.
    # If the folder name doesn't match this pattern, moves the aip to an error folder and begins processing the next aip.

    try:
        regex = re.match('^((harg|rbrl)[a-z\d-]+)_(.*)', aip)
 
    except AttributeError:
        move_error('folder_name', aip)
        continue
 
    aip_id = regex.group(1)

    department = ''
    if regex.group(2) == 'harg':
        department = 'hargrett'

    elif regex.group(2) == 'rbrl':
        department = 'russell'

    else:
        move_error('department_name', aip)
        continue

    aip_title = regex.group(3)


    #Renames the folder from aipid_aiptitle to just the aip id.

    os.replace(aip, aip_id)  


    # Extracts technical metadata from the files using FITS.

    if aip_id in os.listdir('.'):
        subprocess.run(f'python3 "{aip_scripts}/fits.py" "{aip_id}"', shell=True)


    # Transforms the FITS metadata into the PREMIS master.xml file.

    if aip_id in os.listdir('.'):
        subprocess.run(f'python3 "{aip_scripts}/master_xml.py" "{aip_id}" "{aip_title}" "{department}"', shell=True)


    # Bags, tars, and zips the aip.

    if aip_id in os.listdir('.'):
        subprocess.run(f'python3 "{aip_scripts}/package.py" "{aip_id}"', shell=True)


# Makes a MD5 manifest of all aips the in this download using md5deep.
# The manifest is named manifest.txt and saved in the aips-to-ingest folder.
# The manifest has one line per aip, formatted md5<tab>filename

os.chdir('../aips-to-ingest')
subprocess.run(f'md5deep -b * > manifest.txt', shell=True)


# Announces the end of the script.
# Including the date and time it finishes to check how long it takes to run.

print(f'\nScript finished running at {datetime.datetime.today()}.')
