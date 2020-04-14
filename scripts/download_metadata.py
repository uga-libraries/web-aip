# Purpose: Uses the Partner API to download metadata reports to include in the aips for archived websites.

import csv
import datetime
import os
import requests
import sys
from web_variables import api, u, p, webpres, move_error


# Data about the aip and seed is passed as arguments from website_preservation.py.

aip_id = sys.argv[1]
aip_title = sys.argv[2]
filename = sys.argv[3]
ait_collection = sys.argv[4]
crawl_def = sys.argv[5]
seed_id = sys.argv[6]


# The path for the metadata folder (used multiple times in this script).

metadata = f'{aip_id}_{aip_title}/metadata'


# ------------------------------------------------------
# PART ONE: DOWNLOADS SIX METADATA REPORTS FOR THE SEED.
# ------------------------------------------------------

# Starts a list for any download errors.
# For each report, if there is an error in downloading, adds an error to the list.
# Will try to download all reports before ending the script.

download_errors = []


# Gets the seed table for the seed as a csv file.
# Limit of -1 will return all matches. Default is only the first 100.

filters = {'limit':-1, 'id':seed_id,'format':'csv'}

report = requests.get(f'{api}/seed', params=filters, auth=(u, p))

if report.status_code == 200:
    with open(f'{metadata}/{aip_id}_seed.csv', 'wb') as doc:
        doc.write(report.content)
else:
    download_errors.append(f'seed: status code {report.status_code}')


# Gets the scope_rule table for the seed.

filters = {'limit':-1, 'seed':seed_id,'format':'csv'}

report = requests.get(f'{api}/scope_rule', params=filters, auth=(u, p))

if report.status_code == 200:
    with open(f'{metadata}/{aip_id}_seedscope.csv', 'wb') as doc:
        doc.write(report.content)
else:
    download_errors.append(f'seed scope: status code {report.status_code}')


# Gets the scope_rule table for the seed's Archive-It collection.

filters = {'limit':-1, 'collection':ait_collection,'format':'csv'}

report = requests.get(f'{api}/scope_rule', params=filters, auth=(u, p))

if report.status_code == 200:
    with open(f'{metadata}/{aip_id}_collscope.csv', 'wb') as doc:
        doc.write(report.content)
else:
    download_errors.append(f'collection scope: status code {report.status_code}')


# Gets the collection table for the seed's Archive-It collection.

filters = {'limit':-1, 'id':ait_collection,'format':'csv'}

report = requests.get(f'{api}/collection', params=filters, auth=(u, p))

if report.status_code == 200:
    with open(f'{metadata}/{aip_id}_coll.csv', 'wb') as doc:
        doc.write(report.content)
else:
    download_errors.append(f'collection: status code {report.status_code}')


# Gets the crawl job table for the seed's Archive-It collection.

filters = {'limit':-1, 'collection':ait_collection,'format':'csv'}

report = requests.get(f'{api}/crawl_job', params=filters, auth=(u, p))

if report.status_code == 200:
    with open(f'{metadata}/{aip_id}_crawljob.csv', 'wb') as doc:
        doc.write(report.content)
else:
    download_errors.append(f'crawl job: status code {report.status_code}')


# Gets the crawl definition table for the seed's crawl.

filters = {'limit':-1, 'id':crawl_def,'format':'csv'}

report = requests.get(f'{api}/crawl_definition', params=filters, auth=(u, p))

if report.status_code == 200:
    with open(f'{metadata}/{aip_id}_crawldef.csv', 'wb') as doc:
        doc.write(report.content)
else:
    download_errors.append(f'crawl definition: status code {report.status_code}')


# -------------------------------------------------------------------------------
# PART TWO: DELETES EMPTY FILES, REDACTS RESTRICTED INFORMATION, SAVES ERROR LOG.
# -------------------------------------------------------------------------------

# Iterate over each file in the metadata folder.
# If it has a file size of zero, it is deleted.
# If it is the seed report, the login columns are redacted.
# Otherwise, the file is left as it is.

for doc in os.listdir(metadata):

    # Full file path of the file so it can be deleted if needed.

    fullname = f'{webpres}/aips_{datetime.date.today()}/{metadata}/{doc}'


    # Deletes any empty metadata files (file size of 0) and begins processing the next file.
    # A file is empty if there is no metadata of that type in the Archive-It system.
    # Most commonly, this is either a collection or seed scope.

    if os.path.getsize(fullname) == 0:
        os.remove(fullname)
        continue


    # Redacts login password and username from the seed report.
    # Reads each row, stores that row with redactions in a list, and writes it back to the file.

    if doc.endswith('_seed.csv'):

        updated = []
        with open(fullname) as seedcsv:
            readcsv = csv.reader(seedcsv)

            # Adds the header row to the updated list.

            row1 = next(readcsv)
            updated.append(row1)


            # Checks the headers have the expected values for the columns that are redacted.
            # If not, moves to error folder and starts processing the next document.

            if not(row1[12] == 'login_password' and row1[13] == 'login_username'):
                move_error('seed_table_structure', f'{aip_id}_{aip_title}')
                continue


            # Puts 'REDACTED' in the password and username columns for all non-header rows.
            # This is done if the cell had data or was blank.
            # Adds the updated rows to the updated last.

            for row in readcsv:
                row[12] = 'REDACTED'
                row[13] = 'REDACTED'
                updated.append(row)


        # Opens the seed report again, but in write mode so can edit.
        # Gets each updated row from the updated list and saves to the csv.
        with open(fullname, 'w') as editcsv:
            writecsv = csv.writer(editcsv)
            for row in updated:
                writecsv.writerow(row)


# If there were any download errors, saves the errors to a text file (metadata_download_errors.txt)
# in the metadata folder and moves the aip folder to an error folder.
# This is done at end of this script so passwords are still redacted.

if len(download_errors) != 0:

    with open(f'{metadata}/metadata_download_errors.txt', 'w') as log:

        for error in download_errors:
            log.write(f'{error}\n')

    move_error('metadata_download', f'{aip_id}_{aip_title}')
