# Purpose: Variables used in web archiving preservation scripts.
# These are either used by multiple scripts (which avoids redundancy) or are specific to a local machine (which makes it faster to set up the scripts on a new machine).

import os
import datetime
import dateutil.relativedelta


# Date of last preservation download (quarterly).
# Downloads are quarterly, so the last download was 3 months prior to the current date.

last_download = datetime.date.today() - dateutil.relativedelta.relativedelta(months=3)


# Date of last preservation download (manual).
# Use this to download files crawled since the specified date.

#last_download = 'YYYY-MM-DD'


# Root of the url for Archive-It Partner API.

api = 'https://partner.archive-it.org/api'


# Credentials for Archive-It account (username and password).

u = 'INSERT-USERNAME'
p = 'INSERT-PASSWORD'


# Path to where the script outputs are being saved.

webpres = 'INSERT-PATH'


# Path to the scripts for downloading content from Archive-It.
# These are the scripts in part one of website_preservation.py

web_scripts = 'INSERT-PATH'


# Paths to the scripts and stylesheets for making aips.
# These are in part two of website_preservation.py

aip_scripts = 'INSERT-PATH'
aip_stylesheets = 'INSERT-PATH'


# Function to move the aip folder to an error folder.
# The error folder is named with the error type and is made when the first instance of that error occurs.
# Moving the aip prevents the rest of the scripts from running on it.

def move_error(error_name, item):
    if not os.path.exists(f'../errors/{error_name}'):
        os.makedirs(f'../errors/{error_name}')
    os.replace(item, f'../errors/{error_name}/{item}')
