# Purpose: Downloads warc files and confirms fixity is unchanged after downloading.

# Dependencies: md5deep

import datetime
import re
import requests
import subprocess
import sys
from web_variables import u, p, move_error, webpres


# Data about the aip and warc is passed as arguments from website_preservation.py.

aip_id = sys.argv[1]
aip_title = sys.argv[2]
filename = sys.argv[3]
warc_url = sys.argv[4]
pre_md5 = sys.argv[5]

# The path for where the warc is saved on the local machine (it is long and used twice in this script).

warc_path = f'{webpres}/aips_{datetime.date.today()}/{aip_id}_{aip_title}/objects/{filename}'


# Downloads the warc(s).
# Saves the warc(s) in the objects folder, keeping the original filename.
# If there is an error in downloading (status is not 200), moves the aip to an error folder.

download = requests.get(f"{warc_url}", auth=(u, p))

if download.status_code == 200:
    with open(warc_path, 'wb') as warcfile:
        warcfile.write(download.content)
else:
    move_error(f'warc_download_status_{download.status_code}', f'{aip_id}_{aip_title}')
    exit()


# The rest of the script compares the warc md5 before and after download.

# Calculates the md5 for the downloaded warc.
# Uses a regular expression to get the md5 from the md5deep output.
# If the regular expression doesn't match anything, the post_md5 is given a value of 'none' so it is caught as an error.

result = subprocess.run(f'md5deep "{warc_path}"', stdout=subprocess.PIPE, shell=True)

try:
    regex = re.match("^.*b'([a-z0-9]*) ", str(result))
    post_md5 = regex.group(1)

except:
    post_md5 = 'none'


# Compares the md5 of the download warc (post_md5) to what Archive-It has for the warc (pre-md5).
# If post_md5 could not be calculated or is different, moves the aip to an error folder.

if not pre_md5 == post_md5:
    move_error('warc_fixity_changed', f'{aip_id}_{aip_title}')
