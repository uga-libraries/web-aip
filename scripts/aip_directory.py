# Purpose: Makes the aip directory structure:

#   aip-id_AIP Title
#       metadata
#       objects


import os
import sys


# The aip id and title are passed as arguments from website_preservation.py.

aip_id = sys.argv[1]
aip_title = sys.argv[2]

aip_folder = f'{aip_id}_{aip_title}' 


# Makes the metadata and objects folders within the aip folder, if they don't exist.
# Simultaneously makes the aip folder, if it doesn't exist.

if not os.path.exists(f'{aip_folder}/metadata'):
    os.makedirs(f'{aip_folder}/metadata')

if not os.path.exists(f'{aip_folder}/objects'):
    os.makedirs(f'{aip_folder}/objects')


