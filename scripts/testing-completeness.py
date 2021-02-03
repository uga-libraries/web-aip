# Testing calling check_aips(), which did not include the RBRL AIP in the 2/1 download.

import os
import configuration as c
import web_functions as web

# Variables that are typically calculated by the script
aips_directory = "aips_2021-02-01"
current_download = "2021-02-01"
last_download = "2020-11-01"
seed_to_aip = {"2173769": "harg-0000-web-202102-0001", "2184360": "harg-0000-web-202102-0002",
               "2027707": "rbrl-000-web-202102-0001", "2467335": "rbrl-new-web"}
log_path = "../script_log_2021-02-03.txt"

# Changes current directory to the AIPs folder.
os.chdir(f'{c.script_output}/{aips_directory}')

print('\nStarting completeness check.')
web.check_aips(current_download, last_download, seed_to_aip, log_path)
print('\nEnd of completeness check.')
