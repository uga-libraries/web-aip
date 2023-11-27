# Preservation Download Workflow Part 2, 2023-10-18

## Overview

The UGA Libraries downloads all WARCs saved in a quarter (3-month period), along with a subset of the metadata reports.
Downloads are the first of February, May, August, and November for the 3 months prior.
The downloaded content is saved to the Libraries’ digital preservation system (ARCHive).

## Responsibility

UGA’s Archive-It Administrator (Head of Digital Stewardship) runs this workflow for Hargrett, MAGIL, and Russell.
It is not currently used by any other department at UGA.

## Workflow

### Part 1

Use the [Archive-It APIs scripts](https://github.com/uga-libraries/web-archive-it-api) 
to verify metadata completeness two weeks prior to the download and make a tracking spreadsheet for the download.

### Part 2: Download

Because the script can take days to run, due to the time required to download WARCs, it often gets interrupted. 
If this happens, running the script again will cause it to restart the seed that was in-progress when the error happened 
and download content for all seeds that had not started yet.
Any seed that already completed, even if it had errors, will not be downloaded again.

The script output is saved in the script output folder, defined in the configuration file.

1. Uses Archive-It API's, or the seeds_log.csv from an earlier iteration of the script, 
   to get data about the seeds for this download and make the metadata.csv used by the general-aip.py script. 
   

2. For each seed in the download:
   1. Makes a folder named with the seed id.
   2. Downloads the metadata reports.
   3. Deletes empty metadata reports (there is no metadata of that type in Archive-It).
   4. Redacts login information from the seed report.
   5. Downloads each WARC and verifies the fixity against the MD5 in Archive-It.
   6. Unzips each WARC.      
   7. Saves a summary of the errors, if any, to the log (seeds_log.csv).

   
3. Checks if everything expected was downloaded and makes a log (completeness_check.csv).


### Part 3

Use the [General AIP script](https://github.com/uga-libraries/general-aip) 
to transform the downloaded content into AIPs and ingest into our digital preservation system (ARCHive). 

   
## History

This workflow has been in use since 2020, but not previously documented beyond the scripts used and a Trello card template.