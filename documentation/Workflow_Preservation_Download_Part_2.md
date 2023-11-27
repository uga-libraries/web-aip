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

1. Change your computer power settings to not go to sleep. A quarterly batch is usually about 100 GB and takes several days to download.


2. Run the ait_download.py script, with a start_date of the previous quarterly download date (e.g., 2023-08-01) and an end_date of the current quarterly download date (e.g., 2023-11-01). The script will:
   1. Get data about the seeds in the download from the Archive-It API and create the seeds_log.csv file and metadata.csv file.
   2. Make a folder for each seed, named with the seed id, in the script_output folder.
   3. Download the metadata reports, deleting empty ones and redacting login information from the seed report.
   4. Download each WARC, verify its fixity, and unzip it.
   5. Save a summary of errors, if any, to the seeds_log.csv
   6. Checks if everything expected was downloaded and makes a log, completeness_check.csv
   

3. If the script is interrupted before it is complete, it can be restarted. 
   Run the script again, with the same arguments
   It will download anything with a blank "Complete" column in seeds_log.csv and update the logs. 
   It will restart the seed that was in progress when the script was interrupted, re-downloading anything that was previously downloaded.
   It will not retry a seed that completed but had errors.
   To download fewer at a time, put text in the Complete column, leaving a few blank, and run the script multiple times, deleting the text from Complete a few at a time.

### Part 3

Use the [General AIP script](https://github.com/uga-libraries/general-aip) 
to transform the downloaded content into AIPs and ingest into our digital preservation system (ARCHive). 

   
## History

This workflow has been in use since 2020, but not previously documented beyond the scripts used and a Trello card template.