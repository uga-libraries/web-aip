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

   
4. Review seeds_log.csv and record the results in the preservation download tracker (Success or a summary of the errors)
   1. Metadata_Report_Empty field: in the tracker, note if collection or seed scope were empty 
      (not an error if there is no scope rule of that type, which is common) in their own columns, 
      and if any other report was empty (likely an error) under Other Report.
   2. Metadata_Report_Errors: in the tracker, note any errors to review under Other Report.
   3. Seed_Report_Redaction: in the tracker, note anything except "Successfully redacted" or "No login columns to redact". 
      Archive-It is inconsistent about if the login fields are present, even for the same seed.
   4. WARC_Download_Errors, WARC_Fixity_Errors, and WARC_Unzip_Errors should have a success message for every WARC. 
      If there is anything else, put it in the tracker under WARC Download.
   5. Complete: If a seed was successful, it will have Complete in the "Complete" column. 
      Otherwise, it will have the type of error and will need to be downloaded again.
   
   
5. Review completeness_check.csv and record the results in the preservation download tracker (Success or a summary of the errors)
   1. Seed Folder Made: should be TRUE 
   2. coll.csv and seed.csv: should be TRUE 
   3. collscope.csv and seedscope.csv: may be TRUE or FALSE, depending on if they are empty in seeds_log.csv. 
      Only record an error if it is FALSE and not empty. 
      During QC, we will check a few against Archive-It to confirm the empty had no scope rules of that type. 
   4. crawldef.csv count and crawljob.csv count: match the tracker 
   5. WARC Count Correct: should be TRUE 
   6. All Expected File Types: should be TRUE

   
6. Address any errors. This usually involves deleting the information from "Complete" in seeds_log.csv for any with errors and running the script again to re-download them. 
   Review seeds_log.csv (see step 4) and completeness_check.csv (see Step 5) for the re-downloads, and continue until all errors are addressed.
   If any errors are addressed manually (e.g., downloading directly from Archive-It interface), document the steps in seeds_log.csv.
   However, run the script again as much as possible for consistency and automatic logging.

### Part 3

Use the [General AIP script](https://github.com/uga-libraries/general-aip) 
to transform the downloaded content into AIPs and ingest into our digital preservation system (ARCHive). 

   
## History

This workflow has been in use since 2020, but not previously documented beyond the scripts used and a Trello card template.