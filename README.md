# Download Archive-It Web Content for Preservation

# Overview

Downloads WARCs and six metadata reports for crawls saved during a specified time period 
from the Archive-It web archiving service to use for creating a preservation copy of web crawls.

The downloaded content is saved to folders organized by seed.
The metadata reports are collection, collection scope, crawl definition, crawl job, seed, and seed scope. 
After the script is complete, the folders are ready to use as input for the [UGA Libraries' General AIP Script](https://github.com/uga-libraries/general-aip), 
which prepares them for UGA's digital preservation system (ARCHive).

UGA downloads web content using this script on a quarterly basis.

# Getting Started

## Dependencies

* md5deep (https://github.com/jessek/hashdeep) - used to calculate fixity of the downloaded WARC
* pandas - used to work with API data and CSV (log) data
* requests - used to get data via Archive-It APIs

## Installation

Prior to running the script, create a file named configuration.py, modeled after the configuration_template.py,
and save it to your local copy of this repository.
This defines a place for script output to be saved and includes your Archive-It login credentials.

This script must be run in Linux, due to Windows commonly having unzip errors with gzip.

## Script Arguments

Run the script in the command line: `python ait_download.py date_start date_end`

   * date_start is inclusive: the download will include WARCs stored on date_start.
   * date_end is exclusive: the download will not include WARCs stored on date_end.
   * Format both dates YYYY-MM-DD
   
## Testing

There are unit tests for all the script functions used by ait_download.py and for running the entire script.
The tests for check_seeds() could use more detail, which will be done once the function is updated.
The tests in test_script.py will fail if run at the same time as all other tests in the folder,
because one of the previous tests changes the current directory. 
Run test_script.py on its own for an accurate result.

The unit tests use UGA Archive-It data.
Any other organization will need to update the expected results with their own data.

# Workflow

1. Verify metadata completeness with the [Archive-It APIs scripts](https://github.com/uga-libraries/web-archive-it-api)
2. Download the WARCs and related metadata with this script: [download workflow documentation](documentation/Workflow_Preservation_Download_Part_2.md)
3. Transform the downloaded content into AIPs with the [General AIP script](https://github.com/uga-libraries/general-aip)
4. Ingest the AIPs into our digital preservation system (ARCHive)

# Author

Adriane Hanson, Head of Digital Stewardship, University of Georgia

# History

UGA Libraries has downloaded all WARCs and the six associated metadata reports for local preservation since 2020.
Originally, the WARCs were stored as zipped (gzip) files, which is how they are downloaded from Archive-It.
The WARCs were unzipped beginning with the August 2022 download, in line with new preservation format procedures.
They often had to be unzipped in Linux due to a Windows bug with gzip.
In November 2023, the entire script was switched to Linux to be more efficient.