# Quality Control for Web AIPs

Review AIPs prior to ingesting into ARCHive (UGA's digital preservation system) to verify they were made correctly.
Some of the review is done by the script and the rest requires human review of a sample.

## Automated by the Script
The completeness_check() function produces a log with the results from checking the following:

1. All expected AIPs were made and no extra AIPs were made.
2. Each AIP has the correct number of WARCs.
3. Everything in the AIP object folder is a WARC.
4. Each AIP has the expected metadata reports.
5. Everything in the AIP metadata folder is an expected report type.

Additionally, the script moves AIPs that encounter anticipated errors to an error folder during processing. Review and address any AIPs in the error folder or with errors in the completeness log.

## Manual Review
Web AIPs are very consistent, so this review is done with a very small sample. Include an AIP with one WARC, multiple WARCs, no repeated metadata reports, and repeated metadata reports.

1. The directory structure is correct. There is a bag with the correct checksum manifests and the data folder contains a metadata folder and objects folder. 


2. The preservation.xml data is correct.
   1. Validate in ARCHive.
   2. Verify title matches Archive-It.
   3. Verify the information copied from FITS correctly.
   4. Verify the collection title in ARCHive for the related object identifier matches the creator in the AIP title.


3. After ingest into ARCHive.
   1. Verify the metadata matches preservation.xml.
   2. Copy one AIP back out to verify it is unchanged.
