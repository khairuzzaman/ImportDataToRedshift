# Python Script for Upload Data to Redshift from SQL Server

The purpose of this script is to automate the manual task of uploading data to Redshift cluster from SQL Server. It works as like
##### 1. Export data from SQL Server to a text file using BCP command.
##### 2. Compressed the text file to gzip using gzip module.
##### 3. Upload the compressed file to s3 bucket.
##### 4. Truncate the target table of Redshift.
##### 5. Load the data from the compressed file using the copy command.
