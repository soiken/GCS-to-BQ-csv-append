# GCS-to-BQ-csv-append
GCS to BQ csv append

## V1

This Google Cloud Function allows you to easily append data from a file stored in a Google Cloud Storage bucket to a specific table in a BigQuery dataset. The function starts by extracting metadata from the event that triggered the function and constructing the necessary file path to retrieve the file from the specified bucket. It then verifies that the file is located in the correct folder before proceeding with the data append process.

A BigQuery client object is constructed and used to retrieve the existing schema of the table to which the data will be appended. A load job configuration is created, including options for skipping leading rows and setting a time partitioning based on a specific column in the data. The data is then loaded into the table using the load_table_from_uri() method and the job is executed.

Upon successful completion of the load job, a message is printed to indicate that the data has been successfully appended to the specified table. This function provides a convenient and efficient way to keep your BigQuery tables up to date with new data as it becomes available in your storage bucket.


## V2

V2 uses a mapping file in JSON format, which maps the parent directory of the file to the relevant information for the BigQuery table, including the project ID, dataset ID, and table ID. Additionally, the function also supports time partitioning by a specified field in the mapping file.
