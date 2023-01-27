from google.cloud import bigquery, storage

def append_data_to_table(event, context):
    # Get the file metadata from the event
    file = event
    bucket_name = file['bucket']
    file_name = file['name']
    file_path = (f"{bucket_name}.{file_name}")
    uri = f"gs://{bucket_name}/{file_name}"

    # Verify that the file is in the 'bucket.test/' folder 
    if not file_path.startswith('bucket.test/'): # update this with your file path
        print(f'File {file_path} is not in the correct folder. Exiting function.')
        return

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Table information
    project_id = "project_id" # update this with your project id
    dataset_id = "dataset_id" # update this with your dataset id
    table_id = "table_id" # update this with your table id

    # Retrieve the existing schema of the table
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    table_schema = table.schema

    # Create a load job configuration
    job_config = bigquery.LoadJobConfig(
    schema=table_schema,
    skip_leading_rows=1,
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH,
        field="timestamp",  # Name of the column to use for partitioning.
    ),
    )
    job_config.write_disposition = 'WRITE_APPEND'

    # Load the data from the file into the table
    load_job = client.load_table_from_uri(
        uri, table_ref, job_config=job_config)
    load_job.result()

    # Print a message to indicate that the data has been loaded
    print(f'Data from file {file_name} has been successfully appended to table {table_id}.')
