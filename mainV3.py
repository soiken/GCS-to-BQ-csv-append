from google.cloud import bigquery, storage
import json 
import os

def append_data_to_table(event, context):
    # Get the file metadata from the event
    file = event
    bucket_name = file['bucket']
    file_name = file['name']
    file_path = f"{bucket_name}.{file_name}"
    uri = f"gs://{bucket_name}/{file_name}"

    # Open the mapping json file
    with open("mapping.json") as f:
        mapping = json.load(f)["mapping"]

    # Extract the parent directory of the file as the key for the mapping    
    key = os.path.dirname(file_name).split("/")[0]

    table_mapping = mapping.get(key)
    if not table_mapping:
        print(f'File {file_path} is not in a mapped folder. Exiting function.')
        return

    # Get the table information
    project_id = table_mapping.get("project_id")
    dataset_id = table_mapping.get("dataset_id")
    table_id = table_mapping.get("table_id")

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Table information
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    table_schema = table.schema

    # Create a load job configuration
    job_config = bigquery.LoadJobConfig(
    schema=table_schema,
    skip_leading_rows=1,
    )
    
    # Get write disposition from the JSON mapping
    write_disposition = table_mapping.get("write_disposition", "WRITE_APPEND")
    job_config.write_disposition = write_disposition

    # Extract the time partitioning field from the json if specified
    partitioning_field = table_mapping.get("time_partitioning_field")
    if partitioning_field:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field=partitioning_field
        )

    # Load the data from the file into the table
    load_job = client.load_table_from_uri(
        uri, table_ref, job_config=job_config)
    load_job.result()

    # Print a message to indicate that the data has been loaded
    print(f'Data from file {file_name} has been successfully {write_disposition} to table {table_id}.')
