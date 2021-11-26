from google.cloud import bigquery
# import pandas as pd

########### SET YOUR PARAMETERS HERE ####################################
BQ_DATASET_NAME = 'dataset-name'
BQ_TABLE_NAME = 'table-name'
SERVICE_ACCOUNT_FILE = 'service-account-file-name.json'
################ END OF PARAMETERS ######################################

#For testing purposes only
# d = {'col1': [1, 2], 'col2': [3, 4]}
# df = pd.DataFrame(data=d)

class DataframeToBigQuery:
    """
    This is a simple load job to BigQuery. It takes a daframe as input and appends
    it to an existing table in BigQuery.
    
    If the table does not exist it will create one. The underlying dataset 
    does however need to exist prior to the script running
    """
    def __init__(self, df, SERVICE_ACCOUNT_FILE, BQ_DATASET_NAME, BQ_TABLE_NAME):
        self.loadDFToBigQuery(df, SERVICE_ACCOUNT_FILE, BQ_DATASET_NAME, BQ_TABLE_NAME)
        
    def loadDFToBigQuery(self, df, SERVICE_ACCOUNT_FILE ,BQ_DATASET_NAME, BQ_TABLE_NAME):
        # establish a BigQuery client
        client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
        
        # create a job config
        job_config = bigquery.LoadJobConfig()
        
        # Set the destination table
        table_ref = client.dataset(BQ_DATASET_NAME).table(BQ_TABLE_NAME)
    
        job_config.write_disposition = 'WRITE_APPEND'
    
        load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        load_job.result()

def main():
    DataframeToBigQuery(df, SERVICE_ACCOUNT_FILE, BQ_DATASET_NAME, BQ_TABLE_NAME)
        
# For testing purposes only   
# if __name__ == '__main__':
#     main()