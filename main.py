import requests
from datetime import datetime
import csv
from google.cloud import bigquery
import pandas as pd
import time
import threading

########### SET YOUR PARAMETERS HERE ####################################
BQ_DATASET_NAME = 'DataSetName'
BQ_TABLE_NAME = 'TableName'
SERVICE_ACCOUNT_FILE = 'service-account-file-name.json'
PAGE_SPEED_API_KEY = 'insert-key-here'
################ END OF PARAMETERS ######################################

DATE = str(datetime.now().date())
URL_LIST = []
FRAMES = []
df = pd.DataFrame(columns=['date','URL','PSI','TTI','LCP','FID','CLS', 'SPI'])

def readCSV():
    with open('URLs.csv', newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            URL_LIST.append(row[0])

    return URL_LIST

def getPageSpeedData(url, device):

    endpoint = 'https://www.googleapis.com/pagespeedonline/v5/runPagespeed?url=' + url + '&key=' + PAGE_SPEED_API_KEY + '&strategy=' + device

    r = requests.get(endpoint)
    parsedJson = r.json()
    
    try:
        PSI = str(parsedJson["lighthouseResult"]["categories"]["performance"]["score"]*100)
        TTI = parsedJson["lighthouseResult"]["audits"]["interactive"]["displayValue"][:-2]
        LCP = parsedJson["lighthouseResult"]["audits"]["largest-contentful-paint"]["displayValue"][:-2]
        FID = parsedJson["lighthouseResult"]["audits"]["max-potential-fid"]["displayValue"][:-3]
        CLS = parsedJson["lighthouseResult"]["audits"]["cumulative-layout-shift"]["displayValue"]
        SPI = str(round(parsedJson["lighthouseResult"]["audits"]["speed-index"]["numericValue"]/1000, 2))
    except:
        PSI = ""
        TTI = ""
        LCP = ""
        FID = ""
        CLS = ""
        SPI = ""
    
    pageSpeedDict = {
            'date': [DATE],
            'device': [device],
            'URL': [url],
            'PSI': [PSI],
            'TTI': [TTI],
            'LCP': [LCP],
            'FID': [FID],
            'CLS': [CLS],
            'SPI': [SPI]
    }
    
    df = pd.DataFrame.from_dict(pageSpeedDict)
    FRAMES.append(df)

def concatDFs():
    TOTAL_DF = pd.concat(FRAMES)
    return TOTAL_DF

def loadToBigQuery(df):
    # establish a BigQuery client
    client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)

    dataset_id = BQ_DATASET_NAME
    table_name = BQ_TABLE_NAME
    
    # create a job config
    job_config = bigquery.LoadJobConfig()
    
    # Set the destination table
    table_ref = client.dataset(dataset_id).table(table_name)

    job_config.write_disposition = 'WRITE_APPEND'

    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()

def main():
    start = time.perf_counter()
    URL_LIST = readCSV()
    print("readCSV Done")
    print("running threads...")
    
    threads = []
    
    for url in URL_LIST:
        tm = threading.Thread(target=getPageSpeedData, args=[url, 'mobile'])
        tm.start()
        threads.append(tm)
        time.sleep(0.1)
        td = threading.Thread(target=getPageSpeedData, args=[url, 'desktop'])
        td.start()
        threads.append(td)
        time.sleep(0.1)
    
    for thread in threads:
        thread.join()
        
    TOTAL_DF = concatDFs()
    loadToBigQuery(TOTAL_DF)
    
    finish = time.perf_counter()
    print(f'Finished in {round(finish-start, 2)} second(s)')
        
if __name__ == '__main__':
    main()
