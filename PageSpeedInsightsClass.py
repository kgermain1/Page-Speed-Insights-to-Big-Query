import requests
from datetime import datetime
import pandas as pd
import time
import threading

########### SET YOUR PARAMETERS HERE ####################################
PAGE_SPEED_API_KEY = 'enter-key-here'
URL_LIST = ['https://www.example.com/', 'https://www.google.com/']
################ END OF PARAMETERS ######################################

class PageSpeedInsightsAPI:
    """
    This is used for pulling data from the Google Page Speed Insights API, 
    enables pulling multiple pages worth of data efficiently using threading.
    

    The PageSpeed API is inconsistent in what it returns via JSON,
    if fields are empty they will be ommitted from the file and throw an error.
    In which case the script will return an empty dataframe for that URL.
    """
    def __init__(self, URL_LIST, PAGE_SPEED_API_KEY):
        self.run_speed_audit(URL_LIST, PAGE_SPEED_API_KEY)
    
    def getPageSpeedData(self, url, device, DATE, PAGE_SPEED_API_KEY, FRAMES):
    
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
        
        except Exception as E:
            print(E)
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
        
        try:
            FRAMES.append(df)
        except:
            return df
    
    def run_speed_audit(self, URL_LIST, PAGE_SPEED_API_KEY):
        DATE = str(datetime.now().date())
        FRAMES = []
        print("running threads...")
        threads = []
        
        for url in URL_LIST:
            tm = threading.Thread(target=self.getPageSpeedData, args=[url, 'mobile', DATE, PAGE_SPEED_API_KEY, FRAMES])
            tm.start()
            threads.append(tm)
            time.sleep(0.1)
            td = threading.Thread(target=self.getPageSpeedData, args=[url, 'desktop', DATE, PAGE_SPEED_API_KEY, FRAMES])
            td.start()
            threads.append(td)
            time.sleep(0.1)
        
        for thread in threads:
            thread.join()
            
        self.TOTAL_DF = pd.concat(FRAMES)
        return self.TOTAL_DF
    
def main():
    start = time.perf_counter() #starts a timer, useful for this API
    PS_OBJECT = PageSpeedInsightsAPI(URL_LIST, PAGE_SPEED_API_KEY)
    print(PS_OBJECT.TOTAL_DF)# prints out the overall dataframe you get back from the API
    finish = time.perf_counter()#ends the timer
    print(f'Finished in {round(finish-start, 2)} second(s)')
        
##Useful for debugging
# if __name__ == '__main__':
#     main()