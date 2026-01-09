import pandas as pd
import numpy as np
import boto3
import sys
import os 

from datetime import datetime, timedelta, date
from src.exception import CustomException
from src.logger import logging

s3 = boto3.client('s3')
bucketName = 'open-weather-data-storage'
yesterdaysDate = date.today() - timedelta(days=1)
realDate = str(yesterdaysDate)
day= realDate[-2] + realDate[-1]
month=realDate[5] + realDate[6]
year=realDate[0:4]
filePath = os.path.join('notebooks','data',f'dailyWeatherData_{day}_{month}_{year}.csv')


def downloadRawData(bucketName,path,day,month,year):

    try:
        with open(path, 'wb') as f:
            s3.download_fileobj(bucketName,f'raw/dailyDatasets/dailyWeatherData_{day}_{month}_{year}.csv',f)
            logging.info('Today\'s weather data has been saved')

    except Exception as e:
        raise CustomException(e,sys)

downloadRawData(bucketName,filePath,day,month,year)



    
