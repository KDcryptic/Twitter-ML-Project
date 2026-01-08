import pandas as pd
import numpy as np
import requests
import os
import json
import sys
import boto3
import pushbullet
from datetime import datetime

from datetime import datetime
from dataclasses import dataclass
from src.exception import CustomException
from src.logger import logging
from io import BytesIO
from io import StringIO


s3 = boto3.client('s3')
s3Resource = boto3.resource('s3')
bucketName = 'open-weather-data-storage'
pbKey='o.W7x354jJw6oAAh9exQEAxEikAQQ89XTj'
pb = pushbullet.PushBullet(pbKey)
s3Bucket = s3Resource.Bucket(bucketName)
csv_buffer = BytesIO()
csv_buffer2 = StringIO()

def downloadFiles(csv_buffer,fileName):
    
    try:
        
        
        s3.download_fileobj(bucketName, f'{fileName}', csv_buffer)
        logging.info('File Downloaded!')
        csv_buffer = BytesIO(csv_buffer.getvalue())
        df = pd.read_csv(csv_buffer)

        return df
        
        
    except Exception as e:
        raise CustomException(e,sys)

keys =[]
dataframes=[]
todaysDate = datetime.now().strftime("%d_%m_%Y")
def getFileNames(date):
    str(date)
    for obj in s3Bucket.objects.filter(Prefix='raw/hourlyDatasets/'):
        if date in obj.key:
            keys.append(obj.key)
            logging.info(keys)
    
    return keys
            

def uploadDailyDataset(dataPath,folder,fileName):

    try:

        key=f'{folder}/{fileName}'
        s3.put_object(
        Bucket=bucketName,
        Key=key,
        Body=dataPath.getvalue(),
        ContentType="text/csv"
    )
        logging.info(f"Uploaded {key} to {bucketName}")

    except Exception as e:
        raise CustomException(e,sys)
    
    


def compileData(dataframes):

    try:
        logging.info(f'receives {dataframes}')
        df = pd.concat(dataframes, ignore_index=True)
        logging.info('daily dataset compiled')
        fileName = f'dailyWeatherData_{datetime.now().strftime("%d_%m_%Y")}.csv'
        df.to_csv(csv_buffer2, index=False)
        logging.info('Data stored in string buffer')
        uploadDailyDataset(csv_buffer2,'raw/dailyDatasets',fileName)
        pb.push_note('Daily Data Alert',f'{fileName} data sent to datalake')
        logging.info(f" Weather data saved to {'datalake'}")
    
    except Exception as e:
        raise CustomException(e,sys)
        
        
    
for key in getFileNames(todaysDate):
            logging.info(f'downloading {key}')
            df = downloadFiles(csv_buffer,key)
            logging.info('dataset downloaded')
            dataframes.append(df)
            logging.info('downloaded dataset added')

compileData(dataframes)
logging.info('File uploaded to datalake')


