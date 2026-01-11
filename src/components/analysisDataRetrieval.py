import pandas as pd
import numpy as np
import boto3
import sys
from io import BytesIO
from io import StringIO
import os 

from datetime import datetime, timedelta, date
from src.exception import CustomException
from src.logger import logging
import pushbullet

s3 = boto3.client('s3')
bucketName = 'open-weather-data-storage'
yesterdaysDate = date.today() - timedelta(days=1)
realDate = str(yesterdaysDate)
day= realDate[-2] + realDate[-1]
month=realDate[5] + realDate[6]
year=realDate[0:4]
csv_buffer = BytesIO()
csv_buffer2 = StringIO()
filePath = f'raw/dailyDatasets/dailyWeatherData_{day}_{month}_{year}.csv'
fileName = f'dailyWeatherData_{day}_{month}_{year}.csv'
pbKey='o.W7x354jJw6oAAh9exQEAxEikAQQ89XTj'
pb = pushbullet.PushBullet(pbKey)


def downloadFile(csv_buffer,fileName):
    
    try:
        
        
        s3.download_fileobj(bucketName, f'{fileName}', csv_buffer)
        logging.info('File Downloaded!')
        csv_buffer = BytesIO(csv_buffer.getvalue())
        df = pd.read_csv(csv_buffer)

        return df
        
        
    except Exception as e:
        raise CustomException(e,sys)
    
def processData(df):
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
    cleanDf = df.dropna()
    cleanDf = cleanDf.astype({
    
    'lat':float,
    'lon':float,
    'temp':float,
    'feels_like':float,
    'temp_min':float,
    'temp_max':float,
    'pressure':float,
    'humidity':float,
    'wind_speed':float,
    'clouds':float,
    
                              
    })

    cleanDf['sunrise'] = pd.to_datetime(cleanDf['sunrise'], unit='s', utc=True)
    cleanDf['sunset'] = pd.to_datetime(cleanDf['sunset'], unit='s', utc=True)
    cleanDf['timestamp'] = pd.to_datetime(cleanDf['timestamp'], unit='s', utc=True,)
    cleanDf['sunrise'] = cleanDf['sunrise'].dt.tz_convert('Africa/Windhoek')
    cleanDf['sunset']  = cleanDf['sunset'].dt.tz_convert('Africa/Windhoek')

    
    cleanDf['sunrise_time'] = cleanDf['sunrise'].dt.time
    cleanDf['sunset_time'] = cleanDf['sunset'].dt.time

    cleanDf = cleanDf[~cleanDf['city'].isin(['Gibeon','Leonardville'])]

    cleanDf.drop(columns=['lat','lon','timestamp','sunset','sunrise'],inplace=True)
    cleanDf.loc[cleanDf['weather'] == "broken clouds", 'weather'] = 'Broken Clouds'
    cleanDf.loc[cleanDf['weather'] == "few clouds", 'weather'] = 'Few Clouds'
    cleanDf.loc[cleanDf['weather'] == "scattered clouds", 'weather'] = 'Scattered Clouds'
    cleanDf.loc[cleanDf['weather'] == "clear sky", 'weather'] = 'Clear Sky'
    cleanDf.loc[cleanDf['weather'] == "overcast clouds", 'weather'] = 'Overcast Clouds'
    cleanDf.loc[cleanDf['weather'] == "light rain", 'weather'] = 'Light Rain'
    cleanDf.loc[cleanDf['weather'] == "moderate rain", 'weather'] = 'Moderate Rain'
    cleanDf.loc[cleanDf['weather'] == "heavy rain", 'weather'] = 'Heavy Rain'

    cleanDf.loc[cleanDf['city']=='Lüderitz', 'city'] = 'Luderitz'

    cleanDf.loc[cleanDf["city"]=='Windhoek', 'Region']='Khomas'
    cleanDf.loc[cleanDf["city"].isin(['Swakopmund','Walvis Bay','Arandis','Usakos','Omaruru','Henties Bay']), 'Region']='Erongo'
    cleanDf.loc[cleanDf["city"].isin(['Oshakati','Ongwediva','Ompundja']), 'Region']='Oshana'
    cleanDf.loc[cleanDf["city"].isin(['Eenhana','Helao Nafidi']), 'Region']='Ohangwena'
    cleanDf.loc[cleanDf["city"].isin(['Tsandi','Okahao']), 'Region']='Omusati'
    cleanDf.loc[cleanDf["city"].isin(['Tsumeb','Oniipa']), 'Region']='Oshikoto'
    cleanDf.loc[cleanDf["city"].isin(['Otjiwarongo','Grootfontein','Okakarara']), 'Region']='Otjozondjupa'
    cleanDf.loc[cleanDf["city"].isin(['Opuwo','Khorixas']), 'Region']='Kunene'
    cleanDf.loc[cleanDf["city"].isin(['Katima Mulilo','Kongola']), 'Region']='Zambezi'
    cleanDf.loc[cleanDf["city"]=='Rundu', 'Region']='Kavango East'
    cleanDf.loc[cleanDf["city"].isin(['Mariental','Rehoboth']), 'Region']='Hardap'
    cleanDf.loc[cleanDf["city"].isin(['Keetmanshoop','Luderitz', 'Oranjemund','Karasburg']), 'Region']='Karas'
    cleanDf.loc[cleanDf["city"].isin(['Gobabis', 'Witvlei']), 'Region']='Omaheke'

    cleanDf.loc[cleanDf['Region'].isin(['Oshana','Ohangwena','Omusati','Oshikoto','Kunene','Zambezi','Kavango East']),'Direction']='North'
    cleanDf.loc[cleanDf['Region'].isin(['Khomas','Otjozondjupa','Omaheke']),'Direction']='Central'
    cleanDf.loc[cleanDf['Region']=='Erongo','Direction']='West'
    cleanDf.loc[cleanDf['Region'].isin(['Hardap','Karas']),'Direction']='South'

    return cleanDf

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
        pb.push_note('Processed Data Alert',f'{fileName} has been sent to datalake')

    except Exception as e:
        raise CustomException(e,sys)
    
cleanDf=processData(downloadFile(csv_buffer,filePath))
cleanDf.to_csv(csv_buffer2, index=False)
uploadDailyDataset(csv_buffer2,'processed/dailyDatasets', fileName)
                   
                   


