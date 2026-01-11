from jinja2 import Environment, FileSystemLoader
import pdfkit
from datetime import date, timedelta
import io
from pathlib import Path
from src.exception import CustomException
from src.logger import logging
import sys
import pushbullet
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

import os
import boto3
from io import BytesIO
import base64


s3=boto3.client('s3')
pbKey='o.W7x354jJw6oAAh9exQEAxEikAQQ89XTj'
pb = pushbullet.PushBullet(pbKey)
bucketName=bucketName = 'open-weather-data-storage'
env = Environment(loader=FileSystemLoader('.'))
template = env.get_template('weatherReportTemplate.html')
yesterdaysDate = date.today() - timedelta(days=1)
realDate = str(yesterdaysDate)
day= realDate[-2] + realDate[-1]
month=realDate[5] + realDate[6]
csv_buffer=BytesIO()
year=realDate[0:4]
filePath = f'processed/dailyDatasets/dailyWeatherData_{day}_{month}_{year}.csv'


cwd = Path.cwd()

def downloadFiles(csv_buffer,fileName):
    
    try:
        
        
        s3.download_fileobj(bucketName, f'{fileName}', csv_buffer)
       
        csv_buffer = BytesIO(csv_buffer.getvalue())
        df = pd.read_csv(csv_buffer)

        return df
    except Exception as e:
        raise CustomException(e,sys)

def uploadDailyReport(dataPath,folder,fileName):

    try:

        key=f'{folder}/{fileName}'
        s3.put_object(
        Bucket=bucketName,
        Key=key,
        Body=dataPath.getvalue(),
        ContentType="application/pdf"
    )
        logging.info(f"Uploaded {key} to {bucketName}")

    except Exception as e:
        raise CustomException(e,sys)


eda_path = cwd / 'notebooks/EDA.ipynb'

df = downloadFiles(csv_buffer,filePath)
df2 = df.groupby('city')[['city','temp','clouds','pressure','humidity']].max().head(35)
earlySunrise =df.groupby('city')[['city','sunrise_time','sunset_time']].head(35)
corr_matrix= df.corr(numeric_only=True)
sns.heatmap(corr_matrix, annot=True, cmap="coolwarm", center=0)
buf=BytesIO()
plt.savefig(buf, format="png")
buf.seek(0)
plot_data = base64.b64encode(buf.read()).decode("utf-8")




# Example data
weather_data = df2.to_dict(orient="records")
earliestSunrise = earlySunrise.to_dict(orient="records")

html_out = template.render(
    date=f'{day}-{month}-{year}',
    weather_data=weather_data,
    earliestSunrise=earliestSunrise,
    plot_data=plot_data

)

# Convert to PDF
pdf_data = pdfkit.from_string(html_out, False)
pdf_io = io.BytesIO(pdf_data)

uploadDailyReport(pdf_io,'reports/dailyReports',f'daily_report_{day}_{month}_{year}.pdf')
pb.push_note('Daily Weather Report',f'daily_report_{day}_{month}_{year}.pdf uploaded to datalake')