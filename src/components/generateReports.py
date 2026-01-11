import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt 
from datetime import date,timedelta
import os
import boto3
from io import BytesIO
import pdfkit

from nbconvert import HTMLExporter

import sys
import pushbullet

s3=boto3.client('s3')
pbKey='o.W7x354jJw6oAAh9exQEAxEikAQQ89XTj'
pb = pushbullet.PushBullet(pbKey)
bucketName=bucketName = 'open-weather-data-storage'
csv_buffer = BytesIO()
yesterdaysDate = date.today() - timedelta(days=1)
realDate = str(yesterdaysDate)
day= realDate[-2] + realDate[-1]
month=realDate[5] + realDate[6]
year=realDate[0:4]
filePath = f'processed/dailyDatasets/dailyWeatherData_{day}_{month}_{year}.csv'
from src.exception import CustomException
from src.logger import logging
import subprocess
import papermill as pm
import nbformat
from nbconvert import PDFExporter
import io

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


eda_path = 'C:\\Users\\KimKa\\Desktop\\ML Project 2\\notebooks\\EDA.ipynb'


nb=pm.execute_notebook(
    str(eda_path),
    "-" ,      
    return_output=True   
)

nb_json = nbformat.writes(nb)

html_exporter = HTMLExporter()
html_data, _ = html_exporter.from_notebook_node(nb)

pdf_data = pdfkit.from_string(html_data, False)  # False = return bytes
pdf_io = io.BytesIO(pdf_data)




uploadDailyReport(pdf_io,'reports/dailyReports',f'daily_report_{day}_{month}_{year}.pdf')
pb.push_note('Daily Weather Report',f'daily_report_{day}_{month}_{year}.pdf uploaded to datalake')







