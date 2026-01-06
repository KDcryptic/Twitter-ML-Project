import pandas as pd
import numpy as np
import requests
import os

from datetime import datetime
from dataclasses import dataclass
from src.exception import CustomException
from src.logger import logging


openWeatherKey = os.getenv('openWeatherKey')
base_url = "http://api.openweathermap.org/data/2.5/weather?"



namibia_settlements = [
    
    "Windhoek", "Dordabis", "Seeis",
    "Swakopmund", "Walvis Bay", "Arandis", "Usakos", "Omaruru", "Henties Bay",
    "Oshakati", "Ongwediva", "Ompundja",
    "Eenhana", "Helao Nafidi", "Okongo",
    "Outapi", "Tsandi", "Okahao", "Ruacana",
    "Tsumeb", "Omuthiya", "Oniipa",
    "Otjiwarongo", "Grootfontein", "Okakarara",
    "Opuwo", "Khorixas", "Sesfontein",
    "Katima Mulilo", "Kongola",
    "Rundu", "Divundu",
    "Nkurenkuru", "Mpungu",
    "Mariental", "Rehoboth", "Gibeon",
    "Keetmanshoop", "Lüderitz", "Oranjemund", "Karasburg",
    "Gobabis", "Leonardville", "Witvlei"

]

def getSettlementData(settlement):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={settlement}&appid={openWeatherKey}&units=metric"
    response = requests.get(url)
    data = response.json()

   
    if data.get("cod") != 200:
        logging.warning(f"Settlement '{settlement}' not found. Error: {data.get('message')}")
        return None

    settlementData = {
        'city': settlement,
        "lat": data["coord"]["lat"],
        "lon": data["coord"]["lon"],
        "temp": data["main"]["temp"],
        "feels_like": data["main"]["feels_like"],
        "temp_min": data["main"]["temp_min"],
        "temp_max": data["main"]["temp_max"],
        "pressure": data["main"]["pressure"],
        "humidity": data["main"]["humidity"],
        "wind_speed": data["wind"]["speed"],
        "clouds": data["clouds"]["all"],
        "weather": data["weather"][0]["description"],
        "sunrise": data["sys"]["sunrise"],
        "sunset": data["sys"]["sunset"],
        "timestamp": data["dt"]
    }

    return pd.DataFrame([settlementData])


def compileData(dataframes):
    df = pd.concat(dataframes, ignore_index=True)
    filename = datetime.now().strftime("%d_%m_%Y'")
    output_path = os.path.join("notebooks","data", f"weatherData_{filename}.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    logging.info(f" Weather data saved to {output_path}")



dataframes = []

for settlement in namibia_settlements:
    settlementDf = getSettlementData(settlement)
    if settlementDf is not None:   
        dataframes.append(settlementDf)
    else:
        logging.info(f" Skipped {settlement}")


compileData(dataframes)
   



