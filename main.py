import requests, os

from datetime import datetime, timedelta

API_KEY = os.getenv("WEATHER_API_KEY")
CITY = 'Portland'
STATE_CODE = 'OR'
COUNTRY_CODE = 'US'

def is_raining_now():
    url = f'https://api.openweathermap.org/data/2.5/weather?q={CITY},{STATE_CODE},{COUNTRY_CODE}&appid={API_KEY}&units=metric'
    response = requests.get(url)
    data = response.json()
    
    weather = data.get('weather', [])
    for w in weather:
        if 'rain' in w['main'].lower():
            return True
    return 'rain' in data.get('weather', [{}])[0].get('main', '').lower()

def will_rain_3days():
    url = f'https://api.openweathermap.org/data/2.5/forecast?q={CITY},{STATE_CODE},{COUNTRY_CODE}&appid={API_KEY}&units=metric'
    response = requests.get(url)
    data = response.json()

    now = datetime.now()
    laterr = now + timedelta(days=3)

    for forecast in data.get('list', []):
        forecast_time = datetime.fromtimestamp(forecast['dt'])
        if forecast_time > laterr:
            continue
        weather_list = forecast.get('weather', [])
        for w in weather_list:
            if 'rain' in w['main'].lower():
                return True
    return False

if __name__ == "__main__":
    raining_now = is_raining_now()
    rain_soon = will_rain_3days()

    print("A. Is it raining in Portland, OR right now?")
    print("Yes." if raining_now else "No.")

    print("\nB. Is it forecasted to be raining in Portland within the next three days?")
    print("Yes." if rain_soon else "No.")
