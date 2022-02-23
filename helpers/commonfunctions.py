import datetime
import requests


def pandas_difference_time(framedate, **kwargs):
    future_date = datetime.datetime.strptime(
                str(framedate),
                '%Y-%m-%d %H:%M:%S'
            )
    past_date = kwargs['objdate']

    difference = (future_date - past_date).total_seconds() / 60
    return difference


def calc_difference_time(futuretime, dctime):
    futuretime = datetime.datetime.strptime(
                str(futuretime),
                '%Y-%m-%d %H:%M:%S'
            )
    difference = (futuretime - dctime).total_seconds() / 60
    return difference


async def google_distance_time(origin, destination):
    url = f'https://maps.googleapis.com/maps/api/distancematrix/json?units=metric&origins={origin[1]},{origin[0]}&destinations={destination[1]},{destination[0]}&key=AIzaSyDZqoCtaDBN2VejoyeED3yPYbzgb6yEO0Q'
    result = requests.get(url).json()
    result = result['rows'][0]['elements'][0]
    distance = round(result['distance']['value']/1000)
    time = round(result['duration']['value']/60)
    return distance, time