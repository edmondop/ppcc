import httpx
from prefect import task, flow


@task
def fetch_weather(lat: float, lon: float):
    base_url = "https://archive-api.open-meteo.com/v1/archive"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, daily="shortwave_radiation_sum", start_date="2023-05-01", end_date="2023-05-16", timezone="PST"),
    )
    return weather.json()["daily"]["shortwave_radiation_sum"]

@task
def average_radiation_sum(records):
    count = 0
    total = 0.0
    for record in records:
        if record is not None:
            count +=1
            total += float(record)
    return total/count

@flow
def pipeline(location1: str, lat1: float, lon1: float, location2: str, lat2: float, lon2: float):
    radiation1 = fetch_weather(lat1, lon1)
    radiation1_avg = average_radiation_sum(radiation1)
    radiation2 = fetch_weather(lat2, lon2)
    radiation2_avg = average_radiation_sum(radiation2)
    ratio = (radiation1_avg - radiation2_avg) / radiation1_avg
    print(f"{location1} has {ratio:.0%} more or less sunwave than {location2}")

if __name__ == "__main__":
    wateridge_lat=32.8991812
    wateridge_long=-117.2078171
    boulder_lat=40.0293099
    boulder_long=-105.2399774
    miami_lat=25.7824075
    miami_long=-80.2294585
    seattle_lat=47.6130284
    seattle_long=-122.3420645
    pipeline('San Diego', wateridge_lat, wateridge_long,'Boulder', boulder_lat, boulder_long)
    pipeline('San Diego', wateridge_lat, wateridge_long,'Miami', miami_lat, miami_long)
    pipeline('San Diego', wateridge_lat, wateridge_long,'Seattle', seattle_lat, seattle_long)
    