from datetime import timedelta

import httpx
from prefect import flow, task
from prefect.tasks import task_input_hash


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def get_rain(lat: float, lon: float) -> float:
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="precipitation"),
    )
    return float(weather.json()["hourly"]["precipitation"][0])


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def get_humidity(lat: float, lon: float) -> float:
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="relativehumidity_2m"),
    )
    return float(weather.json()["hourly"]["relativehumidity_2m"][0])


@task(retries=3, retry_delay_seconds=5, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def get_temp(lat: float, lon: float) -> float:
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
    )
    # raise ValueError("This is a test")
    return float(weather.json()["hourly"]["temperature_2m"][0])


@task
def print_results(temp: float, humidity: float, rain: float) -> None:
    print(f"Temperature : {temp}, Relative Humitidy : {humidity}, Rain : {rain}%")


@flow(log_prints=True)
def test_flow(lat: float, lon: float):
    temp = get_temp(lat, lon)
    humidity = get_humidity(lat, lon)
    rain = get_rain(lat, lon)
    print_results(temp, humidity, rain)


if __name__ == "__main__":
    test_flow(2, 3)
