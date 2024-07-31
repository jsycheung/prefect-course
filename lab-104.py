from prefect import flow, task, runtime
from prefect.tasks import task_input_hash
from datetime import timedelta
import httpx
from prefect.artifacts import create_markdown_artifact


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=1))
def fetch_humidity(latitude: float, longitude: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    humidity = httpx.get(
        base_url,
        params=dict(
            latitude=latitude, longitude=longitude, hourly="relative_humidity_2m"
        ),
    )
    forecasted_humidity = float(humidity.json()["hourly"]["relative_humidity_2m"][0])
    print(
        f"Flow run {runtime.flow_run.name} Forecasted humidity: {forecasted_humidity}%"
    )
    return forecasted_humidity


@task
def save_humidity(humidity: float):
    with open("humidity.csv", "w+") as w:
        w.write(f"The humidity is {str(humidity)}")
    return "Successfully wrote humidity"


@flow(log_prints=True, retries=1)
def humidity_pipeline(latitude: float = 38.9, longitude: float = -77.0):
    humidity = fetch_humidity(latitude, longitude)
    result = save_humidity(humidity)
    return result


@task
def report(precpiation_prob: float):
    markdown_report = f"""# Weather Report

## Recent weather

| Time        | Temperature |
|:--------------|-------:|
| Precipitation Probability Forecast  | {precpiation_prob} |
"""
    create_markdown_artifact(
        key="weather-report",
        markdown=markdown_report,
        description="Very scientific weather report",
    )


@flow
def fetch_precipitation_prob(latitude: float, longitude: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    humidity = httpx.get(
        base_url,
        params=dict(
            latitude=latitude, longitude=longitude, hourly="precipitation_probability"
        ),
    )
    precipitation_prob = float(
        humidity.json()["hourly"]["precipitation_probability"][0]
    )
    report(precipitation_prob)


# if __name__ == "__main__":
#     humidity_pipeline(latitude=42.4, longitude=-71.0).deploy(
#         name="my_humidity_deployment", work_pool_name="my_managed_workpool"
#     )
