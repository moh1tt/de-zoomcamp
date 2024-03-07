import dlt
import itertools
import requests
import io
import pyarrow.parquet as pq


def load_taxi_data_service() -> None:

    # services = ["yellow", "green"]
    # years = ["2019", "2020", "2022"]
    # months = list(i for i in range(1, 13))

    services = ["yellow"]
    years = ["2019"]
    months = [1]

    # configure the pipeline: provide the destination and dataset name to which the data should go
    pipeline = dlt.pipeline(
        pipeline_name="load_taxi_data",
        destination='filesystem',
        dataset_name="ny_taxi_data",
    )

    for service, year, month in itertools.product(services, years, months):
        print(
            f"Now processing:\nService: {service}, Year: {year}, Month: {month}")
        month = f"{month:02d}"
        file_name = f"{service}_tripdata_{year}-{month}.parquet"
        request_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
        object_key = f"{service}/{year}/{service}_tripdata_{year}_{month}"

        print(f"request url: {request_url}")

        try:
            response = requests.get(request_url)
            response.raise_for_status()
            data = io.BytesIO(response.content)

            data = pq.read_table(data).to_pandas()
            print(
                f"Parquet loaded:\n{file_name}\nDataFrame shape:\n{data.shape}")

            info = pipeline.run(data, table_name=object_key,
                                write_disposition="replace")
            print(info)

        except requests.HTTPError as e:
            print(f"HTPP Error: {e}")


if __name__ == "__main__":
    load_taxi_data_service()
