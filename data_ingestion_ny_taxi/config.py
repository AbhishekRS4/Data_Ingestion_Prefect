import os
from typing import List
from dataclasses import dataclass, field


@dataclass()
class DataDownloaderConfig:
    dir_dataset: str = field(default="dataset_ny_taxi")
    taxi_type: str = field(default="green")  # ["green", "yellow", "fhv", "fhvhv"]
    file_base_url: str = field(
        default="https://d37ci6vzurychx.cloudfront.net/trip-data"
    )
    month: int = field(default=1)
    year: int = field(default=2021)
    file_large: str = field(default="")
