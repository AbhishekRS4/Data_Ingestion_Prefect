import os
import logging
import argparse
import numpy as np
import pandas as pd
import urllib.request

from prefect import task, flow

from config import DataDownloaderConfig

df_year = None

@task()
def create_dir(dir_dataset: str) -> None:
    if not os.path.isdir(dir_dataset):
        os.makedirs(dir_dataset)
        logging.info(f"created dataset directory: {dir_dataset}")
    return

@task(retries=3, retry_delay_seconds=2)
def download(config_downloader: DataDownloaderConfig) -> None:
    logging.info(f"NY Taxi dataset downloader config:{config_downloader}")

    if not os.path.isdir(config_downloader.dir_dataset):
        logging.info(f"created dataset directory: {config_downloader.dir_dataset}")
        os.makedirs(config_downloader.dir_dataset)

    file_name = f"{config_downloader.taxi_type}_tripdata_{config_downloader.year}-{config_downloader.month:02d}.parquet"
    file_url = f"{config_downloader.file_base_url}/{file_name}"

    try:
        urllib.request.urlretrieve(
            file_url, os.path.join(config_downloader.dir_dataset, file_name)
        )
        logging.info(f"downloaded file: {file_url}")
    except:
        logging.info(f"file url not found: {file_url}")

    return

@task()
def load_df_month(config_downloader: DataDownloaderConfig) -> pd.DataFrame:
    file_parq = f"{config_downloader.taxi_type}_tripdata_{config_downloader.year}-{config_downloader.month:02d}.parquet"
    df_month = pd.read_parquet(os.path.join(config_downloader.dir_dataset, file_parq))
    return df_month

@task()
def merge_save_large_df(config_downloader: DataDownloaderConfig, df_month: pd.DataFrame) -> None:
    df_large = None
    if os.path.isfile(os.path.join(config_downloader.dir_dataset, config_downloader.file_large)):
        df_large = pd.read_parquet(os.path.join(config_downloader.dir_dataset, config_downloader.file_large))
        df_large = pd.concat([df_large, df_month], sort=False)
    else:
        df_large = df_month
    df_large.to_parquet(os.path.join(config_downloader.dir_dataset, config_downloader.file_large), index=False)
    return

@flow
def data_ingestion_flow() -> None:
    month = 1
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--month",
        default=month,
        type=int,
        help="month of the dataset to be downloaded",
    )
    ARGS, unparsed = parser.parse_known_args()

    config_downloader = DataDownloaderConfig(month=ARGS.month)
    config_downloader.file_large = f"{config_downloader.taxi_type}_{config_downloader.year}.parquet"

    # create dataset directory
    create_dir(config_downloader.dir_dataset)

    # download
    download(config_downloader)

    # load df from parquet
    df_month = load_df_month(config_downloader)

    # merge and save yearly data
    merge_save_large_df(config_downloader, df_month)

    return

if __name__ == "__main__":
    data_ingestion_flow()