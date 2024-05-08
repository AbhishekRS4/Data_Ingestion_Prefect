import os
import logging
import argparse
import numpy as np
import pandas as pd
import urllib.request

from prefect import task, flow

from config import DataDownloaderConfig


@task(log_prints=True)
def create_dir(dir_dataset: str) -> None:
    """
    ---------
    Arguments
    ---------
    dir_dataset: str
        full path of the dataset directory path

    -------
    Returns
    -------
    None
    """
    if not os.path.isdir(dir_dataset):
        os.makedirs(dir_dataset)
        logging.info(f"created dataset directory: {dir_dataset}")
    return


@task(retries=3, retry_delay_seconds=2, log_prints=True)
def download(config_downloader: DataDownloaderConfig) -> None:
    """
    ---------
    Arguments
    ---------
    config_downloader: DataDownloaderConfig
        object of class DataDownloaderConfig

    -------
    Returns
    -------
    None
    """
    file_name = f"{config_downloader.taxi_type}_tripdata_{config_downloader.year}-{config_downloader.month:02d}.parquet"
    file_url = f"{config_downloader.file_base_url}/{file_name}"

    urllib.request.urlretrieve(
        file_url, os.path.join(config_downloader.dir_dataset, file_name)
    )
    logging.info(f"downloaded file: {file_url}")

    return


@task(log_prints=True)
def load_df_month(config_downloader: DataDownloaderConfig) -> pd.DataFrame:
    """
    ---------
    Arguments
    ---------
    config_downloader: DataDownloaderConfig
        object of class DataDownloaderConfig

    -------
    Returns
    -------
    df_month: pd.DataFrame
        a dataframe with the monthly data
    """
    file_parq = f"{config_downloader.taxi_type}_tripdata_{config_downloader.year}-{config_downloader.month:02d}.parquet"
    df_month = pd.read_parquet(os.path.join(config_downloader.dir_dataset, file_parq))
    logging.info(f"loaded dataframe from file: {file_parq}")
    return df_month


@task(log_prints=True)
def merge_save_large_df(
    config_downloader: DataDownloaderConfig, df_month: pd.DataFrame
) -> None:
    """
    ---------
    Arguments
    ---------
    config_downloader: DataDownloaderConfig
        object of class DataDownloaderConfig
    df_month: pd.DataFrame
        a dataframe with the monthly data

    -------
    Returns
    -------
    None
    """
    df_year = None
    if os.path.isfile(
        os.path.join(config_downloader.dir_dataset, config_downloader.file_large)
    ):
        df_year = pd.read_parquet(
            os.path.join(config_downloader.dir_dataset, config_downloader.file_large)
        )
        df_year = pd.concat([df_year, df_month], sort=False)
    else:
        df_year = df_month
    df_year.to_parquet(
        os.path.join(config_downloader.dir_dataset, config_downloader.file_large),
        index=False,
    )
    logging.info(f"saved combined dataframe to: {config_downloader.file_large}")
    return


@flow(log_prints=True)
def data_ingestion_flow() -> None:
    month = 1
    year = 2021
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--month",
        default=month,
        type=int,
        help="month of the dataset to be downloaded",
    )
    parser.add_argument(
        "--year",
        default=year,
        type=int,
        help="year of the dataset to be downloaded",
    )
    ARGS, unparsed = parser.parse_known_args()

    logging.basicConfig(level=logging.INFO)
    config_downloader = DataDownloaderConfig(month=ARGS.month, year=ARGS.year)
    config_downloader.file_large = (
        f"{config_downloader.taxi_type}_{config_downloader.year}.parquet"
    )
    logging.info(f"NY Taxi dataset downloader config:{config_downloader}")

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
