"""Script to run backfills for flows."""

import logging
import os
from datetime import datetime

import click
import yaml
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery

# configure logging
logging.basicConfig(level=logging.INFO)


def get_month_list(start_month: str, end_month: str) -> list[str]:
    """Return list of months between start_month and end_month inclusive.

    Parameters
    ----------
    start_month : str
        Earliest month to include in the backfill
        Must have format %Y-%m
    end_month : str
        Final month to include in the backfill
        Must have format %Y-%m

    Returns
    -------
    list[str]
        List of all months from start_month to end_month inclusive
        as strings with the format %Y-%m
    """
    start_month_datetime = datetime.strptime(f"{start_month}-01", "%Y-%m-%d")
    end_month_datetime = datetime.strptime(f"{end_month}-01", "%Y-%m-%d")
    month_list = []

    current_month = start_month_datetime
    while current_month <= end_month_datetime:
        month_list.append(current_month.strftime("%Y-%m"))
        current_month += relativedelta(months=1)

    return month_list


def load_config_data(config_path: str) -> dict:
    """Load config data from file.

    Parameters
    ----------
    config_path : str
        config file path

    Returns
    -------
    dict
       dict of config data
    """
    with open(config_path, "rb") as infile:
        config_data = yaml.safe_load(infile)
    return config_data


def get_output_db_from_config(config_data: dict, test_mode: bool) -> str:
    """Return full output table from config.

    Parameters
    ----------
    config_data : doct
        config data as a dict
    test_mode : bool
        Indicates whether or not to use the test or prod table

    Returns
    -------
    str
        Full output table
    """
    if test_mode:
        # case where testing locally
        output_info = config_data["output"]["test"]
    else:
        output_info = config_data["output"]["prod"]
    target_table = (
        f"{output_info['project']}.{output_info['dataset']}.{output_info['table']}"
    )
    return target_table


@click.command()
@click.option(
    "--test_mode", default=True, help="Whether to write to test or production dataset"
)
@click.option(
    "--start_month",
    help="Earliest month to run the data for, format %Y-%m",
    required=True,
)
@click.option(
    "--end_month", help="Latest month to run the data for, format %Y-%m", required=True
)
@click.option("--config", help="path to config file to pass to flow", required=True)
@click.option("--flow", help="path to flow", required=True)
def run_backfill(
    start_month: str, end_month: str, test_mode: bool, config: str, flow: str
):
    """Run backfill for all months.

    Parameters
    ----------
    start_month : str
        Earliest month to include in the backfill
        Must have format %Y-%m
    end_month : str
        Final month to include in the backfill
        Must have format %Y-%m
    test_mode : bool
        Indicates whether or not to use the test or prod table
    config : str
        path to config file for flow from root of project
    flow : str
        path to flow module from root of project

    Raises
    ------
    ValueError
        Raised if there is already data for any input month
    """
    months_to_run = get_month_list(start_month, end_month)

    # set up client to check if data exists
    client = bigquery.Client(project="mozdata")
    config_data = load_config_data(config)
    output_table = get_output_db_from_config(config_data, test_mode)

    # get products for filtering
    # in tables where multiple flows write to the same place
    products = config_data["product"]
    products_string = ",".join([f"'{x}'" for x in products])
    logging.info("Writing to: %s", output_table)

    # check to make sure month doesn't exist
    # accounting for which products are in the config
    query = f"""SELECT DATE(forecast_month) AS month, COUNT(1) AS numrows
                    FROM {output_table}
                    WHERE product IN ({products_string})
                    GROUP BY 1"""
    output_df = client.query(query).to_dataframe()
    output_months = [x.strftime("%Y-%m") for x in output_df.month.values]
    overlap_months = set(output_months) & set(months_to_run)
    if len(overlap_months) > 0:
        included_months = ",".join(sorted(overlap_months))
        raise ValueError(f"Data for {included_months} already exists in {output_table}")

    for month in months_to_run:
        logging.info("Now running %s", month)

        command = [
            "uv",
            "run",
            flow,
            "run",
            f"--test_mode={test_mode}",
            f"--config={config}",
            f"--forecast_month={month}",
            "--write=True",  # always writing when doing backfill
        ]

        if test_mode:
            # if running locally, set METAFLOW_PROFILE
            # env var by appending this to the front
            command = ["METAFLOW_PROFILE=local"] + command
        else:
            # if running in OB, add info about the container to use
            image_loc = "us-docker.pkg.dev/moz-fx-mfouterbounds-prod-f98d/mfouterbounds-prod/moz-forecasting:latest"  # noqa: E501
            command.append(f"--with kubernetes:image={image_loc}")
        logging.info(f"Running process: {' '.join(command)}")
        os.system(" ".join(command))


if __name__ == "__main__":
    run_backfill()
