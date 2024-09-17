# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

from datetime import datetime, timedelta

from metaflow import FlowSpec, step, project, IncludeFile
from google.cloud import bigquery
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import yaml

GCS_PROJECT_NAME = "moz-fx-data-bq-data-science"
GCS_BUCKET_NAME = "bucket-name-here"


def get_direct_allocation_df(
    allocation_config: list, min_month: pd.Timestamp, max_month: pd.Timestamp
) -> pd.DataFrame:
    """function to generate a dataframe where each record represents a
        single month and country, with the cumulative direct allocation
        in the "direct_sales_allocations" column

    Args:
        allocation_config (list): list of direct allocation segments
            must include 'live_markets' (country) and 'allocation' keys,
            `start_month` and `end_month` are optional
        min_month (pd.Timestamp): minimum month of the data the output
            of this function will be joined to, acts as the starting month for
            a segment when start_month is not set
        max_month (pd.Timestamp): maximum month of the data the output
            of this function will be joined to, acts as the ending month (inclusive) for
            a segment when end_month is not set

    Raises:
        ValueError: if the allocation is >100% for a given record an error is raised

    Returns:
        pd.DataFrame: dataframe containing direct allocation information for each
            country and month specified in the allocation_config
    """
    direct_allocation_df_list = []
    for segment in allocation_config:
        # if start_month isn't set for the direct allocation segment
        # use min_month
        first_month = min_month
        if "start_month" in segment:
            first_month = datetime.strptime(segment["start_month"], "%Y-%m")

        # if end_month isn't used for the direct allocation segment
        # use max_month
        last_month = max_month
        if "end_month" in segment:
            last_month = datetime.strptime(segment["end_month"], "%Y-%m")

        date_range = pd.date_range(first_month, last_month, freq="MS").tolist()
        df = pd.DataFrame(
            {
                "submission_month": date_range,
                "direct_sales_allocations": [segment["allocation"]] * len(date_range),
            }
        )
        for country in segment["markets"]:
            df["live_markets"] = country
            direct_allocation_df_list.append(df.copy())
    direct_allocation_df = pd.concat(direct_allocation_df_list)

    # the same month/country combination can be present in multiple
    # direct allocation segments
    # aggregate to get the sum
    direct_allocation_df = (
        direct_allocation_df.groupby(["submission_month", "live_markets"])
        .sum()
        .reset_index()
    )

    # ensure that no month/country combination is more than 100% allocated
    all_allocated = direct_allocation_df[
        direct_allocation_df["direct_sales_allocations"] > 1
    ]
    if len(all_allocated) > 0:
        raise ValueError(
            f"More than 100% of inventory allocated for direct sales\n{all_allocated}"
        )
    return direct_allocation_df


@project(name="ad_tiles_forecast")
class AdTilesForecastFlow(FlowSpec):
    """
    Flow for ads tiles forecasting
    """

    config = IncludeFile(
        name="config",
        is_text=True,
        help="configuration for flow",
        default="moz_forecasting/ad_tiles_forecast/config.yaml",
    )

    @step
    def start(self):
        """
        Each flow has a 'start' step.

        You can use it for collecting/preprocessing data or other setup tasks.
        """
        # load config
        self.config_data = yaml.safe_load(self.config)

        first_day_of_current_month = datetime.today().replace(day=1)
        last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
        first_day_of_previous_month = last_day_of_previous_month.replace(day=1)
        self.observed_start_date = first_day_of_previous_month - relativedelta(years=1)
        self.observed_end_date = last_day_of_previous_month

        # tables to get data from
        self.tile_data_table = "moz-fx-data-bq-data-science.jsnyder.tiles_results_temp"
        self.kpi_forecast_table = (
            "moz-fx-data-shared-prod.telemetry_derived.kpi_forecasts_v0"
        )
        self.active_users_aggregates_table = (
            "moz-fx-data-shared-prod.telemetry.active_users_aggregates"
        )

        self.next(self.get_tile_data)

    @step
    def get_tile_data(self):
        """
        retrieve tile impressions
        """
        tile_data_query = f"SELECT * FROM `{self.tile_data_table}`"
        client = bigquery.Client(project=GCS_PROJECT_NAME)
        hist_inventory = client.query(tile_data_query).to_dataframe()

        inventory = hist_inventory.copy()
        data_types_dict = {
            "country": str,
            "submission_month": str,
            "user_count": float,
            "impression_count_1and2": float,
            "visit_count": float,
            "clients": float,
            "total_inventory_1and2": float,
            "fill_rate": float,
        }
        inventory = inventory.replace(r"^\s*$", np.nan, regex=True).astype(
            data_types_dict
        )
        inventory["submission_month"] = pd.to_datetime(inventory["submission_month"])
        inventory.rename(columns={"country": "live_markets"}, inplace=True)

        self.inventory = inventory

        self.next(self.get_kpi_forecast)

    @step
    def get_kpi_forecast(self):
        """Get KPI forecast data"""
        forecast_date_start = self.observed_end_date.strftime("%Y-%m-%d")
        forecast_date_end_dt = self.observed_end_date.replace(day=1) + relativedelta(
            months=18
        )
        forecast_date_end = forecast_date_end_dt.strftime("%Y-%m-%d")

        observed_start_date = self.observed_start_date.strftime("%Y-%m-%d")
        observed_end_date = self.observed_end_date.strftime("%Y-%m-%d")

        # there can be multiple forecasts for a given date
        # joining to most_recent_forecasts selects only the most recent
        query = f"""
            WITH most_recent_forecasts AS (
                SELECT aggregation_period,
                    metric_alias,
                    metric_hub_app_name,
                    metric_hub_slug,
                    MAX(forecast_predicted_at) AS forecast_predicted_at
                FROM `moz-fx-data-shared-prod.telemetry_derived.kpi_forecasts_v0`
                GROUP BY aggregation_period, metric_alias, metric_hub_app_name, metric_hub_slug
            ),
            tmp_kpi_forecasts as (
            SELECT forecasts.* EXCEPT(forecast_parameters)
                FROM `{self.kpi_forecast_table}` AS forecasts
                JOIN most_recent_forecasts
            USING(aggregation_period, metric_alias, metric_hub_app_name, metric_hub_slug, forecast_predicted_at)
            )
        SELECT (tmp_kpi_forecasts.submission_date ) AS submission_month,
            AVG(tmp_kpi_forecasts.value ) AS cdau
        FROM tmp_kpi_forecasts
        WHERE (
            ((tmp_kpi_forecasts.measure ) = 'observed'  AND (( tmp_kpi_forecasts.submission_date  ) >= (DATE('{observed_start_date}')) AND ( tmp_kpi_forecasts.submission_date  ) < (DATE('{observed_end_date}'))))
            OR ((tmp_kpi_forecasts.measure ) = 'p50'  AND (( tmp_kpi_forecasts.submission_date  ) >= (DATE('{forecast_date_start}')) AND ( tmp_kpi_forecasts.submission_date  ) <= (DATE('{forecast_date_end}'))))
            )
        AND (tmp_kpi_forecasts.aggregation_period ) = 'month'
        AND (tmp_kpi_forecasts.metric_alias ) LIKE 'desktop_dau'
        GROUP BY
            1
        HAVING cdau IS NOT NULL
        """

        client = bigquery.Client(project=GCS_PROJECT_NAME)
        query_job = client.query(query)

        kpi_forecast = query_job.to_dataframe()

        self.kpi_forecast = kpi_forecast

        self.next(self.get_dau_by_country)

    @step
    def get_dau_by_country(self):
        """get dau by country"""
        # get markets from RPM
        live_markets = self.config_data["RPM"].keys()
        live_markets_string = ",".join(f"'{el}'" for el in live_markets)
        query = f"""
        SELECT
        (FORMAT_DATE('%Y-%m', submission_date )) AS submission_month,
        IF(country IN ({live_markets_string}), country, "Other") AS live_markets,
        COALESCE(SUM((dau) ), 0) AS dau_by_country
        FROM
        `{self.active_users_aggregates_table}` AS active_users_aggregates
        WHERE
        (app_name ) = 'Firefox Desktop'
        AND ((( submission_date ) >= ((DATE_ADD(DATE_TRUNC(CURRENT_DATE('UTC'), MONTH), INTERVAL -12 MONTH)))
            AND ( submission_date ) < DATE_TRUNC(CURRENT_DATE('UTC'), MONTH)))
        GROUP BY
        1,
        2
        """

        client = bigquery.Client(project=GCS_PROJECT_NAME)
        query_job = client.query(query)

        self.dau_by_country = query_job.to_dataframe()
        self.next(self.join_kpi_forecasts_and_historical_usage)

    @step
    def join_kpi_forecasts_and_historical_usage(self):
        """join observed values for dau and dau_by_country to get observed share by market"""
        # Join KPI forecast and historical usage distribution to get country-level KPI forecast
        self.kpi_forecast["submission_month"] = pd.to_datetime(
            self.kpi_forecast["submission_month"]
        )
        self.dau_by_country["submission_month"] = pd.to_datetime(
            self.dau_by_country["submission_month"]
        )

        kpi_forecast_observed = self.kpi_forecast[
            (self.kpi_forecast.submission_month >= self.observed_start_date)
            & (self.kpi_forecast.submission_month <= self.observed_end_date)
        ]

        dau_live_markets = self.dau_by_country[
            self.dau_by_country.live_markets != "Other"
        ]

        hist_dau = pd.merge(
            kpi_forecast_observed,
            dau_live_markets,
            how="left",
            on=["submission_month"],
        )

        hist_dau["share_by_market"] = hist_dau["dau_by_country"] / hist_dau["cdau"]
        self.hist_dau = hist_dau
        self.next(self.calculate_observed_dau_by_country)

    @step
    def calculate_observed_dau_by_country(self):
        """Get the mean of the observed share_by_market and inventory_per_country
        over time by country"""
        # Merge country level KPI forecast with inventory data

        inventory_observed_data_filter = (
            self.inventory.submission_month >= self.observed_start_date
        ) & (self.inventory.submission_month <= self.observed_end_date)
        inventory_observed = self.inventory.loc[
            inventory_observed_data_filter,
            ["submission_month", "live_markets", "total_inventory_1and2"],
        ]

        # recall 'live_markets' column has country codes as values
        hist_dau_inv = pd.merge(
            self.hist_dau,
            inventory_observed,
            how="inner",
            on=["live_markets", "submission_month"],
        )
        hist_dau_inv["inv_per_client"] = (
            hist_dau_inv["total_inventory_1and2"] / hist_dau_inv["dau_by_country"]
        )

        hist_avg = (
            hist_dau_inv.groupby("live_markets")
            .mean()[["share_by_market", "inv_per_client"]]
            .reset_index()
        )
        self.hist_avg = hist_avg
        self.next(self.calculate_forecasted_inventory_by_country)

    @step
    def calculate_forecasted_inventory_by_country(self):
        """merge country averages onto forecast to calculate forecast
        of the inventory by country"""
        kpi_forecast_future = self.kpi_forecast[
            self.kpi_forecast.submission_month > self.observed_end_date
        ]
        inventory_forecast = pd.merge(
            kpi_forecast_future,
            self.hist_avg,
            how="cross",
        )[
            [
                "submission_month",
                "live_markets",
                "cdau",
                "share_by_market",
                "inv_per_client",
            ]
        ]

        inventory_forecast["country_dau"] = (
            inventory_forecast["cdau"] * inventory_forecast["share_by_market"]
        )
        inventory_forecast["country_inventory"] = (
            inventory_forecast["country_dau"] * inventory_forecast["inv_per_client"]
        )
        self.inventory_forecast = inventory_forecast
        self.next(self.add_impression_forecast)

    @step
    def add_impression_forecast(self):
        """Add forecast for expected impressions"""
        six_months_before_obs_end = self.observed_end_date - relativedelta(months=6)
        observed_fill_rate_by_country = self.inventory.loc[
            (self.inventory.submission_month <= self.observed_end_date)
            & (self.inventory.submission_month >= six_months_before_obs_end),
            ["live_markets", "fill_rate"],
        ]

        average_fill_rate_by_country = (
            observed_fill_rate_by_country.groupby("live_markets").mean().reset_index()
        )

        self.revenue_forecast = pd.merge(
            self.inventory_forecast, average_fill_rate_by_country, on="live_markets"
        )
        self.revenue_forecast["expected_impressions_last_cap"] = (
            self.revenue_forecast["country_inventory"]
            * self.revenue_forecast["fill_rate"]
        )
        self.next(self.account_for_direct_allocations)

    @step
    def account_for_direct_allocations(self):
        """remove direct sales allocations from impressions forecast"""
        direct_allocation_df = get_direct_allocation_df(
            self.config_data["direct_allocations"],
            min_month=self.revenue_forecast["submission_month"].min(),
            max_month=self.revenue_forecast["submission_month"].max(),
        )
        self.revenue_forecast = self.revenue_forecast.merge(
            direct_allocation_df,
            on=["submission_month", "live_markets"],
            how="left",
        )

        self.revenue_forecast["direct_sales_markets"] = "n"
        self.revenue_forecast.loc[
            self.revenue_forecast["direct_sales_allocations"] > 0.0,
            "direct_sales_markets",
        ] = "y"

        self.revenue_forecast["direct_sales_allocations"] = self.revenue_forecast[
            "direct_sales_allocations"
        ].fillna(1.0)

        self.revenue_forecast["expected_impressions_direct_sales"] = (
            self.revenue_forecast["expected_impressions_last_cap"]
            * self.revenue_forecast["direct_sales_allocations"]
        )

        # self.revenue_forecast["expected_impressions_direct_sales"] = np.where(
        #     self.revenue_forecast.direct_sales_markets == "y",
        #     self.revenue_forecast["expected_impressions_last_cap"]
        #     * self.revenue_forecast["direct_sales_allocations"],
        #     self.revenue_forecast["expected_impressions_last_cap"],
        # )
        self.next(self.forecast_revenue)

    @step
    def forecast_revenue(self):
        """forecast revenue accounting for direct sales"""
        RPMs = self.config_data["RPM"]

        RPM_df = pd.DataFrame(
            [{"live_markets": key, "RPM": val} for key, val in RPMs.items()]
        )

        revenue_forecast = pd.merge(self.revenue_forecast, RPM_df, on="live_markets")

        after_valid_date = revenue_forecast["submission_month"] > "2024-09-01"
        revenue_forecast.loc[after_valid_date, "RPM"] = (
            revenue_forecast.loc[after_valid_date, "RPM"] * 1.1
        )

        # multiply inventory by RPMs
        revenue_forecast["revenue_no_ds"] = (
            pd.to_numeric(revenue_forecast["expected_impressions_last_cap"])
            * revenue_forecast["RPM"]
            / 1000
        )
        revenue_forecast["revenue_ds"] = (
            pd.to_numeric(revenue_forecast["expected_impressions_direct_sales"])
            * revenue_forecast["RPM"]
            / 1000
        )

        revenue_forecast.groupby(["submission_month"]).sum()[
            ["revenue_ds", "revenue_no_ds"]
        ]

        self.output_df = revenue_forecast

        self.next(self.end)

    @step
    def end(self):
        """
        This is the mandatory 'end' step: it prints some helpful information
        to access the model and the used dataset.
        """
        print(
            f"""
            Flow complete.

            {self.output_df}
            """
        )
        # write output
        self.output_df.to_parquet("metaflow_output.parquet", index=False)
        self.output_df["submission_month"] = self.output_df["submission_month"].astype(
            "datetime64[ms]"
        )
        nb_df = pd.read_parquet("nb_output_new.parquet")
        assert set(nb_df.columns) == set(self.output_df.columns)
        pd.testing.assert_frame_equal(
            nb_df.sort_values(["submission_month", "live_markets"]).reset_index(
                drop=True
            ),
            self.output_df[nb_df.columns]
            .sort_values(["submission_month", "live_markets"])
            .reset_index(drop=True),
            check_exact=False,
            rtol=0.05,
            check_dtype=False,
        )


if __name__ == "__main__":
    AdTilesForecastFlow()
