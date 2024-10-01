"""Flow for the Ad Tiles Forecast."""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

from datetime import datetime, timedelta

import pandas as pd
import yaml
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
from metaflow import FlowSpec, IncludeFile, Parameter, project, step

GCS_PROJECT_NAME = "moz-fx-data-bq-data-science"
GCS_BUCKET_NAME = "bucket-name-here"


def get_direct_allocation_df(
    allocation_config: list, min_month: pd.Timestamp, max_month: pd.Timestamp
) -> pd.DataFrame:
    """Generate dataframe for direct allocation.

    Creates a dataframe where each record represents a
        single month and country, with the cumulative direct allocation
        in the "direct_sales_allocations" column

    Args:
        allocation_config (list): list of direct allocation segments
            must include 'country' (country) and 'allocation' keys,
            `start_month` and `end_month` are optional
        min_month (pd.Timestamp): minimum month of the data the output
            of this function will be joined to, acts as the starting month for
            a segment when start_month is not set
        max_month (pd.Timestamp): maximum month of the data the output
            of this function will be joined to, acts as the ending month (inclusive) for
            a segment when end_month is not set

    Raises
    ------
        ValueError: if the allocation is >100% for a given record an error is raised

    Returns
    -------
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
            df["country"] = country
            direct_allocation_df_list.append(df.copy())
    direct_allocation_df = pd.concat(direct_allocation_df_list)

    # the same month/country combination can be present in multiple
    # direct allocation segments
    # aggregate to get the sum
    direct_allocation_df = (
        direct_allocation_df.groupby(["submission_month", "country"])
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


def vectorized_date_to_month(series: pd.Series) -> pd.Series:
    """Turn datetime into the first day of the corresponding month.

    Parameters
    ----------
    series : pd.Series
       series of datetimes

    Returns
    -------
    pd.Series
        datetimes set to the first day of the month
    """
    return pd.to_datetime({"year": series.dt.year, "month": series.dt.month, "day": 1})


@project(name="ad_tiles_forecast")
class AdTilesForecastFlow(FlowSpec):
    """Flow for ads tiles forecasting."""

    config = IncludeFile(
        name="config",
        is_text=True,
        help="configuration for flow",
        default="moz_forecasting/ad_tiles_forecast/config.yaml",
    )

    test_mode = Parameter(
        name="test_mode",
        help="indicates whether or not run should affect production",
        default=True,
    )

    @step
    def start(self):
        """
        Each flow has a 'start' step.

        You can use it for collecting/preprocessing data or other setup tasks.
        """
        # load config
        self.config_data = yaml.safe_load(self.config)

        self.first_day_of_current_month = datetime.today().replace(day=1)
        last_day_of_previous_month = self.first_day_of_current_month - timedelta(days=1)
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
        self.event_aggregates_table = (
            "moz-fx-data-shared-prod.contextual_services.event_aggregates"
        )
        self.newtab_aggregates_table = (
            "mozdata.telemetry.newtab_clients_daily_aggregates"
        )

        self.next(self.get_tile_impression_data)

    @step
    def get_tile_impression_data(self):
        """Retrieve tile impressions."""
        query_start_date = self.observed_start_date.strftime("%Y-%m-%d")
        tile_impression_data_query = f""" SELECT
                                country,
                                submission_date,
                                form_factor,
                                release_channel,
                                SUM(IF(position <= 2, event_count, 0))
                                    AS sponsored_impressions_1and2,
                                SUM(event_count) AS sponsored_impressions_all
                            FROM
                                `{self.event_aggregates_table}`
                            WHERE
                                event_type = 'impression'
                                AND source = 'topsites'
                                AND (
                                    submission_date >= DATE_TRUNC(PARSE_DATE('%Y-%m-%d',
                                                                    '{query_start_date}'),
                                                                     MONTH)
                                    AND submission_date <= DATE_TRUNC(CURRENT_DATE(),
                                                                        MONTH)
                                )
                            GROUP BY
                                country,
                                submission_date,
                                form_factor,
                                release_channel"""
        client = bigquery.Client(project=GCS_PROJECT_NAME)
        self.inventory_raw = client.query(tile_impression_data_query).to_dataframe()
        self.next(self.get_newtab_visits)

    @step
    def get_newtab_visits(self):
        """Get newtab visits by country."""
        query_start_date = self.observed_start_date.strftime("%Y-%m-%d")
        countries = self.config_data["RPM"].keys()
        countries_string = ",".join(f"'{el}'" for el in countries)
        query = f""" SELECT
                    DATE_TRUNC(submission_date, MONTH) AS submission_month,
                    country_code as country,
                    SUM(newtab_visit_count) AS newtab_visits,
                FROM
                    `{self.newtab_aggregates_table}`
                WHERE
                    topsites_enabled
                    AND topsites_sponsored_enabled
                    AND (
                                    submission_date >= DATE_TRUNC(PARSE_DATE('%Y-%m-%d',
                                                                    '{query_start_date}'),
                                                                     MONTH)
                                    AND submission_date <= DATE_TRUNC(CURRENT_DATE(),
                                                                        MONTH)
                                )
                    AND country_code IN ({countries_string})
                GROUP BY
                    submission_month,
                    country_code"""
        client = bigquery.Client(project=GCS_PROJECT_NAME)
        newtab_visits = client.query(query).to_dataframe()
        newtab_visits["submission_month"] = pd.to_datetime(
            newtab_visits["submission_month"]
        )
        newtab_visits["total_inventory_1and2"] = newtab_visits["newtab_visits"] * 2
        newtab_visits["total_inventory_1to3"] = newtab_visits["newtab_visits"] * 3
        self.newtab_vists = newtab_visits
        self.next(self.desktop_tile_impression_cleaning)

    @step
    def desktop_tile_impression_cleaning(self):
        """Clean tile impression data and join to newtab visits.

        Creates fill_rate and sponsored_impressions columns
        """
        countries = self.config_data["RPM"].keys()
        inventory_raw = self.inventory_raw[
            (self.inventory_raw["form_factor"] == "desktop")
            & (self.inventory_raw["country"].isin(countries))
        ]
        inventory_raw["submission_month"] = vectorized_date_to_month(
            pd.to_datetime(inventory_raw["submission_date"])
        )
        inventory_agg = (
            inventory_raw[
                [
                    "submission_month",
                    "country",
                    "sponsored_impressions_1and2",
                    "sponsored_impressions_all",
                ]
            ]
            .groupby(["submission_month", "country"])
            .sum()
            .reset_index()
        )

        # join on newtab vists and calculate fill rates
        inventory = inventory_agg.merge(
            self.newtab_vists, on=["submission_month", "country"], how="inner"
        )
        inventory["fill_rate"] = (
            inventory.sponsored_impressions_1and2 / inventory.total_inventory_1and2
        )
        inventory["visits_total_fill_rate_1to3"] = (
            inventory.sponsored_impressions_all / inventory.total_inventory_1to3
        )

        self.inventory = inventory

        self.next(self.get_kpi_forecast)

    @step
    def get_kpi_forecast(self):
        """Get KPI forecast data."""
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
                GROUP BY aggregation_period,
                    metric_alias,
                    metric_hub_app_name,
                    metric_hub_slug
            ),
            tmp_kpi_forecasts as (
            SELECT forecasts.* EXCEPT(forecast_parameters)
                FROM `{self.kpi_forecast_table}` AS forecasts
                JOIN most_recent_forecasts
            USING(aggregation_period,
                    metric_alias,
                    metric_hub_app_name,
                    metric_hub_slug,
                    forecast_predicted_at)
            )
        SELECT (tmp_kpi_forecasts.submission_date ) AS submission_month,
            AVG(tmp_kpi_forecasts.value ) AS cdau
        FROM tmp_kpi_forecasts
        WHERE (
            ((tmp_kpi_forecasts.measure = 'observed')
                AND (tmp_kpi_forecasts.submission_date >= DATE('{observed_start_date}'))
                AND (tmp_kpi_forecasts.submission_date < DATE('{observed_end_date}')))
            OR
            ((tmp_kpi_forecasts.measure = 'p50')
                AND (tmp_kpi_forecasts.submission_date >= DATE('{forecast_date_start}'))
                AND (tmp_kpi_forecasts.submission_date <= DATE('{forecast_date_end}')))
            )
        AND tmp_kpi_forecasts.aggregation_period = 'month'
        AND tmp_kpi_forecasts.metric_alias LIKE 'desktop_dau'
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
        """Get dau by country."""
        # get markets from RPM
        countries = self.config_data["RPM"].keys()
        countries_string = ",".join(f"'{el}'" for el in countries)
        query = f"""
        SELECT
        (FORMAT_DATE('%Y-%m', submission_date )) AS submission_month,
        IF(country IN ({countries_string}), country, "Other") AS country,
        COALESCE(SUM((dau) ), 0) AS dau_by_country
        FROM
        `{self.active_users_aggregates_table}` AS active_users_aggregates
        WHERE
        (app_name ) = 'Firefox Desktop'
        AND ((submission_date >=
                DATE_ADD(DATE_TRUNC(CURRENT_DATE('UTC'), MONTH), INTERVAL -12 MONTH)
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
        """Get observed share_by_market.

        Join observed values for dau and dau_by_country
        to get observed share by market.
        """
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

        dau_live_markets = self.dau_by_country[self.dau_by_country.country != "Other"]

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
        """Create share_by_market and inv_per_client factors.

        share_by_market and inv_per_client are indexed by country.
        They are obtained by taking the mean of the observed share_by_market
        and inventory_per_country over time by country

        """
        # Merge country level KPI forecast with inventory data

        inventory_observed_data_filter = (
            self.inventory.submission_month >= self.observed_start_date
        ) & (self.inventory.submission_month <= self.observed_end_date)
        inventory_observed = self.inventory.loc[
            inventory_observed_data_filter,
            ["submission_month", "country", "total_inventory_1and2"],
        ]

        hist_dau_inv = pd.merge(
            self.hist_dau,
            inventory_observed,
            how="inner",
            on=["country", "submission_month"],
        )
        hist_dau_inv["inv_per_client"] = (
            hist_dau_inv["total_inventory_1and2"] / hist_dau_inv["dau_by_country"]
        )

        hist_avg = (
            hist_dau_inv.groupby("country")
            .mean()[["share_by_market", "inv_per_client"]]
            .reset_index()
        )
        self.hist_avg = hist_avg
        self.next(self.calculate_forecasted_inventory_by_country)

    @step
    def calculate_forecasted_inventory_by_country(self):
        """Calculate inventory_forecast.

        This is obtained by merging country averages onto forecast cdau
        and multiplying on the share_by_market and inv_per_client
        country-level factors
        """
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
                "country",
                "cdau",
                "share_by_market",
                "inv_per_client",
            ]
        ]

        inventory_forecast["country_dau"] = (
            inventory_forecast["cdau"] * inventory_forecast["share_by_market"]
        )
        inventory_forecast["inventory_forecast"] = (
            inventory_forecast["country_dau"] * inventory_forecast["inv_per_client"]
        )
        self.inventory_forecast = inventory_forecast
        self.next(self.add_impression_forecast)

    @step
    def add_impression_forecast(self):
        """Add expected_impressions.

        This is obtained by multipolying the inventory forecast
        by the fill rate
        """
        six_months_before_obs_end = self.observed_end_date - relativedelta(months=6)
        observed_fill_rate_by_country = self.inventory.loc[
            (self.inventory.submission_month <= self.observed_end_date)
            & (self.inventory.submission_month >= six_months_before_obs_end),
            ["country", "fill_rate"],
        ]

        average_fill_rate_by_country = (
            observed_fill_rate_by_country.groupby("country").mean().reset_index()
        )

        self.revenue_forecast = pd.merge(
            self.inventory_forecast, average_fill_rate_by_country, on="country"
        )
        self.revenue_forecast["expected_impressions"] = (
            self.revenue_forecast["inventory_forecast"]
            * self.revenue_forecast["fill_rate"]
        )
        self.next(self.account_for_direct_allocations)

    @step
    def account_for_direct_allocations(self):
        """Add columns related to direct sales.

        Following columns are added:
            direct_sales_markets: indicates whether country/month has direct sales
                "y" for yes and "n" for no
            direct_sales_allocations: The fraction of impressions allocated AMP
            expected_impressions_direct_sales: Number of impressions allocated to AMP
        """
        direct_allocation_df = get_direct_allocation_df(
            self.config_data["direct_allocations"],
            min_month=self.revenue_forecast["submission_month"].min(),
            max_month=self.revenue_forecast["submission_month"].max(),
        )
        self.revenue_forecast = self.revenue_forecast.merge(
            direct_allocation_df,
            on=["submission_month", "country"],
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
            self.revenue_forecast["expected_impressions"]
            * self.revenue_forecast["direct_sales_allocations"]
        )

        self.next(self.forecast_revenue)

    @step
    def forecast_revenue(self):
        """Add revenue_ds and renvenue_no_ds columns.

        These correspond to the revenue forecast for AMP
        when direct sales are remove and the forecast
        assuming all impressions got to AMP, respectively
        """
        RPMs = self.config_data["RPM"]

        RPM_df = pd.DataFrame(
            [{"country": key, "RPM": val} for key, val in RPMs.items()]
        )

        revenue_forecast = pd.merge(self.revenue_forecast, RPM_df, on="country")

        # Desktop RPMs were increased by 10% in summer of 2024
        # with an effective date of 2024-09-30
        # as part of the AMP contract renewal conversations.
        after_valid_date = revenue_forecast["submission_month"] <= "2024-09-01"
        revenue_forecast.loc[after_valid_date, "RPM"] = (
            revenue_forecast.loc[after_valid_date, "RPM"] * 1 / 1.1
        )

        # multiply inventory by RPMs
        revenue_forecast["no_direct_sales"] = (
            pd.to_numeric(revenue_forecast["expected_impressions"])
            * revenue_forecast["RPM"]
            / 1000
        )
        revenue_forecast["with_direct_sales"] = (
            pd.to_numeric(revenue_forecast["expected_impressions_direct_sales"])
            * revenue_forecast["RPM"]
            / 1000
        )

        revenue_forecast.groupby(["submission_month"]).sum()[
            ["no_direct_sales", "with_direct_sales"]
        ]

        self.output_df = revenue_forecast

        self.next(self.test)

    @step
    def test(self):
        from metaflow import Step, Run, Flow, Metaflow, namespace

        runs_on_main = [
            el for el in Flow("AdTilesForecastFlow").runs("main") if el.successful
        ]
        runs_on_main = sorted(runs_on_main, key=lambda x: x.finished_at)
        main_run = runs_on_main[-1]
        main_output_df = main_run["end"].task.data.output_df
        pd.testing.assert_frame_equal(main_output_df, self.output_df)
        self.next(self.end)

    @step
    def end(self):
        """Write to BQ."""
        write_df = pd.melt(
            self.output_df,
            value_vars=["no_direct_sales", "with_direct_sales"],
            id_vars=[
                "submission_month",
                "inventory_forecast",
                "expected_impressions",
                "country",
            ],
            value_name="revenue",
            var_name="forecast_type",
        )

        write_df["device"] = "desktop"
        write_df["forecast_month"] = self.first_day_of_current_month.strftime(
            "%Y-%m-%d"
        )

        assert set(write_df.columns) == {
            "forecast_month",
            "country",
            "submission_month",
            "inventory_forecast",
            "expected_impressions",
            "revenue",
            "device",
            "forecast_type",
        }
        if self.test_mode:
            output_info = self.config_data["output"]["test"]
        else:
            output_info = self.config_data["output"]["prod"]
        target_table = f"{output_info['output_database']}.{output_info['output_table']}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        client = bigquery.Client(project=GCS_PROJECT_NAME)

        # client.load_table_from_dataframe(write_df, target_table, job_config=job_config)


if __name__ == "__main__":
    AdTilesForecastFlow()
