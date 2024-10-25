"""Flow for the Ad Tiles Forecast."""

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yaml
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
from metaflow import FlowSpec, IncludeFile, Parameter, project, step

GCP_PROJECT_NAME = os.environ.get("GCP_PROJECT_NAME", "moz-fx-mfouterbounds-prod-f98d")


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

    write = Parameter(name="write", help="whether or not to write to BQ", default=False)

    set_forecast_month = Parameter(
        name="forecast_month",
        help="indicate historical month to set forecast date to in %Y-%m format",
        default=None,
    )

    @step
    def start(self):
        """
        Each flow has a 'start' step.

        You can use it for collecting/preprocessing data or other setup tasks.
        """
        # load config
        self.config_data = yaml.safe_load(self.config)

        if not self.set_forecast_month:
            self.first_day_of_current_month = datetime.today().replace(day=1)
        else:
            self.first_day_of_current_month = datetime.strptime(
                self.set_forecast_month + "-01", "%Y-%m-%d"
            )
        last_day_of_previous_month = self.first_day_of_current_month - timedelta(days=1)
        first_day_of_previous_month = last_day_of_previous_month.replace(day=1)
        self.observed_start_date = first_day_of_previous_month - relativedelta(years=1)
        self.observed_end_date = last_day_of_previous_month

        # tables to get data from
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

        self.next(self.get_dau_forecast_by_country)

    @step
    def get_dau_forecast_by_country(self):
        """Get by-country dau forecast.

        The ultimate outcome is creating columns of the form
        dau_forecast_<product> where product is specified as a key under
        the `elgibility` field in the config.  For each product, elgibility
        rules can be specified for desktop and mobile.

        The dau forecast is created by multiplying the global dau
        forecast by two factors:  a country-level  dau fraction and a
        country-level elgbility fraction.  The former is the fraction of dau
        for a given country over total dau from the events_users_aggregates table.
        The latter is the fraction of eligible dau over total dau within a country,
        and captures country-level rules and variation in elgilibilty for the product
        (IE tiles was released on different dates in different countries).
        These rules are captured in the elgilbility function in the config.

        The active users aggregates query is saved as dau_by_country
        so it can be used in other steps.

        Both factors are calculated and the quotients are averaged over
        the timeframe of the observed data

        """
        forecast_date_end_dt = self.observed_end_date.replace(day=1) + relativedelta(
            months=18
        )
        forecast_date_end = forecast_date_end_dt.strftime("%Y-%m-%d")

        observed_start_date = self.observed_start_date.strftime("%Y-%m-%d")
        observed_end_date = self.observed_end_date.strftime("%Y-%m-%d")

        # first get the global KPI forecast
        # there can be multiple forecasts for a given date
        # joining to most_recent_forecasts selects only the most recent
        # groupby in last step is because for monthly the current
        # month will have two records, one for prediction and one forecast
        # ANY_VALUE selects the first non-null value, so it will
        # merge this case
        query = f"""
            WITH most_recent_forecasts AS (
                SELECT aggregation_period,
                    metric_alias,
                    metric_hub_app_name,
                    metric_hub_slug,
                    MAX(forecast_predicted_at) AS forecast_predicted_at
                FROM `{self.kpi_forecast_table}`
                WHERE forecast_predicted_at <= '{observed_end_date}'
                GROUP BY aggregation_period,
                    metric_alias,
                    metric_hub_app_name,
                    metric_hub_slug
            ),
            only_most_recent_kpi_forecasts as (
            SELECT *
                FROM `{self.kpi_forecast_table}` AS forecasts
                JOIN most_recent_forecasts
            USING(aggregation_period,
                    metric_alias,
                    metric_hub_app_name,
                    metric_hub_slug,
                    forecast_predicted_at)
            ),
            pivoted_table as (SELECT * FROM only_most_recent_kpi_forecasts
                                    PIVOT (SUM(value)
                                    FOR measure
                                        IN ('observed','p10', 'p90', 'mean', 'p50')))
        SELECT submission_date as submission_month,
            forecast_predicted_at,
            REPLACE(CAST(metric_alias AS STRING), "_dau", "") as platform,
            ANY_VALUE(observed) as observed_dau,
            ANY_VALUE(p10) as p10_forecast,
            ANY_VALUE(p90) as p90_forecast,
            ANY_VALUE(mean) as mean_forecast,
            ANY_VALUE(p50) as median_forecast
        FROM pivoted_table
        WHERE (submission_date >= DATE('{observed_start_date}'))
            AND (submission_date <= DATE('{forecast_date_end}'))
            AND aggregation_period = 'month'
        GROUP BY 1,2,3
        """

        client = bigquery.Client(project=GCP_PROJECT_NAME)
        query_job = client.query(query)

        global_dau_forecast = query_job.to_dataframe()

        # get forecast_predicted_at
        # joined on before writing so the exact kpi forecast
        # used is known
        forecast_predicted_at = global_dau_forecast[
            ["platform", "forecast_predicted_at"]
        ].drop_duplicates()
        if len(forecast_predicted_at) != 2:
            raise ValueError(
                f"Unexpected forecast_predicted_at dates:\n{forecast_predicted_at}"
            )
        self.forecast_predicted_at = forecast_predicted_at.rename(
            columns={"platform": "device"}
        )

        # get dau by country from events_users_aggregates

        # extract elgibility functions from config
        # and turn them into a string that can be used in query
        # mobile and desktop each get their own columns
        # when counting eligible daily users for each product
        eligibility_functions = []
        eligibility_function_calls = []
        # iterate over products in the config
        for forecast, elgibility_data in self.config_data["elgibility"].items():
            # currently only support partitioning by platform
            for platform in ["mobile", "desktop"]:
                partition_data = elgibility_data[platform]
                eligibility_functions.append(partition_data["bq_function"])

                eligibility_function_calls.append(
                    (
                        partition_data["function_call"],
                        f"eligible_{platform}_{forecast}_clients",
                    )
                )
        call_string = [
            f"SUM(IF({x[0]}, daily_users, 0)) as {x[1]},"
            for x in eligibility_function_calls
        ]
        elgibility_string = "\n".join(eligibility_functions)
        call_string = "\n".join(call_string)
        query = f"""
                {elgibility_string}

                SELECT
                        (FORMAT_DATE('%Y-%m', submission_date )) AS submission_month,
                        country,
                        IF(app_name = 'Firefox Desktop', 'desktop', 'mobile')
                            as platform,
                        COALESCE(SUM((dau)), 0) AS total_active,
                        COALESCE(SUM((daily_users) ), 0) AS total_clients,
                        {call_string}
                        FROM `{self.active_users_aggregates_table}`
                        WHERE
                        submission_date >= "{observed_start_date}"
                        AND app_name in ("Fenix", "Firefox iOS", "Firefox Desktop")
                        GROUP BY
                        1,2,3"""

        client = bigquery.Client(project=GCP_PROJECT_NAME)
        query_job = client.query(query)

        self.dau_by_country = query_job.to_dataframe()

        global_dau_forecast["submission_month"] = pd.to_datetime(
            global_dau_forecast["submission_month"]
        )
        self.dau_by_country["submission_month"] = pd.to_datetime(
            self.dau_by_country["submission_month"]
        )

        # join dau by country onto observed forecast data
        global_dau_forecast_observed = global_dau_forecast.loc[
            (global_dau_forecast.submission_month >= self.observed_start_date)
            & (global_dau_forecast.submission_month <= self.observed_end_date),
            ["submission_month", "observed_dau", "platform"],
        ]

        global_dau_forecast_observed = pd.merge(
            global_dau_forecast_observed,
            self.dau_by_country,
            how="left",
            on=["submission_month", "platform"],
        )

        # for each product, add a column with a count of eligible
        # daily users for that product
        new_columns = []
        for forecast in self.config_data["elgibility"]:
            output_column_name = f"elgibility_fraction_{forecast}"
            # create the column and fill in values for mobile and desktop separately
            global_dau_forecast_observed[output_column_name] = np.nan
            new_columns.append(output_column_name)
            for platform in ["desktop", "mobile"]:
                input_column_name = f"eligible_{platform}_{forecast}_clients"

                partition_filter = global_dau_forecast_observed["platform"] == platform
                global_dau_forecast_observed.loc[
                    partition_filter, output_column_name
                ] = (
                    global_dau_forecast_observed.loc[
                        partition_filter, input_column_name
                    ]
                    / global_dau_forecast_observed.loc[
                        partition_filter, "total_clients"
                    ]
                )

        # add dau by country factor
        # calculate by taking total dau by month from active_users_aggregates
        # and dividing country-level dau with it
        # assumpting here that effect of single user in multiple countries
        # is negligible
        dau_by_country_rollup = (
            self.dau_by_country[["total_active", "submission_month", "platform"]]
            .groupby(["submission_month", "platform"])
            .sum()
            .reset_index()
        )
        dau_by_country_rollup = dau_by_country_rollup.rename(
            columns={"total_active": "total_dau"}
        )

        global_dau_forecast_observed = global_dau_forecast_observed.merge(
            dau_by_country_rollup, on=["submission_month", "platform"], how="left"
        )

        global_dau_forecast_observed["share_by_market"] = (
            global_dau_forecast_observed["total_active"]
            / global_dau_forecast_observed["total_dau"]
        )

        self.global_dau_forecast_observed = global_dau_forecast_observed

        # average over the observation period to get
        # country-level factors
        self.dau_factors = (
            global_dau_forecast_observed[
                ["country", "platform", "share_by_market"] + new_columns
            ]
            .groupby(["country", "platform"])
            .mean()
            .reset_index()
        )

        # get forecasted values
        global_dau_forecast_future = global_dau_forecast.loc[
            global_dau_forecast.submission_month > self.observed_end_date,
            [
                "submission_month",
                "median_forecast",
                "mean_forecast",
                "p10_forecast",
                "p90_forecast",
                "platform",
            ],
        ]
        dau_forecast_by_country = pd.merge(
            global_dau_forecast_future, self.dau_factors, how="inner", on=["platform"]
        )[
            [
                "submission_month",
                "country",
                "median_forecast",
                "mean_forecast",
                "p10_forecast",
                "p90_forecast",
                "share_by_market",
                "platform",
            ]
            + new_columns
        ]

        # calculate by-country forecast
        for column in new_columns:
            forecast_column_name = column.replace("elgibility_fraction", "dau_forecast")
            dau_forecast_by_country[forecast_column_name] = (
                dau_forecast_by_country[column]  # elgilibity factor
                * dau_forecast_by_country["share_by_market"]
                * dau_forecast_by_country["median_forecast"]
            )

            # add 90th and 10th percentiles
            dau_forecast_by_country[forecast_column_name + "_p90"] = (
                dau_forecast_by_country[column]  # elgilibity factor
                * dau_forecast_by_country["share_by_market"]
                * dau_forecast_by_country["p90_forecast"]
            )

            dau_forecast_by_country[forecast_column_name + "_p10"] = (
                dau_forecast_by_country[column]  # elgilibity factor
                * dau_forecast_by_country["share_by_market"]
                * dau_forecast_by_country["p10_forecast"]
            )
        self.dau_forecast_by_country = dau_forecast_by_country
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
        client = bigquery.Client(project=GCP_PROJECT_NAME)
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
        client = bigquery.Client(project=GCP_PROJECT_NAME)
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

        self.next(self.calculate_inventory_per_client)

    @step
    def calculate_inventory_per_client(self):
        """Create inv_per_client factors.

        inv_per_client is indexed by country.
        It is obtained by taking the mean of the observed share_by_market
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

        dau_by_country = self.dau_by_country.loc[
            self.dau_by_country.platform == "desktop",
            ["total_active", "country", "submission_month"],
        ].copy()

        observed_data = pd.merge(
            dau_by_country,
            inventory_observed,
            how="inner",
            on=["country", "submission_month"],
        )
        observed_data["inv_per_client"] = (
            observed_data["total_inventory_1and2"] / observed_data["total_active"]
        )

        inventory_per_client = (
            observed_data.groupby("country").mean()[["inv_per_client"]].reset_index()
        )
        self.inventory_per_client = inventory_per_client
        self.next(self.calculate_forecasted_inventory_by_country)

    @step
    def calculate_forecasted_inventory_by_country(self):
        """Calculate inventory_forecast.

        This is obtained by merging country averages onto forecast cdau
        and multiplying on the share_by_market and inv_per_client
        country-level factors
        """
        # subset to desktop and relevant countries
        # get markets from RPM
        countries = self.config_data["RPM"].keys()

        dau_forecast = self.dau_forecast_by_country.loc[
            (self.dau_forecast_by_country["platform"] == "desktop")
            & (self.dau_forecast_by_country.country.isin(countries)),
            ["submission_month", "country", "dau_forecast_tiles", "share_by_market"],
        ]
        inventory_forecast = dau_forecast.merge(self.inventory_per_client, on="country")

        inventory_forecast["inventory_forecast"] = (
            inventory_forecast["dau_forecast_tiles"]
            * inventory_forecast["inv_per_client"]
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
        """Test."""
        from metaflow import Flow

        runs_on_main = [
            el for el in Flow("AdTilesForecastFlow").runs("main") if el.successful
        ]
        runs_on_main = sorted(runs_on_main, key=lambda x: x.finished_at)
        main_run = runs_on_main[-1]
        main_output_df = (
            main_run["end"]
            .task.data.output_df.sort_values(["submission_month", "country"])
            .reset_index(drop=True)
        ).drop(columns=["cdau"])
        branch_output = (
            self.output_df.rename(columns={"dau_forecast_tiles": "country_dau"})
            .sort_values(["submission_month", "country"])
            .reset_index(drop=True)
        )

        main_columns = set(main_output_df.columns)
        branch_columns = set(branch_output.columns)

        assert main_columns == branch_columns
        pd.testing.assert_frame_equal(
            main_output_df[list(main_columns)],
            branch_output[list(main_columns)],
            check_exact=False,
            rtol=0.1,
        )
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
        write_df["forecast_month"] = self.first_day_of_current_month
        write_df = write_df.merge(self.forecast_predicted_at, how="inner", on="device")

        assert set(write_df.columns) == {
            "forecast_month",
            "forecast_predicted_at",
            "country",
            "submission_month",
            "inventory_forecast",
            "expected_impressions",
            "revenue",
            "device",
            "forecast_type",
        }
        if not self.write:
            return

        if self.test_mode and GCP_PROJECT_NAME != "moz-fx-mfouterbounds-prod-f98d":
            # case where testing locally
            output_info = self.config_data["output"]["test"]
        elif self.test_mode and GCP_PROJECT_NAME == "moz-fx-mfouterbounds-prod-f98d":
            # case where testing in outerbounds, just want to exit
            return
        else:
            output_info = self.config_data["output"]["prod"]
        target_table = (
            f"{output_info['project']}.{output_info['database']}.{output_info['table']}"
        )
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job_config.schema_update_options = [
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]

        client = bigquery.Client(project=GCP_PROJECT_NAME)

        client.load_table_from_dataframe(write_df, target_table, job_config=job_config)


if __name__ == "__main__":
    AdTilesForecastFlow()
