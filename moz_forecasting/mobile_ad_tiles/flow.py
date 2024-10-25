"""Flow for the Mobile Ad Tiles Forecast."""
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

# Defaults to the project for Outerbounds Deployment
# To run locally, set to moz-fx-data-bq-data-science on command line before run command
GCP_PROJECT_NAME = os.environ.get("GCP_PROJECT_NAME", "moz-fx-mfouterbounds-prod-f98d")


@project(name="mobile_ad_tiles_forecast")
class MobileAdTilesForecastFlow(FlowSpec):
    """Flow for ads tiles forecasting."""

    config = IncludeFile(
        name="config",
        is_text=True,
        help="configuration for flow",
        default="moz_forecasting/mobile_ad_tiles/config.yaml",
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
        self.countries = self.config_data["countries"]
        self.excluded_advertisers = self.config_data["excluded_advertisers"]

        if not self.set_forecast_month:
            self.first_day_of_current_month = datetime.today().replace(day=1)
        else:
            self.first_day_of_current_month = datetime.strptime(
                self.set_forecast_month + "-01", "%Y-%m-%d"
            )
        last_day_of_previous_month = self.first_day_of_current_month - timedelta(days=1)
        first_day_of_previous_month = last_day_of_previous_month.replace(day=1)
        self.first_day_of_previous_month = first_day_of_previous_month
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
        self.cpc_table = "mozdata.revenue.revenue_data_admarketplace_cpc"

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
        self.next(self.get_tile_data)

    @step
    def get_tile_data(self):
        """Get tile interaction data.

        This step produces information about both clicks and
        impressions for tiles.  The following columns are created
        p_amazon: the fraction of users where the advertiser is amazon
        p_other: the fraction of users where the advertiser is not amazon (or excluded)
        amazon_clicks: number of clicks on amazon tiles
        other_clicks: number of clicks on non-amazon tiles
        """
        forecast_start = self.first_day_of_previous_month.strftime("%Y-%m-%d")
        countries_string = ",".join(f"'{el}'" for el in self.countries)
        excluded_advertisers_string = ",".join(
            f"'{el}'" for el in self.excluded_advertisers
        )
        query = f"""SELECT
                        (FORMAT_DATE('%Y-%m', submission_date )) AS submission_month,
                        country,
                        event_type,
                        COALESCE(SUM(IF(advertiser = "amazon",
                                            user_count,
                                            0))/SUM(user_count)) AS p_amazon,
                        COALESCE(
                            SUM(IF(advertiser NOT IN ("amazon",
                                                        {excluded_advertisers_string}),
                                            user_count,
                                            0))/SUM(user_count)) AS p_other,
                        COALESCE(SUM(
                            IF(advertiser = "amazon",
                                event_count,
                                0)),
                            0) AS amazon_interaction_count,
                        COALESCE(SUM(
                            IF(advertiser NOT IN ("amazon",
                                                    {excluded_advertisers_string}),
                                event_count,
                                0)),
                            0) AS other_interaction_count
                    FROM
                        {self.event_aggregates_table}
                    WHERE
                        submission_date >= "{forecast_start}"
                        AND form_factor = "phone"
                        AND event_type in ("impression", "click")
                        AND source = "topsites"
                        AND country IN ({countries_string})
                    GROUP BY
                        submission_month,
                        country,
                        event_type"""
        client = bigquery.Client(project=GCP_PROJECT_NAME)
        query_job = client.query(query)
        tile_data = query_job.to_dataframe()
        clicks = tile_data.loc[
            tile_data["event_type"] == "click",
            [
                "submission_month",
                "country",
                "amazon_interaction_count",
                "other_interaction_count",
            ],
        ].rename(
            columns={
                "amazon_interaction_count": "amazon_clicks",
                "other_interaction_count": "other_clicks",
            }
        )
        inventory = tile_data.loc[
            tile_data["event_type"] == "impression",
            ["submission_month", "country", "p_amazon", "p_other"],
        ]

        self.events_data = clicks.merge(inventory, on=["submission_month", "country"])

        self.events_data["submission_month"] = pd.to_datetime(
            self.events_data["submission_month"]
        )

        self.next(self.aggregate_data)

    @step
    def aggregate_data(self):
        """Aggregate events and dau data and create new fields.

        Creates the following new columns:
        - amazon_clicks_per_qdau
        - other_clicks_per_qdau
        """
        dau_by_country = self.dau_by_country[
            self.dau_by_country.platform == "mobile"
        ].drop(columns="platform")
        aggregate_data = self.events_data.merge(
            dau_by_country, on=["submission_month", "country"]
        )

        previous_month = self.first_day_of_previous_month.strftime(format="%Y-%m")

        # filter to previous month
        aggregate_data = aggregate_data[
            aggregate_data.submission_month == previous_month
        ].drop(columns="submission_month")

        aggregate_data["amazon_clients"] = (
            aggregate_data["p_amazon"] * aggregate_data["eligible_mobile_tiles_clients"]
        )
        aggregate_data["other_clients"] = (
            aggregate_data["p_other"] * aggregate_data["eligible_mobile_tiles_clients"]
        )

        aggregate_data["amazon_clicks_per_qdau"] = (
            aggregate_data["amazon_clicks"] / aggregate_data["amazon_clients"]
        )
        aggregate_data["other_clicks_per_qdau"] = (
            aggregate_data["other_clicks"] / aggregate_data["other_clients"]
        )

        # in notebook this is mobile_forecasting_data
        self.usage_by_country = aggregate_data[
            [
                "country",
                "p_amazon",
                "p_other",
                "amazon_clicks_per_qdau",
                "other_clicks_per_qdau",
            ]
        ]

        self.next(self.get_cpcs)

    @step
    def get_cpcs(self):
        """Calculate the cpc by country.

        Creates the following country-level columns
        - amazon_cpc
        - other_cpc
        """
        date_start = self.first_day_of_previous_month.strftime("%Y-%m-%d")
        countries_string = ",".join(f"'{el}'" for el in self.countries)
        query = f"""
        with group_ads AS (
            SELECT
            revenue_data_admarketplace.country_code  AS country,
            advertiser,
            SAFE_DIVIDE(COALESCE(SUM(revenue_data_admarketplace.payout ), 0),
                COALESCE(SUM(revenue_data_admarketplace.valid_clicks ), 0)) AS cpc
            FROM `{self.cpc_table}` AS revenue_data_admarketplace
            WHERE
            (revenue_data_admarketplace.adm_date ) >= (DATE('{date_start}'))
            AND (revenue_data_admarketplace.country_code ) IN ({countries_string})
            AND (revenue_data_admarketplace.product ) = 'mobile tile'
            GROUP BY 1, 2
        ), sep_CPC AS (
            SELECT
            country,
            CASE WHEN advertiser = 'amazon' THEN cpc ELSE 0 END AS amazon_cpc,
            CASE WHEN advertiser != 'amazon' THEN cpc ELSE 0 END AS other_cpc,
            FROM group_ads
        )
        SELECT country,
            MAX(amazon_cpc) as amazon_cpc,
            MAX(other_cpc) as other_cpc
        FROM sep_CPC
        GROUP BY 1
        """

        client = bigquery.Client(project=GCP_PROJECT_NAME)
        query_job = client.query(query)

        self.mobile_cpc = query_job.to_dataframe()

        self.next(self.combine_bq_tables)

    @step
    def combine_bq_tables(self):
        """Combine all data and calculate metrics."""
        forecast_start_date = self.first_day_of_current_month.strftime("%Y-%m-%d")
        country_level_metrics = pd.merge(
            self.usage_by_country, self.mobile_cpc, how="left", on="country"
        )

        dau_forecast_by_country = self.dau_forecast_by_country[
            (self.dau_forecast_by_country["platform"] == "mobile")
            & (
                self.dau_forecast_by_country.submission_month
                >= pd.to_datetime(forecast_start_date)
            )
        ]
        rev_forecast_dat = pd.merge(
            country_level_metrics, dau_forecast_by_country, how="inner", on="country"
        )

        rev_forecast_dat["est_value_amazon_qdau"] = (
            rev_forecast_dat["dau_forecast_tiles"] * rev_forecast_dat["p_amazon"]
        )
        rev_forecast_dat["10p_amazon_qdau"] = (
            rev_forecast_dat["dau_forecast_tiles_p10"] * rev_forecast_dat["p_amazon"]
        )
        rev_forecast_dat["90p_amazon_qdau"] = (
            rev_forecast_dat["dau_forecast_tiles_p90"] * rev_forecast_dat["p_amazon"]
        )

        rev_forecast_dat["amazon_clicks"] = (
            rev_forecast_dat["est_value_amazon_qdau"]
            * rev_forecast_dat["amazon_clicks_per_qdau"]
        )
        # amazon cpc
        rev_forecast_dat["amazon_revenue"] = (
            rev_forecast_dat["amazon_clicks"] * rev_forecast_dat["amazon_cpc"]
        )

        rev_forecast_dat["est_value_other_qdau"] = (
            rev_forecast_dat["dau_forecast_tiles"] * rev_forecast_dat["p_other"]
        )
        rev_forecast_dat["10p_other_qdau"] = (
            rev_forecast_dat["dau_forecast_tiles_p10"] * rev_forecast_dat["p_other"]
        )
        rev_forecast_dat["90p_other_qdau"] = (
            rev_forecast_dat["dau_forecast_tiles_p90"] * rev_forecast_dat["p_other"]
        )
        rev_forecast_dat["other_clicks"] = (
            rev_forecast_dat["est_value_other_qdau"]
            * rev_forecast_dat["other_clicks_per_qdau"]
        )
        # other cpc
        rev_forecast_dat["other_revenue"] = (
            rev_forecast_dat["other_clicks"] * rev_forecast_dat["other_cpc"]
        )

        rev_forecast_dat["total_clicks"] = (
            rev_forecast_dat["other_clicks"] + rev_forecast_dat["amazon_clicks"]
        )
        rev_forecast_dat["total_revenue"] = (
            rev_forecast_dat["other_revenue"] + rev_forecast_dat["amazon_revenue"]
        )
        rev_forecast_dat["device"] = "mobile"

        prefix = "automated_kpi_confidence_intervals_estimated"
        rev_forecast_dat = rev_forecast_dat.rename(
            columns={
                "p90_forecast": f"{prefix}_90th_percentile",
                "p10_forecast": f"{prefix}_10th_percentile",
                "mean_forecast": f"{prefix}_value",
            }
        )

        self.rev_forecast_dat = rev_forecast_dat

        self.next(self.end)

    @step
    def end(self):
        """Write data."""
        output_columns = [
            "submission_month",
            "country",
            "est_value_amazon_qdau",
            "10p_amazon_qdau",
            "90p_amazon_qdau",
            "amazon_clicks_per_qdau",
            "amazon_clicks",
            "amazon_cpc",
            "amazon_revenue",
            "est_value_other_qdau",
            "10p_other_qdau",
            "90p_other_qdau",
            "other_clicks_per_qdau",
            "other_clicks",
            "other_cpc",
            "other_revenue",
            "total_clicks",
            "total_revenue",
        ]

        write_df = self.rev_forecast_dat[output_columns]

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
        client = bigquery.Client(project=GCP_PROJECT_NAME)

        client.load_table_from_dataframe(write_df, target_table, job_config=job_config)


if __name__ == "__main__":
    MobileAdTilesForecastFlow()
