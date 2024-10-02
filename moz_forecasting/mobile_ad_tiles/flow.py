"""Flow for the Mobile Ad Tiles Forecast."""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

from datetime import datetime, timedelta
import os

import pandas as pd
import yaml
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
from metaflow import FlowSpec, IncludeFile, project, step, Parameter

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

        self.first_day_of_current_month = datetime.today().replace(day=1)
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

        self.next(self.get_kpi_forecast)

    @step
    def get_kpi_forecast(self):
        """Get Mobile KPI Data.

        Creates the following columns, all of which are aggregate
        metrics of the mobile dau forecast:
        automated_kpi_confidence_intervals_estimated_value: mean value
        automated_kpi_confidence_intervals_estimated_10th_percentile: 10th percentile
        automated_kpi_confidence_intervals_estimated_90th_percentile: 90th percentile
        """
        query = f"""
        WITH
            most_recent_forecasts AS (
                SELECT aggregation_period,
                    metric_alias,
                    metric_hub_app_name,
                    metric_hub_slug,
                    MAX(forecast_predicted_at) AS forecast_predicted_at
                FROM `{self.kpi_forecast_table}`
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
            renamed_indices as (SELECT forecast_end_date as asofdate,
                                        submission_date,
                                        metric_alias as target,
                                        aggregation_period as unit,
                                        DATE(forecast_predicted_at) as forecast_date,
                                        forecast_parameters, measure, value
                                FROM only_most_recent_kpi_forecasts),
            pivoted_table as (SELECT * FROM renamed_indices
                                    PIVOT (SUM(value)
                                    FOR measure in ('observed','p10', 'p90', 'mean')))
        SELECT
            CAST(submission_date as STRING)
                AS automated_kpi_confidence_intervals_submission_month,
            (SELECT MAX(a) FROM UNNEST([mean, observed]) a WHERE a is not NULL)
                AS automated_kpi_confidence_intervals_estimated_value,
            p10 AS automated_kpi_confidence_intervals_estimated_10th_percentile,
            p90 AS automated_kpi_confidence_intervals_estimated_90th_percentile
        FROM pivoted_table
        WHERE
            CAST(unit AS STRING) = 'month'
            AND REPLACE(CAST(target AS STRING), "_dau", "") = 'mobile'
        """

        client = bigquery.Client(project=GCP_PROJECT_NAME)
        query_job = client.query(query)

        mobile_kpi = query_job.to_dataframe()
        mobile_kpi = mobile_kpi[
            mobile_kpi.automated_kpi_confidence_intervals_submission_month
            >= self.first_day_of_current_month.strftime("%Y-%m-%d")
        ]
        final_forecast_month = mobile_kpi[
            "automated_kpi_confidence_intervals_submission_month"
        ].max()
        mobile_kpi = mobile_kpi[
            mobile_kpi["automated_kpi_confidence_intervals_submission_month"]
            < final_forecast_month
        ]

        self.mobile_kpi = mobile_kpi

        self.next(self.get_dau_by_country)

    @step
    def get_dau_by_country(self):
        """Get dau by country.

        This step creates two columns:
            - eligible_share_country: the quotient of eligible clients within a
                given country to the total number of clients across all countries
                for each day
            - eligible_clients: the number of eligible clients
                within a country on given day
                that are using Fenix or Firefox iOS on the release channel
        """
        forecast_start = self.first_day_of_current_month.strftime("%Y-%m-%d")
        query = f"""CREATE TEMP FUNCTION IsEligible(os STRING,
                                                    version NUMERIC,
                                                    country STRING,
                                                    submission_date DATE)
                    RETURNS BOOL
                    AS ((os = "Android"
                            AND  version > 100
                            AND (
                                (country = "US" AND submission_date >= "2022-09-20")
                                OR (country = "DE" AND submission_date >= "2022-12-05")
                                OR (country IN ("BR", "CA", "ES",
                                                    "FR", "GB", "IN", "AU")
                                        AND submission_date >= "2023-05-15")))
                        OR (os = "iOS"
                            AND version > 101
                            AND (
                                (country = "US"
                                    AND submission_date >= "2022-10-04")
                                OR (country = "DE"
                                    AND submission_date >= "2022-12-05")
                                OR (country IN ("BR", "CA", "ES",
                                                    "FR", "GB", "IN", "AU")
                                    AND submission_date >= "2023-05-15")
                            ))) ;
                SELECT
                        submission_date,
                        country,
                        app_name,
                        channel,
                        COALESCE(SUM((dau) ), 0) AS total_active,
                        COALESCE(SUM((daily_users) ), 0) AS total_clients,
                        SUM(IF(IsEligible(os_grouped,
                                            app_version_major,
                                            country,
                                            submission_date),
                                        daily_users,
                                        0)) as eligible_clients,
                        FROM `{self.active_users_aggregates_table}`
                            AS active_users_aggregates
                        WHERE
                        os_grouped in ("iOS", "Android")
                            AND submission_date >= "{forecast_start}"
                        GROUP BY
                        submission_date, country, app_name, channel"""
        client = bigquery.Client(project=GCP_PROJECT_NAME)
        query_job = client.query(query)
        dau_raw = query_job.to_dataframe()

        # make sure countries with eligible clients
        # is a subset of countries in the config
        eligible_by_country = (
            dau_raw[["country", "eligible_clients"]].groupby("country").sum()
        )
        countries_with_eligible = eligible_by_country[
            eligible_by_country["eligible_clients"] > 0
        ].index
        if not set(countries_with_eligible) <= set(self.countries):
            extra_countries = ",".join(
                list(set(self.countries) - set(countries_with_eligible))
            )
            raise ValueError(
                f"Eligible countries with no eligible clients: {extra_countries}"
            )

        clients_by_day = (
            dau_raw[["submission_date", "total_clients"]]
            .groupby("submission_date")
            .sum()
            .reset_index()
        )
        clients_by_day_by_country = (
            dau_raw[["submission_date", "country", "eligible_clients"]]
            .groupby(["submission_date", "country"])
            .sum()
            .reset_index()
        )
        client_share = clients_by_day.merge(
            clients_by_day_by_country, on="submission_date"
        )
        client_share["eligible_share_country"] = (
            client_share["eligible_clients"] / client_share["total_clients"]
        )

        population = (
            dau_raw.loc[
                dau_raw.app_name.isin(["Fenix", "Firefox iOS"])
                & dau_raw.country.isin(self.countries)
                & (dau_raw.channel == "release"),
                ["submission_date", "country", "eligible_clients"],
            ]
            .groupby(["submission_date", "country"])
            .sum()
            .reset_index()
        )

        self.dau_by_country = client_share[
            ["submission_date", "country", "eligible_share_country"]
        ].merge(population, on=["submission_date", "country"])
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
        forecast_start = self.first_day_of_current_month.strftime("%Y-%m-%d")
        countries_string = ",".join(f"'{el}'" for el in self.countries)
        excluded_advertisers_string = ",".join(
            f"'{el}'" for el in self.excluded_advertisers
        )
        query = f"""SELECT
                        submission_date,
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
                        AND release_channel = "release"
                        AND event_type in ("impression", "click")
                        AND source = "topsites"
                        AND country IN ({countries_string})
                    GROUP BY
                        submission_date,
                        country,
                        event_type"""
        client = bigquery.Client(project=GCP_PROJECT_NAME)
        query_job = client.query(query)
        tile_data = query_job.to_dataframe()
        clicks = tile_data.loc[
            tile_data["event_type"] == "click",
            [
                "submission_date",
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
            ["submission_date", "country", "p_amazon", "p_other"],
        ]

        self.events_data = clicks.merge(inventory, on=["submission_date", "country"])

        self.next(self.aggregate_data)

    @step
    def aggregate_data(self):
        """Aggregate events and dau data and create new fields.

        Creates the following new columns:
        - amazon_clicks_per_qdau
        - other_clicks_per_qdau
        """
        aggregate_data = self.events_data.merge(
            self.dau_by_country, on=["submission_date", "country"]
        )

        aggregate_data["amazon_clients"] = (
            aggregate_data["p_amazon"] * aggregate_data["eligible_clients"]
        )
        aggregate_data["other_clients"] = (
            aggregate_data["p_other"] * aggregate_data["eligible_clients"]
        )

        aggregate_data["amazon_clicks_per_qdau"] = (
            aggregate_data["amazon_clicks"] / aggregate_data["amazon_clients"]
        )
        aggregate_data["other_clicks_per_qdau"] = (
            aggregate_data["other_clicks"] / aggregate_data["other_clients"]
        )

        # in notebook this is mobile_forecasting_data
        self.usage_by_date_and_country = aggregate_data

        self.next(self.aggregate_usage)

    @step
    def aggregate_usage(self):
        """Get usage by country by averaging over last month."""
        by_country_usage = (
            self.usage_by_date_and_country[
                (
                    self.usage_by_date_and_country.submission_date
                    >= self.first_day_of_previous_month
                )
            ]
            .groupby("country")[
                [
                    "eligible_share_country",
                    "p_amazon",
                    "p_other",
                    "amazon_clicks_per_qdau",
                    "other_clicks_per_qdau",
                ]
            ]
            .mean()
            .reset_index()
        )

        # in notebook this is last_comp_month
        self.usage_by_country = by_country_usage
        self.next(self.get_cpcs)

    @step
    def get_cpcs(self):
        """Calculate the cpc by country.

        Creates the following country-level columns
        - amazon_cpc
        - other_cpc
        """
        date_start = self.first_day_of_current_month.strftime("%Y-%m-%d")
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
        rev_forecast_dat = pd.merge(country_level_metrics, self.mobile_kpi, how="cross")

        rev_forecast_dat = rev_forecast_dat[
            pd.to_datetime(
                rev_forecast_dat.automated_kpi_confidence_intervals_submission_month
            )
            >= pd.to_datetime(forecast_start_date)
        ]

        rev_forecast_dat["est_value_amazon_qdau"] = (
            rev_forecast_dat["automated_kpi_confidence_intervals_estimated_value"]
            * rev_forecast_dat["eligible_share_country"]
            * rev_forecast_dat["p_amazon"]
        )
        rev_forecast_dat["10p_amazon_qdau"] = (
            rev_forecast_dat[
                "automated_kpi_confidence_intervals_estimated_10th_percentile"
            ]
            * rev_forecast_dat["eligible_share_country"]
            * rev_forecast_dat["p_amazon"]
        )
        rev_forecast_dat["90p_amazon_qdau"] = (
            rev_forecast_dat[
                "automated_kpi_confidence_intervals_estimated_90th_percentile"
            ]
            * rev_forecast_dat["eligible_share_country"]
            * rev_forecast_dat["p_amazon"]
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
            rev_forecast_dat["automated_kpi_confidence_intervals_estimated_value"]
            * rev_forecast_dat["eligible_share_country"]
            * rev_forecast_dat["p_other"]
        )
        rev_forecast_dat["10p_other_qdau"] = (
            rev_forecast_dat[
                "automated_kpi_confidence_intervals_estimated_10th_percentile"
            ]
            * rev_forecast_dat["eligible_share_country"]
            * rev_forecast_dat["p_other"]
        )
        rev_forecast_dat["90p_other_qdau"] = (
            rev_forecast_dat[
                "automated_kpi_confidence_intervals_estimated_90th_percentile"
            ]
            * rev_forecast_dat["eligible_share_country"]
            * rev_forecast_dat["p_other"]
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
        rev_forecast_dat[
            "submission_month"
        ] = rev_forecast_dat.automated_kpi_confidence_intervals_submission_month

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
        ]

        write_df = self.rev_forecast_dat[output_columns]
        if self.test_mode:
            output_info = self.config_data["output"]["test"]
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
