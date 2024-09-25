"""Flow for the Mobile Ad Tiles Forecast."""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yaml
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
from metaflow import FlowSpec, IncludeFile, project, step

GCS_PROJECT_NAME = "moz-fx-data-bq-data-science"
GCS_BUCKET_NAME = "bucket-name-here"


@project(name="mobile_ad_tiles_forecast")
class MobileAdTilesForecastFlow(FlowSpec):
    """Flow for ads tiles forecasting."""

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

        self.next(self.get_kpi_forecast)

    @step
    def get_kpi_forecast(self):
        """Get Mobile KPI Data."""
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
                                    FOR measure in ('observed', 'p05', 'p10',
                                                    'p20', 'p30', 'p40', 'p50', 'p60',
                                                    'p70', 'p80', 'p90', 'p95', 'mean'))),
            output_table as (SELECT CAST(asofdate AS STRING) asofdate,
                                    CAST(submission_date AS STRING) date,
                                    REPLACE(CAST(target AS STRING), "_dau", "") target,
                                    CAST(unit AS STRING) unit,
                                    CAST(forecast_date AS STRING) forecast_date,
                                    CAST(forecast_parameters AS STRING) forecast_parameters,
                                    (SELECT MAX(a) FROM UNNEST([mean, observed]) a WHERE a is not NULL) as value,
                                    p05 as yhat_p5,
                                    p10 as yhat_p10,
                                    p20 as yhat_p20,
                                    p30 as yhat_p30,
                                    p40 as yhat_p40,
                                    p50 as yhat_p50,
                                    p60 as yhat_p60,
                                    p70 as yhat_p70,
                                    p80 as yhat_p80,
                                    p90 as yhat_p90,
                                    p95 as yhat_p95,
                                    FROM pivoted_table)
        SELECT
            date AS automated_kpi_confidence_intervals_submission_month
            ,value AS automated_kpi_confidence_intervals_estimated_value
            ,yhat_p10 AS automated_kpi_confidence_intervals_estimated_10th_percentile
            ,yhat_p90 AS automated_kpi_confidence_intervals_estimated_90th_percentile
        FROM output_table
        WHERE
            unit = 'month'
            AND target = 'mobile'
        ORDER BY date ASC
        """

        client = bigquery.Client(project=GCS_PROJECT_NAME)
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
        client = bigquery.Client(project=GCS_PROJECT_NAME)
        query_job = client.query(query)
        dau_raw = query_job.to_dataframe()
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

        population_countries = ["AU", "BR", "CA", "DE", "ES", "FR", "GB", "IN", "US"]
        population = (
            dau_raw.loc[
                dau_raw.app_name.isin(["Fenix", "Firefox iOS"])
                & dau_raw.country.isin(population_countries)
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
        forecast_start = self.first_day_of_current_month.strftime("%Y-%m-%d")
        query = f"""SELECT
                        submission_date,
                        country,
                        event_type,
                        COALESCE(SUM(IF(advertiser = "amazon",
                                            user_count,
                                            0))/SUM(user_count)) AS p_amazon,
                        COALESCE(
                            SUM(IF(advertiser NOT IN ("amazon", "o=45:a", "yandex"),
                                            user_count,
                                            0))/SUM(user_count)) AS p_other,
                        COALESCE(SUM(
                            IF(advertiser = "amazon",
                                event_count,
                                0)),
                            0) AS amazon_interaction_count,
                        COALESCE(SUM(
                            IF(advertiser NOT IN ("amazon", "o=45:a", "yandex"),
                                event_count,
                                0)),
                            0) AS other_interaction_count
                    FROM
                        mozdata.contextual_services.event_aggregates
                    WHERE
                        submission_date >= "{forecast_start}"
                        AND form_factor = "phone"
                        AND release_channel = "release"
                        AND event_type in ("impression", "click")
                        AND source = "topsites"
                        AND country IN ("AU",
                                        "BR",
                                        "CA",
                                        "DE",
                                        "ES",
                                        "FR",
                                        "GB",
                                        "IN",
                                        "US")
                    GROUP BY
                        submission_date,
                        country,
                        event_type"""
        client = bigquery.Client(project=GCS_PROJECT_NAME)
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
        """aggregate events and dau data and create new fields."""

        aggregate_data = self.events_data.merge(
            self.dau_by_country, on=["submission_date", "country"]
        )

        aggregate_data["amazon_clients"] = (
            aggregate_data["p_amazon"] * aggregate_data["eligible_clients"]
        )
        aggregate_data["other_clients"] = (
            aggregate_data["p_other"] * aggregate_data["eligible_clients"]
        )

        aggregate_data["amazon_clicks_per_client"] = (
            aggregate_data["amazon_clicks"] / aggregate_data["amazon_clients"]
        )
        aggregate_data["other_clicks_per_client"] = (
            aggregate_data["other_clicks"] / aggregate_data["other_clients"]
        )

        self.mobile_forecasting_data = aggregate_data

        self.next(self.get_last_comp_month)

    @step
    def get_last_comp_month(self):
        last_comp_month = (
            self.mobile_forecasting_data[
                (
                    pd.to_datetime(self.mobile_forecasting_data.submission_date)
                    >= pd.to_datetime("2024-08-01")
                )
            ]
            .groupby("country")[
                [
                    "eligible_share_country",
                    "p_amazon",
                    "p_other",
                    "amazon_clicks_per_client",
                    "other_clicks_per_client",
                ]
            ]
            .mean()
            .reset_index()
        )
        self.last_comp_month = last_comp_month
        self.next(self.get_cpcs)

    @step
    def get_cpcs(self):
        table_id_1 = "mozdata.revenue.revenue_data_admarketplace"
        date_start = self.first_day_of_current_month.strftime("%Y-%m-%d")

        query = f"""
        with group_ads AS (
            SELECT
            revenue_data_admarketplace.country_code  AS country,
            advertiser,
            SAFE_DIVIDE(COALESCE(SUM(revenue_data_admarketplace.payout ), 0), COALESCE(SUM(revenue_data_admarketplace.valid_clicks ), 0)) AS cpc
            FROM `{table_id_1}` AS revenue_data_admarketplace
            WHERE
            (revenue_data_admarketplace.adm_date ) >= (DATE('{date_start}'))
            AND (revenue_data_admarketplace.country_code ) IN ('BR', 'CA', 'DE', 'ES', 'FR', 'GB', 'IN', 'AU', 'US')
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

        client = bigquery.Client(project=GCS_PROJECT_NAME)
        query_job = client.query(query)

        self.mobile_cpc = query_job.to_dataframe()

        self.next(self.combine_bq_tables)

    @step
    def combine_bq_tables(self):
        """Combine this biz."""
        forecast_start_date = self.first_day_of_current_month.strftime("%Y-%m-%d")
        rev_forecast_assump = pd.merge(
            self.last_comp_month, self.mobile_cpc, how="left", on="country"
        )
        rev_forecast_dat = pd.merge(rev_forecast_assump, self.mobile_kpi, how="cross")

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
        rev_forecast_dat["amazon_clicks_per_qdau"] = rev_forecast_dat[
            "amazon_clicks_per_client"
        ]
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
        rev_forecast_dat["other_clicks_per_qdau"] = rev_forecast_dat[
            "other_clicks_per_client"
        ]
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

        rev_forecast_dat[
            pd.to_datetime(rev_forecast_dat.submission_month)
            == pd.to_datetime(forecast_start_date)
        ].groupby(["country"])[["total_revenue", "total_clicks"]].sum()

        self.rev_forecast_dat = rev_forecast_dat

        self.next(self.test)

    @step
    def test(self):
        nb_df = pd.read_parquet("mobile_nb_out_0923.parquet")
        output_for_test = self.rev_forecast_dat.copy()
        nb_df = nb_df.drop(columns="merge_key")
        assert set(nb_df.columns) == set(output_for_test)
        pd.testing.assert_frame_equal(
            nb_df.sort_values(["submission_month", "country"]).reset_index(drop=True),
            output_for_test[nb_df.columns]
            .sort_values(["submission_month", "country"])
            .reset_index(drop=True),
            check_exact=False,
            rtol=0.035,
            check_dtype=False,
        )
        self.next(self.end)

    @step
    def end(self):
        print("yay")


if __name__ == "__main__":
    MobileAdTilesForecastFlow()
