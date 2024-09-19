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

        self.next(self.get_mobile_kpi)

    @step
    def get_mobile_kpi(self):
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

        self.next(self.big_ass_query)

    @step
    def big_ass_query(self):
        """Big query!."""
        query = """WITH client_counts AS (
                    SELECT
                        country,
                    -- want qualified desktop clients, any mobile clients
                        (
                        CASE
                            WHEN normalized_app_name = "Firefox Desktop"
                            AND active_hours_sum > 0
                            AND uri_count > 0
                            THEN 'desktop'
                            WHEN normalized_app_name != "Firefox Desktop"
                            THEN 'mobile'
                            ELSE NULL
                        END
                        ) AS device,
                        submission_date,
                        COUNT(*) AS total_clients,
                        COUNT(
                        CASE
                    -- FIREFOX DESKTOP ELIGIBILITY REQUIREMENTS
                            WHEN normalized_app_name = "Firefox Desktop"
                            AND (
                                -- desktop tiles default on
                                (
                                submission_date >= "2021-09-07"
                                AND browser_version_info.major_version > 92
                                AND country IN UNNEST(
                                    ["AU", "BR", "CA", "DE", "ES", "FR", "GB", "IN", "IT", "MX", "US"]
                                )
                                )
                                OR
                                -- Japan desktop now default on
                                (
                                submission_date >= "2022-01-25"
                                AND browser_version_info.major_version > 92
                                AND country = "JP"
                                )
                            )
                            THEN 1
                    -- ANDROID ELIGIBLITY REQUIREMENTS
                            WHEN normalized_app_name != "Firefox Desktop"
                            AND normalized_os = "Android"
                            AND browser_version_info.major_version > 100
                            AND (
                                (country IN UNNEST(["US"]) AND submission_date >= "2022-05-10")
                                OR (country IN UNNEST(["DE"]) AND submission_date >= "2022-12-05")
                                OR (country IN UNNEST(["BR", "CA", "ES", "FR", "GB", "IN", "AU"]) AND submission_date >= "2023-05-15")
                            )
                            THEN 1
                    -- iOS ELIGIBLITY REQUIREMENTS
                            WHEN normalized_app_name != "Firefox Desktop"
                            AND normalized_os = "iOS"
                            AND browser_version_info.major_version > 101
                            AND (
                                (country IN UNNEST(["US"]) AND submission_date >= "2022-06-07")
                                OR (country IN UNNEST(["DE"]) AND submission_date >= "2022-12-05")
                                OR (country IN UNNEST(["BR", "CA", "ES", "FR", "GB", "IN", "AU"]) AND submission_date >= "2023-05-15")
                            )
                            THEN 1
                            ELSE NULL
                        END
                        ) AS eligible_clients
                    FROM
                        mozdata.telemetry.unified_metrics
                    WHERE
                        mozfun.bits28.active_in_range(days_seen_bits, 0, 1)
                        AND submission_date >= "2024-09-01" --update accordingly
                        AND sample_id < 10
                    GROUP BY
                        country,
                        device,
                        submission_date
                    ),
                    grand_total AS (
                    SELECT
                        device,
                        submission_date,
                        SUM(total_clients) AS monthly_total
                    FROM
                        client_counts
                    WHERE
                        device IS NOT NULL
                    GROUP BY
                        device,
                        submission_date
                    ),
                    client_share AS (
                    SELECT
                        device,
                        country,
                        submission_date,
                        eligible_clients / NULLIF(monthly_total, 0) AS eligible_share_country
                    FROM
                        client_counts
                    LEFT JOIN
                        grand_total
                    USING
                        (submission_date, device)
                    WHERE
                        device IS NOT NULL
                    ),
                    -------- REVENUE FORECASTING DATA
                    tiles_percentages AS (
                    SELECT
                        "sponsored_tiles" AS product,
                        submission_date,
                        country,
                        CASE
                        WHEN form_factor = "phone"
                            THEN "mobile"
                        ELSE "desktop"
                        END AS device,
                        SUM(CASE WHEN advertiser = "amazon" THEN user_count ELSE 0 END) / NULLIF(
                        SUM(user_count),
                        0
                        ) AS p_amazon,
                        SUM(
                        CASE
                            WHEN advertiser NOT IN UNNEST(["amazon", "o=45:a", "yandex"])
                            THEN user_count
                            ELSE 0
                        END
                        ) / NULLIF(SUM(user_count), 0) AS p_other
                    FROM
                        mozdata.contextual_services.event_aggregates
                    WHERE
                        submission_date >= "2024-09-01" --update accordingly
                        AND release_channel = "release"
                        AND event_type = "impression"
                        AND source = "topsites"
                        AND country IN UNNEST(["AU", "BR", "CA", "DE", "ES", "FR", "GB", "IN", "IT", "JP", "MX", "US"])
                    GROUP BY
                        product,
                        submission_date,
                        country,
                        device
                    ),
                    suggest_percentages AS (
                    SELECT
                        "suggest" AS product,
                        submission_date,
                        country,
                        CASE
                        WHEN form_factor = "phone"
                            THEN "mobile"
                        ELSE "desktop"
                        END AS device,
                        NULL AS p_amazon,
                        NULL AS p_other,
                        SUM(CASE WHEN advertiser = "amazon" THEN user_count ELSE 0 END) AS amazon_dou,
                        SUM(
                        CASE
                            WHEN advertiser NOT IN UNNEST(["amazon", "wikipedia"])
                            THEN user_count
                            ELSE 0
                        END
                        ) AS other_dou,
                    FROM
                        mozdata.contextual_services.event_aggregates
                    WHERE
                        submission_date >= "2024-09-01" --update accordingly
                        AND release_channel = "release"
                        AND event_type = "impression"
                        AND source = "suggest"
                        AND country IN UNNEST(["US"])
                    GROUP BY
                        product,
                        submission_date,
                        country,
                        device
                    ),
                    mobile_experiment_clients AS (
                    SELECT
                        client_id
                    FROM
                        `moz-fx-data-experiments.mozanalysis.enrollments_firefox_android_sponsored_shortcuts_experiment`
                    WHERE
                        branch = "treatment-a"
                    UNION ALL
                    SELECT
                        client_id
                    FROM
                        `moz-fx-data-experiments.mozanalysis.enrollments_firefox_ios_homepage_experiment_sponsored_shortcuts`
                    WHERE
                        branch = "treatment-a"
                    ),
                    -- mobile = Sponsored Tiles only
                    -- total mobile clients per day from each OS
                    daily_mobile_clients AS (
                    -- experiment clients
                    SELECT
                        *
                    FROM
                        mobile_experiment_clients
                    LEFT JOIN
                        (
                        SELECT
                            submission_date,
                            client_id,
                            country
                        FROM
                            mozdata.telemetry.unified_metrics AS browser_dau
                        WHERE
                            mozfun.bits28.active_in_range(browser_dau.days_seen_bits, 0, 1)
                                -- don't want Focus apps
                            AND browser_dau.normalized_app_name IN ('Fenix', "Firefox iOS")
                            AND country IN UNNEST(["US"])
                            AND normalized_channel = "release"
                            -- AND sample_id = 1
                            AND (submission_date BETWEEN "2022-05-10" AND "2022-10-03")
                            AND (
                            (normalized_app_name = "Fenix" AND submission_date BETWEEN "2022-05-10" AND "2022-09-19")
                            OR (
                                normalized_app_name = "Firefox iOS"
                                AND (submission_date BETWEEN "2022-06-07" AND "2022-10-03")
                            )
                            )
                        )
                    USING
                        (client_id)
                    WHERE
                        submission_date >= "2024-09-01" --update accordingly OLD VALUE: "2023-07-01"
                    -- then mobile tiles went to default
                    UNION ALL
                    SELECT
                        client_id,
                        submission_date,
                        country
                    FROM
                        mozdata.telemetry.unified_metrics AS browser_dau
                    WHERE
                        mozfun.bits28.active_in_range(browser_dau.days_seen_bits, 0, 1)
                        -- don't want Focus apps
                        AND browser_dau.normalized_app_name IN ('Fenix', "Firefox iOS")
                        AND normalized_channel = "release"
                        AND submission_date >= "2024-09-01" --update accordingly
                        AND (
                        (
                            normalized_app_name = "Fenix"
                            AND (
                            (submission_date >= "2022-09-20" AND country IN UNNEST(["US"]))
                            OR (submission_date >= "2022-12-05" AND country IN UNNEST(["DE"]))
                            OR (country IN UNNEST(["BR", "CA", "ES", "FR", "GB", "IN", "AU"]) AND submission_date >= "2023-05-15")
                            )
                        )
                        OR (
                            normalized_app_name = "Firefox iOS"
                            AND (
                            (submission_date >= "2022-10-04" AND country IN UNNEST(["US"]))
                            OR (submission_date >= "2022-12-05" AND country IN UNNEST(["DE"]))
                            OR (country IN UNNEST(["BR", "CA", "ES", "FR", "GB", "IN", "AU"]) AND submission_date >= "2023-05-15")
                            )
                        )
                        )
                        -- AND sample_id = 1
                    ),
                    -- total mobile clients per day
                    mobile_population AS (
                    SELECT
                        "sponsored_tiles" AS product,
                        submission_date,
                        country,
                        "mobile" AS device,
                        COUNT(*) AS clients
                    FROM
                        daily_mobile_clients
                    GROUP BY
                        product,
                        submission_date,
                        country,
                        device
                    ),
                    -- total desktop and mobile clients per day
                    population AS (
                    SELECT
                        product,
                        submission_date,
                        country,
                        device,
                        clients
                    FROM
                        mobile_population
                    ),
                    -- number of clicks by advertiser (and country and user-selected-time-interval)
                    clicks AS (
                    SELECT
                        "sponsored_tiles" AS product,
                        submission_date,
                        country,
                        CASE
                        WHEN form_factor = "phone"
                            THEN "mobile"
                        ELSE "desktop"
                        END AS device,
                        COALESCE(SUM(CASE WHEN advertiser = "amazon" THEN event_count ELSE 0 END), 0) AS amazon_clicks,
                        COALESCE(
                        SUM(
                            CASE
                            WHEN advertiser NOT IN UNNEST(["amazon", "o=45:a", "yandex"])
                                THEN event_count
                            ELSE 0
                            END
                        ),
                        0
                        ) AS other_clicks
                    FROM
                        mozdata.contextual_services.event_aggregates
                    WHERE
                        submission_date >= "2024-09-01" --update accordingly
                        AND release_channel = "release"
                        AND event_type = "click"
                        AND source = "topsites"
                        AND country IN UNNEST(["AU", "BR", "CA", "DE", "ES", "FR", "GB", "IN", "IT", "JP", "MX", "US"])
                    GROUP BY
                        product,
                        submission_date,
                        country,
                        device
                    UNION ALL
                    SELECT
                        "suggest" AS product,
                        submission_date,
                        country,
                        CASE
                        WHEN form_factor = "phone"
                            THEN "mobile"
                        ELSE "desktop"
                        END AS device,
                        COALESCE(SUM(CASE WHEN advertiser = "amazon" THEN event_count ELSE 0 END), 0) AS amazon_clicks,
                        COALESCE(
                        SUM(CASE WHEN advertiser NOT IN UNNEST(["amazon", "wikipedia"]) THEN event_count ELSE 0 END),
                        0
                        ) AS other_clicks
                    FROM
                        mozdata.contextual_services.event_aggregates
                    WHERE
                        submission_date >= "2024-09-01" --update accordingly
                        AND release_channel = "release"
                        AND event_type = "click"
                        AND source = "suggest"
                        AND country IN UNNEST(["US"])
                    GROUP BY
                        product,
                        submission_date,
                        country,
                        device
                    )
                    -- number of clicks and client-days-of-use by advertiser (and country and month)
                    -- daily AS (
                    SELECT
                    product,
                    submission_date,
                    population.country,
                    device,
                    client_share.eligible_share_country,
                        -- Tiles clients are not directly tagged with advertiser, this must be imputed using impression share
                        -- Limitation: This undercounts due to dual-Tile display model.
                    COALESCE(population.clients, 0) AS clients,
                    (CASE WHEN product = "sponsored_tiles" THEN pe.p_amazon ELSE NULL END) AS p_amazon,
                    (CASE WHEN product = "sponsored_tiles" THEN pe.p_other ELSE NULL END) AS p_other,
                    (
                        CASE
                        WHEN product = "sponsored_tiles"
                            THEN COALESCE(population.clients * pe.p_amazon, 0)
                        ELSE suggest_percentages.amazon_dou
                        END
                    ) AS amazon_clients,
                    (
                        CASE
                        WHEN product = "sponsored_tiles"
                            THEN COALESCE(population.clients * pe.p_other, 0)
                        ELSE suggest_percentages.other_dou
                        END
                    ) AS other_clients,
                        -- clicks are directly tagged with advertiser
                    COALESCE(c.amazon_clicks, 0) AS amazon_clicks,
                    COALESCE(c.other_clicks, 0) AS other_clicks,
                        -- clicks per client-day-of-use
                    (
                        CASE
                        WHEN product = "sponsored_tiles"
                            THEN c.amazon_clicks / NULLIF((population.clients * pe.p_amazon), 0)
                        ELSE c.amazon_clicks / NULLIF(suggest_percentages.amazon_dou, 0)
                        END
                    ) AS amazon_clicks_per_client,
                    (
                        CASE
                        WHEN product = "sponsored_tiles"
                            THEN c.other_clicks / NULLIF((population.clients * pe.p_other), 0)
                        ELSE c.other_clicks / NULLIF(suggest_percentages.other_dou, 0)
                        END
                    ) AS other_clicks_per_client
                    FROM
                    population
                    LEFT JOIN
                    tiles_percentages pe
                    USING
                    (product, submission_date, country, device)
                    LEFT JOIN
                    suggest_percentages
                    USING
                    (product, submission_date, country, device)
                    LEFT JOIN
                    clicks c
                    USING
                    (product, submission_date, country, device)
                    LEFT JOIN
                    client_share
                    USING
                    (device, country, submission_date)
                    # WHERE
                    #   submission_date = @submission_date
                    ORDER BY
                    product,
                    submission_date,
                    country,
                    device"""

        client = bigquery.Client(project=GCS_PROJECT_NAME)
        query_job = client.query(query)
        mobile_forecasting_data = query_job.to_dataframe()
        mobile_forecasting_data = mobile_forecasting_data[
            mobile_forecasting_data.device == "mobile"
        ]

        self.mobile_forecasting_data = mobile_forecasting_data
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
            rev_forecast_dat[
                "automated_kpi_confidence_intervals_estimated_value"
            ].apply(lambda x: float(x))
            * rev_forecast_dat["eligible_share_country"]
            * rev_forecast_dat["p_amazon"]
        )
        rev_forecast_dat["10p_amazon_qdau"] = (
            rev_forecast_dat[
                "automated_kpi_confidence_intervals_estimated_10th_percentile"
            ].apply(lambda x: float(x))
            * rev_forecast_dat["eligible_share_country"]
            * rev_forecast_dat["p_amazon"]
        )
        rev_forecast_dat["90p_amazon_qdau"] = (
            rev_forecast_dat[
                "automated_kpi_confidence_intervals_estimated_90th_percentile"
            ].apply(lambda x: float(x))
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
            rev_forecast_dat[
                "automated_kpi_confidence_intervals_estimated_value"
            ].apply(lambda x: float(x))
            * rev_forecast_dat["eligible_share_country"]
            * rev_forecast_dat["p_other"]
        )
        rev_forecast_dat["10p_other_qdau"] = (
            rev_forecast_dat[
                "automated_kpi_confidence_intervals_estimated_10th_percentile"
            ].apply(lambda x: float(x))
            * rev_forecast_dat["eligible_share_country"]
            * rev_forecast_dat["p_other"]
        )
        rev_forecast_dat["90p_other_qdau"] = (
            rev_forecast_dat[
                "automated_kpi_confidence_intervals_estimated_90th_percentile"
            ].apply(lambda x: float(x))
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
        nb_df = pd.read_parquet("mobile_nb_out.parquet")
        output_for_test = self.rev_forecast_dat.copy()
        nb_df = nb_df.drop(columns="merge_key")
        assert set(nb_df.columns) == set(output_for_test)
        pd.testing.assert_frame_equal(
            nb_df.sort_values(["submission_month", "country"]).reset_index(drop=True),
            output_for_test[nb_df.columns]
            .sort_values(["submission_month", "country"])
            .reset_index(drop=True),
            check_exact=False,
            rtol=0.02,
            check_dtype=False,
        )
        self.next(self.end)

    @step
    def end(self):
        print("yay")


if __name__ == "__main__":
    MobileAdTilesForecastFlow()
