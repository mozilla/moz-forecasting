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


@project(name="ad_tiles_forecast")
class NativeForecastFlow(FlowSpec):
    """Flow for ads tiles forecasting."""

    config = IncludeFile(
        name="config",
        is_text=True,
        help="configuration for flow",
        default="moz_forecasting/native/config.yaml",
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

        self.newtab_clients_table = "mozdata.telemetry.newtab_clients_daily"

        self.country_table = "mozdata.static.country_codes_v1"

        self.next(self.get_country_availability)

    @step
    def get_country_availability(self):
        """Get country availability from table."""
        query = f"""SELECT code as country
                        FROM `{self.country_table}` where pocket_available_on_newtab"""
        client = bigquery.Client(project=GCP_PROJECT_NAME)
        query_job = client.query(query)

        self.available_countries = query_job.to_dataframe()
        self.next(self.get_dau_forecast_by_country)

    @step
    def get_dau_forecast_by_country(self):
        """Get by-country dau forecast.

        The ultimate outcome is creating columns of the form
        dau_forecast_<product> where product is specified as a key under
        the `eligibility` field in the config.  For each product, eligibility
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
        # remove from global_dau_forecat
        global_dau_forecast = global_dau_forecast.drop(columns="forecast_predicted_at")

        # get dau by country from events_users_aggregates

        # extract eligibility functions from config
        # and turn them into a string that can be used in query
        # mobile and desktop each get their own columns
        # when counting eligible daily users for each product
        eligibility_functions = []
        eligibility_function_calls = []
        # iterate over products in the config
        for forecast, eligibility_data in self.config_data["eligibility"].items():
            # currently only support partitioning by platform
            for platform in ["mobile", "desktop"]:
                partition_data = eligibility_data[platform]
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
        eligibility_string = "\n".join(eligibility_functions)
        call_string = "\n".join(call_string)
        query = f"""
                {eligibility_string}

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
        for forecast in self.config_data["eligibility"]:
            output_column_name = f"eligibility_fraction_{forecast}"
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
        dau_forecast_by_country = pd.merge(
            global_dau_forecast, self.dau_factors, how="inner", on=["platform"]
        )

        # calculate by-country forecast
        for column in new_columns:
            forecast_column_name = column.replace(
                "eligibility_fraction", "dau_forecast"
            )
            dau_forecast_by_country[forecast_column_name] = (
                dau_forecast_by_country[column]  # eligibility factor
                * dau_forecast_by_country["share_by_market"]
                * dau_forecast_by_country["median_forecast"]
            )

            # add 90th and 10th percentiles
            dau_forecast_by_country[forecast_column_name + "_p90"] = (
                dau_forecast_by_country[column]  # eligibility factor
                * dau_forecast_by_country["share_by_market"]
                * dau_forecast_by_country["p90_forecast"]
            )

            dau_forecast_by_country[forecast_column_name + "_p10"] = (
                dau_forecast_by_country[column]  # eligibility factor
                * dau_forecast_by_country["share_by_market"]
                * dau_forecast_by_country["p10_forecast"]
            )

            dau_forecast_by_country[forecast_column_name + "_observed"] = (
                dau_forecast_by_country[column]  # eligibility factor
                * dau_forecast_by_country["share_by_market"]
                * dau_forecast_by_country["observed_dau"]
            )
        self.dau_forecast_by_country = dau_forecast_by_country
        self.next(self.get_newtab_impressions)

    @step
    def get_newtab_impressions(self):
        """Get ratio of newtab impression to dau."""
        observed_end_date = self.observed_end_date.strftime("%Y-%m-%d")
        observed_start_date = self.observed_start_date.strftime("%Y-%m-%d")
        countries_string = ",".join(
            f"'{el}'" for el in self.available_countries["country"].values
        )

        query = f"""SELECT
                        (FORMAT_DATE('%Y-%m', submission_date )) AS submission_month,
                        country_code as country,
                            COALESCE(SUM(IF(pocket_sponsored_stories_enabled,
                            newtab_visit_count,
                            0)), 0) AS newtab_impressions_with_spocs
                        FROM `{self.newtab_clients_table}`
                        WHERE
                        submission_date >= '{observed_start_date}'
                        AND submission_date <= '{observed_end_date}'
                        AND country_code IN ({countries_string})
                        AND browser_name = 'Firefox Desktop'
                        GROUP BY
                        1,2"""

        client = bigquery.Client(project=GCP_PROJECT_NAME)
        query_job = client.query(query)
        newtab_impressions_by_country_by_month = query_job.to_dataframe()

        desktop_dau = self.dau_by_country[
            self.dau_by_country.platform == "desktop"
        ].drop(columns="platform")

        newtab_impressions_by_country_by_month["submission_month"] = pd.to_datetime(
            newtab_impressions_by_country_by_month["submission_month"]
        )

        impressions_with_dau = desktop_dau.merge(
            newtab_impressions_by_country_by_month, on=["submission_month", "country"]
        )
        impressions_with_dau["ratio_newtab_impressions_with_spocpocket_to_dou"] = (
            impressions_with_dau["newtab_impressions_with_spocs"]
            / impressions_with_dau["total_active"]
        )

        self.impressions_to_spoc = (
            impressions_with_dau[
                ["country", "ratio_newtab_impressions_with_spocpocket_to_dou"]
            ]
            .groupby("country", as_index=False)
            .mean()
        )
        self.next(self.get_forecast)

    @step
    def get_forecast(self):
        """Calculate native newtab impression forecast."""
        desktop_dau_by_country = self.dau_forecast_by_country[
            self.dau_forecast_by_country.platform == "desktop"
        ]
        desktop_dau_by_country = desktop_dau_by_country.drop(columns="platform")
        forecast = desktop_dau_by_country.merge(self.impressions_to_spoc, on="country")
        forecast["newtab_impressions_with_spocs_enabled"] = (
            (
                forecast["ratio_newtab_impressions_with_spocpocket_to_dou"]
                * forecast["dau_forecast_native"]
            )
            .round()
            .astype("Int64")
        )
        forecast["spoc_inventory_forecast"] = (
            forecast["newtab_impressions_with_spocs_enabled"] * 6
        )
        self.forecast = forecast
        self.next(self.end)

    @step
    def end(self):
        """Write to BQ."""
        write_df = self.forecast[
            [
                "country",
                "submission_month",
                "newtab_impressions_with_spocs_enabled",
                "spoc_inventory_forecast",
            ]
        ]

        write_df["device"] = "desktop"
        write_df["forecast_month"] = self.first_day_of_current_month
        write_df = write_df.merge(self.forecast_predicted_at, how="inner", on="device")

        assert set(write_df.columns) == {
            "forecast_month",
            "forecast_predicted_at",
            "country",
            "submission_month",
            "newtab_impressions_with_spocs_enabled",
            "spoc_inventory_forecast",
            "device",
        }
        if self.write:
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
    NativeForecastFlow()
