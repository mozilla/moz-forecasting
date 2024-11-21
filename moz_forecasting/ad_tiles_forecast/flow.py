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
        for position in segment["positions"]:
            for country, cpm in segment["markets"].items():
                df["country"] = country
                df["direct_sales_CPM"] = cpm
                df["position"] = position
                direct_allocation_df_list.append(df.copy())
    direct_allocation_df = pd.concat(direct_allocation_df_list)

    # the same month/country combination can be present in multiple
    # direct allocation segments
    # aggregate to get the sum of the allocation
    # for the cpm, get the average weighted by allocation
    # this will give us the correct amount of revenue when multiplied by
    # allocated impressions later
    direct_allocation_df["direct_sales_CPM"] = (
        direct_allocation_df["direct_sales_CPM"]
        * direct_allocation_df["direct_sales_allocations"]
    )
    direct_allocation_df = direct_allocation_df.groupby(
        ["submission_month", "country", "position"], as_index=False
    ).sum()
    direct_allocation_df["direct_sales_CPM"] = (
        direct_allocation_df["direct_sales_CPM"]
        / direct_allocation_df["direct_sales_allocations"]
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
    """Flow for ads tiles forecasting."""

    config = IncludeFile(
        name="config",
        is_text=True,
        help="configuration for flow",
        default="moz_forecasting/ad_tiles_forecast/config_2025_planning_baseline.yaml",
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

        if self.set_forecast_month is None:
            self.first_day_of_current_month = datetime.today().replace(day=1)
        else:
            self.first_day_of_current_month = datetime.strptime(
                self.set_forecast_month + "-01", "%Y-%m-%d"
            )
        last_day_of_previous_month = self.first_day_of_current_month - timedelta(days=1)
        first_day_of_previous_month = last_day_of_previous_month.replace(day=1)

        observed_months = self.config_data["observed_months"]
        self.observed_start_date = first_day_of_previous_month - relativedelta(
            months=observed_months
        )
        self.observed_end_date = last_day_of_previous_month

        # tables to get data from
        self.kpi_forecast_table = (
            "moz-fx-data-shared-prod.telemetry_derived.kpi_forecasts_v0"
        )
        self.active_users_aggregates_table = (
            "moz-fx-data-shared-prod.telemetry.active_users_aggregates"
        )
        self.event_aggregates_table = (
            "moz-fx-data-shared-prod.contextual_services.event_aggregates_spons_tiles"
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
        eligibility_factor_columns = []
        for forecast in self.config_data["elgibility"]:
            output_column_name = f"elgibility_fraction_{forecast}"
            # create the column and fill in values for mobile and desktop separately
            global_dau_forecast_observed[output_column_name] = np.nan
            eligibility_factor_columns.append(output_column_name)
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
            .groupby(["submission_month", "platform"], as_index=False)
            .sum()
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
        dau_factors = (
            global_dau_forecast_observed[
                ["country", "platform", "share_by_market"] + eligibility_factor_columns
            ]
            .groupby(["country", "platform"], as_index=False)
            .mean()
        )

        self.dau_factors = dau_factors

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
            + eligibility_factor_columns
        ]

        # calculate by-country forecast
        for column in eligibility_factor_columns:
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
        query_end_date = self.first_day_of_current_month.strftime("%Y-%m-%d")
        tile_impression_data_query = f""" SELECT
                                country,
                                position,
                                (FORMAT_DATE('%Y-%m', submission_date ))
                                    AS submission_month,
                                SUM(impression_count) as sponsored_impressions
                            FROM
                                `{self.event_aggregates_table}`
                            WHERE
                                submission_date >= '{query_start_date}'
                                AND submission_date < '{query_end_date}'
                                AND form_factor = "desktop"
                            GROUP BY
                                country,
                                submission_month,
                                position"""
        client = bigquery.Client(project=GCP_PROJECT_NAME)
        impressions = client.query(tile_impression_data_query).to_dataframe()
        impressions["submission_month"] = pd.to_datetime(
            impressions["submission_month"]
        )
        self.impressions = impressions
        self.next(self.get_newtab_visits)

    @step
    def get_newtab_visits(self):
        """Get newtab visits by country."""
        query_start_date = self.observed_start_date.strftime("%Y-%m-%d")
        query_end_date = self.first_day_of_current_month.strftime("%Y-%m-%d")
        countries = self.config_data["CPM"].keys()
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
                    AND submission_date >= '{query_start_date}'
                    AND submission_date < '{query_end_date}'
                    AND country_code IN ({countries_string})
                GROUP BY
                    submission_month,
                    country_code"""
        client = bigquery.Client(project=GCP_PROJECT_NAME)
        newtab_visits = client.query(query).to_dataframe()
        newtab_visits["submission_month"] = pd.to_datetime(
            newtab_visits["submission_month"]
        )
        self.newtab_visits = newtab_visits
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
            self.newtab_visits.submission_month >= self.observed_start_date
        ) & (self.newtab_visits.submission_month <= self.observed_end_date)
        inventory_observed = self.newtab_visits.loc[
            inventory_observed_data_filter,
            ["submission_month", "country", "newtab_visits"],
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
            observed_data["newtab_visits"] / observed_data["total_active"]
        )

        inventory_per_client = observed_data.groupby("country", as_index=False).mean()
        self.inventory_per_client = inventory_per_client[["inv_per_client", "country"]]
        self.next(self.calculate_forecasted_inventory_by_country)

    @step
    def calculate_forecasted_inventory_by_country(self):
        """Calculate newtab inventory (inventory_forecast).

        This is obtained by merging country averages onto forecast cdau
        and multiplying on the share_by_market and inv_per_client
        country-level factors
        """
        # subset to desktop and relevant countries
        # get markets from CPM
        countries = self.config_data["CPM"].keys()

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
        self.next(self.get_fill_rate)

    @step
    def get_fill_rate(self):
        """Get fill rate by country and tile position.

        Creates fill_rate and sponsored_impressions columns.
        Imputes countries specified in config
        """
        # join on newtab vists and calculate fill rates
        impressions_with_newtab = self.impressions.merge(
            self.newtab_visits, on=["submission_month", "country"], how="inner"
        )

        impressions_with_newtab["fill_rate"] = (
            impressions_with_newtab["sponsored_impressions"]
            / impressions_with_newtab.newtab_visits
        )

        self.fill_rate = impressions_with_newtab

        # impute fill rate for countries specified in config
        if "new_markets" in self.config_data:
            fill_rate_columns = ["submission_month", "country", "fill_rate", "position"]
            imputation_data = self.config_data["new_markets"]
            fill_rate_raw_no_imputation = impressions_with_newtab.loc[
                ~impressions_with_newtab.country.isin(imputation_data),
                fill_rate_columns,
            ]
            imputed_data_list = [fill_rate_raw_no_imputation]
            # iterate through countries to impute
            for country, imputation_info in imputation_data.items():
                impute_with = impressions_with_newtab.loc[
                    impressions_with_newtab.country.isin(
                        imputation_info["countries_to_use"]
                    ),
                    fill_rate_columns,
                ]
                impute_grouped = (
                    impute_with.drop(columns=["country"])
                    .groupby(["submission_month", "position"], as_index=False)
                    .mean()
                )
                impute_grouped["country"] = country
                imputed_data_list.append(impute_grouped)
            fill_rate_with_imputation = pd.concat(imputed_data_list)
            self.fill_rate = fill_rate_with_imputation

        fill_rate_lookback_months = self.config_data["observed_months_fill_rate"]
        lookback_start_date = self.observed_end_date - relativedelta(
            months=fill_rate_lookback_months
        )
        observed_fill_rate_by_country_and_position = self.fill_rate.loc[
            (self.fill_rate.submission_month <= self.observed_end_date)
            & (self.fill_rate.submission_month >= lookback_start_date),
            ["country", "fill_rate", "position"],
        ]

        self.fill_rate_by_country_and_position = (
            observed_fill_rate_by_country_and_position.groupby(
                ["country", "position"], as_index=False
            ).mean()
        )

        self.next(self.add_impression_forecast)

    @step
    def add_impression_forecast(self):
        """Add tile impressions (expected_impressions).

        This is obtained by multiplying the inventory forecast
        by the fill rate
        """
        fill_rate_by_country_and_position = self.fill_rate_by_country_and_position
        self.revenue_forecast = pd.merge(
            self.inventory_forecast, fill_rate_by_country_and_position, on="country"
        )

        self.revenue_forecast["impressions"] = (
            self.revenue_forecast["inventory_forecast"]
            * self.revenue_forecast["fill_rate"]
        )
        self.next(self.account_for_direct_allocations)

    @step
    def account_for_direct_allocations(self):
        """Add columns related to direct sales.

        Following columns are added:
            direct_sales_allocations: The fraction of impressions allocated direct sales
        """
        ## TODO BEFORE PR: do pivot on position before here and use position to join
        ## and do revenue calc
        if "direct_allocations" in self.config_data:
            direct_allocation_df = get_direct_allocation_df(
                self.config_data["direct_allocations"],
                min_month=self.revenue_forecast["submission_month"].min(),
                max_month=self.revenue_forecast["submission_month"].max(),
            )
            amp_forecast = self.revenue_forecast.merge(
                direct_allocation_df[
                    [
                        "submission_month",
                        "country",
                        "position",
                        "direct_sales_allocations",
                        "direct_sales_CPM",
                    ]
                ],
                on=["submission_month", "country", "position"],
                how="left",
            )

            amp_forecast["direct_sales_allocations"] = amp_forecast[
                "direct_sales_allocations"
            ].fillna(0)
        else:
            # no direct sales allocations
            amp_forecast = self.revenue_forecast
            amp_forecast["direct_sales_allocations"] = 0
            amp_forecast["direct_sales_CPM"] = 0
            direct_allocation_df = pd.DataFrame()
        self.amp_forecast = amp_forecast
        self.direct_allocation_df = direct_allocation_df

        self.next(self.forecast_revenue)

    @step
    def forecast_revenue(self):
        """Add revenue_ds and renvenue_no_ds columns.

        These correspond to the revenue forecast for AMP
        when direct sales are remove and the forecast
        assuming all impressions got to AMP, respectively
        """
        CPMs = self.config_data["CPM"]

        by_country_cpm_list = []
        for country, cpms in CPMs.items():
            by_country_cpm_list.append(
                {"country": country, "position": 1, "CPM": cpms["tiles_1_and_2"]}
            )
            by_country_cpm_list.append(
                {"country": country, "position": 2, "CPM": cpms["tiles_1_and_2"]}
            )
            by_country_cpm_list.append(
                {"country": country, "position": 3, "CPM": cpms["tile3"]}
            )
        self.by_country_cpm_df = pd.DataFrame(by_country_cpm_list)

        revenue_forecast = pd.merge(
            self.amp_forecast,
            self.by_country_cpm_df,
            on=["country", "position"],
        )

        # Desktop CPMs were increased by 10% in summer of 2024
        # with an effective date of 2024-09-30
        # as part of the AMP contract renewal conversations.
        after_valid_date = revenue_forecast["submission_month"] <= "2024-09-01"
        revenue_forecast.loc[after_valid_date, "CPM"] = (
            revenue_forecast.loc[after_valid_date, "CPM"] * 1 / 1.1
        )

        # add a revenue column for each impression column
        # create two dfs, one with direct sales and one without
        # and concat

        # multiply inventory by CPMs
        remove_direct_sales_df = revenue_forecast.copy()
        ignore_direct_sales_df = revenue_forecast.copy()
        direct_sales_df = revenue_forecast.copy()

        remove_direct_sales_df["impressions"] = (
            remove_direct_sales_df["impressions"]
            * (
                1 - remove_direct_sales_df["direct_sales_allocations"]
            )  # allocated to AMP
        )

        direct_sales_df["impressions"] = (
            direct_sales_df["impressions"] * direct_sales_df["direct_sales_allocations"]
        )

        remove_direct_sales_df["revenue"] = (
            remove_direct_sales_df["impressions"] * remove_direct_sales_df["CPM"] / 1000
        )
        ignore_direct_sales_df["revenue"] = (
            ignore_direct_sales_df["impressions"] * ignore_direct_sales_df["CPM"] / 1000
        )
        direct_sales_df["revenue"] = (
            direct_sales_df["impressions"] * direct_sales_df["direct_sales_CPM"] / 1000
        )

        ignore_direct_sales_df["forecast_type"] = "no_direct_sales"
        ignore_direct_sales_df["direct_sales_included"] = False
        ignore_direct_sales_df["product"] = "tile"

        remove_direct_sales_df["forecast_type"] = "with_direct_sales"
        remove_direct_sales_df["direct_sales_included"] = True
        remove_direct_sales_df["product"] = "tile"

        direct_sales_df["forecast_type"] = "direct_sales"
        direct_sales_df["direct_sales_included"] = True
        direct_sales_df["product"] = "tile direct sales"
        direct_sales_df["CPM"] = direct_sales_df["direct_sales_CPM"]

        # ignoring direct sales

        revenue_forecast = pd.concat(
            [ignore_direct_sales_df, remove_direct_sales_df, direct_sales_df]
        )

        self.output_df = revenue_forecast

        self.next(self.end)

    @step
    def end(self):
        """Write to BQ."""
        write_df = self.output_df
        write_df["device"] = "desktop"
        write_df["placement"] = "newtab"
        write_df["pricing_model"] = "impressions"
        write_df["forecast_month"] = self.first_day_of_current_month
        write_df["clicks"] = None
        write_df["advertiser"] = None
        write_df = write_df.merge(self.forecast_predicted_at, how="inner", on="device")

        write_df = write_df.rename(columns={"country": "country_code"})

        write_df = write_df[
            [
                "country_code",
                "submission_month",
                "direct_sales_included",
                "advertiser",
                "device",
                "placement",
                "product",
                "pricing_model",
                "forecast_month",
                "forecast_predicted_at",
                "position",
                "impressions",
                "clicks",
                "revenue",
                "CPM",
            ]
        ]

        self.write_df = write_df
        if not self.write or "output" not in self.config_data:
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
        schema = [
            bigquery.SchemaField("country_code", "STRING"),
            bigquery.SchemaField("submission_month", "DATETIME"),
            bigquery.SchemaField("direct_sales_included", "BOOLEAN"),
            bigquery.SchemaField("advertiser", "STRING"),
            bigquery.SchemaField("device", "STRING"),
            bigquery.SchemaField("placement", "STRING"),
            bigquery.SchemaField("product", "STRING"),
            bigquery.SchemaField("pricing_model", "STRING"),
            bigquery.SchemaField("forecast_month", "DATETIME"),
            bigquery.SchemaField("forecast_predicted_at", "TIMESTAMP"),
            bigquery.SchemaField("position", "INTEGER"),
            bigquery.SchemaField("impressions", "FLOAT"),
            bigquery.SchemaField("clicks", "FLOAT"),
            bigquery.SchemaField("revenue", "FLOAT"),
            bigquery.SchemaField("CPM", "FLOAT"),
        ]
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_NEVER",
            schema=schema,
        )

        client = bigquery.Client(project=GCP_PROJECT_NAME)
        print(f"writing to: {target_table}")

        client.load_table_from_dataframe(write_df, target_table, job_config=job_config)


if __name__ == "__main__":
    AdTilesForecastFlow()
