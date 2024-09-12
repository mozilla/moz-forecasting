# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.


from metaflow import (
    FlowSpec,
    step,
)
from google.cloud import bigquery
import pandas as pd
import numpy as np

GCS_PROJECT_NAME = "project-name-here"
GCS_BUCKET_NAME = "bucket-name-here"


class AdForecastFlow(FlowSpec):
    """
    This flow is a template for you to use
    for orchestration of your model.
    """

    @step
    def start(self):
        """
        Each flow has a 'start' step.

        You can use it for collecting/preprocessing data or other setup tasks.
        """
        # set up GCS

        self.next(self.get_tile_data)

    # @kubernetes(image="registry.hub.docker.com/chelseatroy/mozmlops:latest", cpu=1)
    @step
    def get_tile_data(self):
        """
        retrieve tile impressions
        """
        tile_data_query = "SELECT * FROM `moz-fx-data-bq-data-science.jsnyder.tiles_results_temp`"
        client = bigquery.Client(project="moz-fx-data-bq-data-science")
        hist_inventory = client.query(tile_data_query).to_dataframe()

        inventory = hist_inventory.copy()
        data_types_dict = {'country': str,
                        'submission_month': str,
                        'user_count': float,
                        'impression_count_1and2': float,
                        'visit_count': float,
                        'clients': float,
                        'total_inventory_1and2': float,
                        'fill_rate': float,
                        }
        inventory = inventory.replace(r'^\s*$', np.nan, regex=True).astype(data_types_dict)
        inventory['submission_month'] = pd.to_datetime(inventory['submission_month'])
        inventory.rename(columns={'country':'live_markets'}, inplace=True)

        self.inventory = inventory

        self.next(self.get_kpi_forecast)

    @step 
    def get_kpi_forecast(self):
        table_id_1 = 'moz-fx-data-shared-prod.telemetry_derived.kpi_forecasts_v0'
        observed_date_start, observed_date_end = '2023-07-01', '2024-07-31' # update observed_date_end every run
        forecast_date_start, forecast_date_end = observed_date_end, '2026-02-01' # update forecast_date_end every run--18 months out

        query = f"""
        WITH tmp_kpi_forecasts AS (
            WITH most_recent_forecasts AS (
                SELECT aggregation_period,
                    metric_alias,
                    metric_hub_app_name,
                    metric_hub_slug,
                    MAX(forecast_predicted_at) AS forecast_predicted_at
                FROM `moz-fx-data-shared-prod.telemetry_derived.kpi_forecasts_v0`
                GROUP BY aggregation_period, metric_alias, metric_hub_app_name, metric_hub_slug
            )

            SELECT forecasts.* EXCEPT(forecast_parameters)
                FROM `{table_id_1}` AS forecasts
                JOIN most_recent_forecasts
            USING(aggregation_period, metric_alias, metric_hub_app_name, metric_hub_slug, forecast_predicted_at)
            ORDER BY submission_date ASC
            )
        SELECT
            *
        FROM
            (SELECT
                    (tmp_kpi_forecasts.submission_date ) AS submission_month,
                    AVG(tmp_kpi_forecasts.value ) AS cdau
                FROM tmp_kpi_forecasts
                WHERE (
                    ((tmp_kpi_forecasts.measure ) = 'observed'  AND (( tmp_kpi_forecasts.submission_date  ) >= (DATE('{observed_date_start}')) AND ( tmp_kpi_forecasts.submission_date  ) < (DATE('{observed_date_end}'))))
                    OR ((tmp_kpi_forecasts.measure ) = 'p50'  AND (( tmp_kpi_forecasts.submission_date  ) >= (DATE('{forecast_date_start}')) AND ( tmp_kpi_forecasts.submission_date  ) < (DATE('{forecast_date_end}'))))
                    )
                AND (tmp_kpi_forecasts.aggregation_period ) = 'month'
                AND (tmp_kpi_forecasts.metric_alias ) LIKE 'desktop_dau'
                GROUP BY
                    1
                HAVING cdau IS NOT NULL) AS t3
        ORDER BY
            1
        """

        client = bigquery.Client(project="mozdata")
        query_job = client.query(query)


        kpi_forecast = query_job.to_dataframe()
        kpi_forecast["merge_key"]=1

        self.kpi_forecast = kpi_forecast

        self.next(self.get_ratio_from_live_markets)

    @step
    def get_ratio_from_live_markets(self):
        table_id_1 = 'moz-fx-data-shared-prod.telemetry.active_users_aggregates'
        table_id_2 = 'mozdata.static.country_codes_v1'


        query = f"""
        SELECT
        (FORMAT_DATE('%Y-%m', active_users_aggregates.submission_date )) AS submission_month,
        CASE
            WHEN (countries.code = 'AU') THEN 'AU'
            WHEN (countries.code = 'BR') THEN 'BR'
            WHEN (countries.code = 'CA') THEN 'CA'
            WHEN (countries.code = 'DE') THEN 'DE'
            WHEN (countries.code = 'ES') THEN 'ES'
            WHEN (countries.code = 'FR') THEN 'FR'
            WHEN (countries.code = 'GB') THEN 'GB'
            WHEN (countries.code = 'IN') THEN 'IN'
            WHEN (countries.code = 'IT') THEN 'IT'
            WHEN (countries.code = 'JP') THEN 'JP'
            WHEN (countries.code = 'MX') THEN 'MX'
            WHEN (countries.code = 'US') THEN 'US'
        ELSE
        'Other'
        END
        AS live_markets,
        COALESCE(SUM((active_users_aggregates.dau) ), 0) AS dau_by_country
        FROM
        `{table_id_1}` AS active_users_aggregates
        LEFT JOIN
        `{table_id_2}` AS countries
        ON
        active_users_aggregates.country = countries.code
        WHERE
        (active_users_aggregates.app_name ) = 'Firefox Desktop'
        AND ((( active_users_aggregates.submission_date ) >= ((DATE_ADD(DATE_TRUNC(CURRENT_DATE('UTC'), MONTH), INTERVAL -12 MONTH)))
            AND ( active_users_aggregates.submission_date ) < ((DATE_ADD(DATE_ADD(DATE_TRUNC(CURRENT_DATE('UTC'), MONTH), INTERVAL -12 MONTH), INTERVAL 12 MONTH)))))
        GROUP BY
        1,
        2
        ORDER BY 1, 2
        """


        client = bigquery.Client(project="mozdata")
        query_job = client.query(query)


        self.mo_by_country = query_job.to_dataframe(progress_bar_type = 'tqdm',
                                      bool_dtype = None,
                                      int_dtype = None,
                                      float_dtype = None,
                                      date_dtype = None)
        self.next(self.join_kpi_forecasts_and_historical_usage)
        
    @step
    def join_kpi_forecasts_and_historical_usage(self):
        # Join KPI forecast and historical usage distribution to get country-level KPI forecast
        self.kpi_forecast["submission_month"] = pd.to_datetime(self.kpi_forecast["submission_month"])
        self.mo_by_country["submission_month"] = pd.to_datetime(self.mo_by_country["submission_month"])
        start_observed_date, end_observed_date = "2023-08-01", "2024-08-01" # update as needed

        hist_dau = pd.merge(self.kpi_forecast[(self.kpi_forecast.submission_month >= pd.to_datetime(start_observed_date)) & \
                                        (self.kpi_forecast.submission_month < pd.to_datetime(end_observed_date))], \
                            self.mo_by_country[self.mo_by_country.live_markets != "Other"], how="left", on=["submission_month"])

        hist_dau["share_by_market"] = hist_dau["dau_by_country"]/hist_dau["cdau"]
        self.hist_dau = hist_dau
        self.next(self.calculate_new_tabs_per_client)

    @step
    def calculate_new_tabs_per_client(self):

        # Merge country level KPI forecast with inventory data
        start_observed_date, end_observed_date = "2023-08-01", "2024-08-01" # update as needed

        inventory_filter = (self.inventory.submission_month >= pd.to_datetime(start_observed_date)) & \
        (self.inventory.submission_month < pd.to_datetime(end_observed_date))
        # 'live_markets' <- 'country'
        hist_dau_inv = pd.merge(self.hist_dau, self.inventory[inventory_filter][["submission_month", "live_markets", "total_inventory_1and2"]], \
                                how="inner", on=["live_markets", "submission_month"])
        hist_dau_inv["inv_per_client"] = 1.0 * hist_dau_inv["total_inventory_1and2"]/ hist_dau_inv["dau_by_country"]

        hist_avg = hist_dau_inv.groupby("live_markets").mean()[["share_by_market", "inv_per_client"]].reset_index()
        hist_avg["merge_key"]=1
        self.hist_avg = hist_avg
        self.next(self.merge_forecast_with_historical_average)

    @step
    def merge_forecast_with_historical_average(self):
        start_observed_date, end_observed_date = "2023-08-01", "2024-08-01" # update as needed

        inventory_forecast = pd.merge(self.kpi_forecast[self.kpi_forecast.submission_month >= pd.to_datetime(end_observed_date)], \
                                    self.hist_avg, how="left", \
                                    on="merge_key")[["submission_month", "live_markets", "cdau", "share_by_market", "inv_per_client"]]

        inventory_forecast["country_dau"] = inventory_forecast["cdau"] *  inventory_forecast["share_by_market"]
        inventory_forecast["country_inventory"] = inventory_forecast["country_dau"] * inventory_forecast["inv_per_client"]
        self.inventory_forecast = inventory_forecast
        self.next(self.add_impression_forecast)

    @step
    def add_impression_forecast(self):
        ### ADJUST FOR AVERAGE OF LAST 6 MONTH'S FILL

        fill_last = {
                'AU': 0.765,
                'BR': 0.847, # changed by ~.05
                'CA': 0.861,
                'DE': 0.882,
                'ES': 0.880,
                'FR': 0.825,
                'GB': 0.867,
                'IN': 0.707, # changed by ~.05
                'IT': 0.866,
                'JP': 0.753,
                'MX': 0.873,
                'US': 0.930
                }


        fill_last_dat = pd.Series(fill_last, name='fill_rate')
        fill_last_dat.index.name = 'live_markets'
        fill_last_dat.reset_index()
        self.revenue_forecast = pd.merge(self.inventory_forecast, fill_last_dat, on="live_markets")
        self.revenue_forecast["expected_impressions_last_cap"] = self.revenue_forecast["country_inventory"] * self.revenue_forecast["fill_rate"]
        self.next(self.account_for_direct_allocations)

    @step
    def account_for_direct_allocations(self):
        # Update these dates accordingly
        date_start_direct_sales , new_year_date = '2023-10-01', '2024-01-01'
        # next_date = '2024-08-01' # Jan 2024: Updated as per agreement with working group on 1/15 that direct sales should go up in H2
        next_date = '2025-01-01' # Feb 2024: Updated as per agreement with working group that direct sales should go up after 2024-12-31

        # Tile 2 inventory allocation / 2 -> full inventory that we'll retain
        first_stage_allocation = 1.00 - (0.10 / 2)
        second_stage_allocation = 1.00 - (0.10 / 2) # Revised down from 0.20 / 2 on 11/8 due to weakness in sales pipeline
        third_stage_allocation =  1.00 - (0.20 / 2)

        country_mask = self.revenue_forecast.live_markets.isin(["DE", "GB", "US"])
        self.revenue_forecast["direct_sales_markets"] = np.where(country_mask, "y", "n")
        first_stage_mask = (((self.revenue_forecast.submission_month >= date_start_direct_sales) & \
                            (self.revenue_forecast.submission_month < new_year_date)) & (country_mask))
        second_stage_mask = (((self.revenue_forecast.submission_month >= new_year_date) & \
                            (self.revenue_forecast.submission_month < next_date)) & (country_mask))
        third_stage_mask = (((self.revenue_forecast.submission_month >= next_date) & (country_mask)))
        self.revenue_forecast["direct_sales_allocations"] = np.where(first_stage_mask,
                                                    first_stage_allocation,
                                                    np.where(second_stage_mask,
                                                                second_stage_allocation,
                                                                np.where(third_stage_mask,
                                                                        third_stage_allocation,
                                                                        1.00)))

        self.revenue_forecast["expected_impressions_direct_sales"] = np.where(self.revenue_forecast.direct_sales_markets == "y",
                                                            self.revenue_forecast["expected_impressions_last_cap"] * self.revenue_forecast["direct_sales_allocations"],
                                                            self.revenue_forecast["expected_impressions_last_cap"] )
        self.next(self.forecast_revenue)

    @step
    def forecast_revenue(self):
        RPMs = {'AU': 0.09,
        'BR': 0.06,
        'CA': 0.30,
        'DE': 0.48,
        'ES': 0.18,
        'FR': 0.21,
        'GB': 0.35,
        'IN': 0.04,
        'IT': 0.33,
        'JP': 0.02,
        'MX': 0.19,
        'US': 0.57
        }

        RPM_dat = pd.Series(RPMs, name='RPM')
        RPM_dat.index.name = 'live_markets'
        RPM_dat.reset_index()
        revenue_forecast_temp = pd.merge(self.revenue_forecast, RPM_dat, on="live_markets")

        RPM_df = pd.DataFrame(RPM_dat)
        RPM_df['valid_until'] = pd.to_datetime('2024-09-01')
        RPM2_df = RPM_df.copy()

        RPM2_df['RPM'] = (RPM2_df['RPM'] * 1.100)
        RPM2_df['valid_until'] = pd.to_datetime('2099-01-01')

        RPM_df = pd.concat([RPM_df,RPM2_df]).reset_index()
        RPM_df


        revenue_forecast_exploded = revenue_forecast_temp.merge(RPM_df, how='cross')
        revenue_forecast_1 = revenue_forecast_exploded.query("live_markets_x == live_markets_y & submission_month<=valid_until & valid_until=='2024-09-01'")
        revenue_forecast_2 = revenue_forecast_exploded.query("live_markets_x == live_markets_y & submission_month>'2024-09-01' & valid_until>'2024-09-01'")

        revenue_forecast_final = pd.concat([revenue_forecast_1, revenue_forecast_2]).drop(columns=['RPM_x', 'live_markets_y', 'valid_until']).sort_values(['live_markets_x','submission_month'])
        revenue_forecast_final.rename(columns={'live_markets_x': 'live_markets',
                                            'RPM_y': 'RPM'},inplace=True)
        
        # multiply inventory by RPMs
        revenue_forecast_final["revenue_no_ds"] = pd.to_numeric(revenue_forecast_final["expected_impressions_last_cap"]) * \
                                            revenue_forecast_final["RPM"]/1000
        revenue_forecast_final["revenue_ds"] = pd.to_numeric(revenue_forecast_final["expected_impressions_direct_sales"]) * \
                                        revenue_forecast_final["RPM"]/1000

        revenue_forecast_final.groupby(["submission_month"]).sum()[["revenue_ds", "revenue_no_ds"]]

        self.output_df = revenue_forecast_final

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
        self.output_df['submission_month'] = self.output_df['submission_month'].astype('datetime64[ms]')
        nb_df = pd.read_parquet("nb_output.parquet")
        pd.testing.assert_frame_equal(nb_df.reset_index(drop=True), self.output_df.reset_index(drop=True), check_exact=False, rtol=.05)

        


if __name__ == "__main__":
    AdForecastFlow()
