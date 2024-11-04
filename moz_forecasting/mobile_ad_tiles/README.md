# Mobile Ads Tiles Revenue Forecast

## Overview
This flow forecasts the number of clicks and revenue on mobile, distinguishing between those from Amazon ads and those from other sources. Rather than forecasting everything on its own, it scales the KPI mobile DAU forecast to get the desired metrics. Specifically, the inventory forecast is combined with fill rates, to get future impressions. These are then used to produce revenue estimates.  There are three primary forecasted values for each ad source created by this pipeline:

- `est_value_amazon_qdau`/`est_value_other_qdau`:  forecasted daily active users for amazon and other ads
- `amazon_clicks`/`other_clicks`: Forecasted clicks for amazon and non-amazon ads
- `amazon_revenue`/`other_revenue`: the forecasted revenue of impressions

The output table is indexed by country, month (`submission_month`).

### `inventory_forecast` derivation
The idea behind the inventory forecast is to execute the following steps:
1. **get by-country dau forecast** The by-country dau forecast is obtained by multipling the global dau forecast by two scalar factors, a `share_by_market` factor and an `elgbility_factor`:
   - global dau forecast: The global dau forecast comes from the table `moz-fx-data-shared-prod.telemetry_derived.kpi_forecasts_v0`.  We use the monthly value, which is the cumulative DAU for the month.
   - `share_by_market`:  The purpose of this factor is to convert the global forcast to a by-country forecast.  It is derived by taking the monthly cumulative DAU from `moz-fx-data-shared-prod.telemetry.active_users_aggregates` for a given country and dividing it by the total across all countries, yielding the fraction of the total dau contributed by that country.  This is then averaged over the number of months of past data specified in `observed_months` in the config file to get a scalar value for each country.
   -  `elgibility_factor`: The elgibility is specified for each product (IE tiles) individually for mobile and desktop as a sql function in the config file for each flow.  It is determined by which app version that the product became available in each country.  The monthly cumulative number of eligibile, active users **within a country** is calculated using this function on `moz-fx-data-shared-prod.telemetry.active_users_aggregates`, and divided by the total number of active users **within a country** to get a monthly elgbility factor for that country.  These are then averaged over the number of months specified in `observed_months` in the config to get a scalar value for each country.
2. **turn that into country-level inventory by multiplying a factor representing the ratio of the inventory to the dau by country**: The country-level inventory by month is obtained from the derived table `desktop_tiles_forecast_inputs`.  It is first stored in `self.dau_by_country["total_inventory_1and2"]`.  The ratio between this and the by month country-level dau is taken and averaged over a year to get a time-independent value.
3. **distinguish between Amazon and non-Amazon impressisions**: This is done by taking the fraction of users with impressions from each category from the `events_aggregates` table.  Occurs first as `self.aggregate_data['p_amazon']` and `self.aggregate_data['p_other']` for amazon and non-amazon respectively.  These are calculated for each country for the month preceeding the forecast month.
4. **get clicks per dau by advertiser** these are obtained from the `events_aggregates` table, occuring as `self.aggregate_data['amazon_clicks_per_qdau']` and `self.aggregate_data['other_clicks_per_qdau']` for amazon and non-amazon respectively.  These are calculated for each country for the month preceeding the forecast month.
5. **get country-level cpcs**
6. **get estimated dau by country and advertiser**: This is the product of the dau forecast, the dau ratio by country, and the ratio of amazon/non-amazon impressions to total impressions. 
7. **get forecasted clicks**: This the the product of the dau forcast and the country-level clicks per dau ratio.
8. **get forecasted revenue**: this is the prodcut of the forecasted clicks and the country-level cpcs

## History and Changes
This pipeline was derived from [this notebook](https://colab.research.google.com/drive/1qOsjCY8G6mM91FU3ZiOfsSZJRi5CpLOj).  Several major changes were made to the large query from the notebook, which is broken into several smaller queries here:
- Instead of using `mozfun.bits28.active_in_range(browser_dau.days_seen_bits, 0, 1)` applied to the unified_metrics table, the `daily_users` column from the `active_users_aggregates` table is used
- Originally the time period where tiles were experimental and rolled out to only 25% of the population is included.  This is removed here to simplify the query.  This also effects the filters that determine elgibility in the US, using the post-experiment date of Sept 20 2022 and Oct 4 2022 of Android and iOS respectively.