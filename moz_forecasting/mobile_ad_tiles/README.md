# Mobile Ads Tiles Revenue Forecast

## Overview
This flow forecasts the number of clicks and revenue on mobile, distinguishing between those from Amazon ads and those from other sources. Rather than forecasting everything on its own, it scales the KPI mobile DAU forecast to get the desired metrics. Specifically, the inventory forecast is combined with fill rates, to get future impressions. These are then used to produce revenue estimates.  There are three primary forecasted values for each ad source created by this pipeline:

- `est_value_amazon_qdau`/`est_value_other_qdau`:  forecasted daily active users for amazon and other ads
- `amazon_clicks`/`other_clicks`: Forecasted clicks for amazon and non-amazon ads
- `amazon_revenue`/`other_revenue`: the forecasted revenue of impressions

The output table is indexed by country, month (`submission_month`).

### `inventory_forecast` derivation
The idea behind the inventory forecast is to execute the following steps:
- **get by-country dau forecast** The basis of the forecast is the global dau forecast in `moz-fx-data-shared-prod.telemetry_derived.kpi_forecasts_v0`.  It is turned into a country-level forecast by multiplying on two factors, the `share_by_market` and the elgibility_factor.  Both factors are calculated at the monthly level for each country.  The share by market for a country is the number of daily users in that country dividen by the total users in that month.  The elgiblility factors is the number of elgibile users within a country divided by the total number of active users within that country.  The elgibility is specified for each product (IE tiles) individually for mobile and desktop as a sql function in the config file for each flow.  It is determined by which app version that the product became available in each country.
- **distinguish between Amazon and non-Amazon impressisions**: This is done by taking the fraction of users with impressions from each category from the `events_aggregates` table.  Occurs first as `self.aggregate_data['p_amazon']` and `self.aggregate_data['p_other']` for amazon and non-amazon respectively.  These are calculated for each country for the month preceeding the forecast month.
- **get clicks per dau by advertiser** these are obtained from the `events_aggregates` table, occuring as `self.aggregate_data['amazon_clicks_per_qdau']` and `self.aggregate_data['other_clicks_per_qdau']` for amazon and non-amazon respectively.  These are calculated for each country for the month preceeding the forecast month.
- **get country-level cpcs**
- **get estimated dau by country and advertiser**: This is the product of the dau forecast, the dau ratio by country, and the ratio of amazon/non-amazon impressions to total impressions. 
- **get forecasted clicks**: This the the product of the dau forcast and the country-level clicks per dau ratio.
- **get forecasted revenue**: this is the prodcut of the forecasted clicks and the country-level cpcs

## History and Changes
This pipeline was derived from [this notebook](https://colab.research.google.com/drive/1qOsjCY8G6mM91FU3ZiOfsSZJRi5CpLOj).  Several major changes were made to the large query from the notebook, which is broken into several smaller queries here:
- Instead of using `mozfun.bits28.active_in_range(browser_dau.days_seen_bits, 0, 1)` applied to the unified_metrics table, the `daily_users` column from the `active_users_aggregates` table is used
- Originally the time period where tiles were experimental and rolled out to only 25% of the population is included.  This is removed here to simplify the query.  This also effects the filters that determine elgibility in the US, using the post-experiment date of Sept 20 2022 and Oct 4 2022 of Android and iOS respectively.