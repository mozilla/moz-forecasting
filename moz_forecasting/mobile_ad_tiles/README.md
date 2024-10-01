# Mobile Ads Tiles Revenue Forecast

## Overview
This flow forecasts the number of clicks and revenue on mobile, distinguishing between those from Amazon ads and those from other sources. Rather than forecasting everything on its own, it scales the KPI mobile DAU forecast to get the desired metrics. Specifically, the inventory forecast is combined with fill rates, to get future impressions. These are then used to produce revenue estimates.  There are three primary forecasted values for each ad source created by this pipeline:

- `est_value_amazon_qdau`/`est_value_other_qdau`:  forecasted daily active users for amazon and other ads
- `amazon_clicks`/`other_clicks`: Forecasted clicks for amazon and non-amazon ads
- `amazon_revenue`/`other_revenue`: the forecasted revenue of impressions

The output table is indexed by country, month (`submission_month`).

### `inventory_forecast` derivation
The idea behind the inventory forecast is to execute the following steps:
- **take the (global) monthly dau forecast**:  This currently lives in [docker-etl/kpi_forecasting](https://github.com/mozilla/docker-etl/tree/main/jobs/kpi-forecasting).  The median, 90th percentile and 10th percentile aggregates are used.
- **turn that into a country-level dau forecast by multiplying a factor representing the fraction of the dau represented by a set of desired countries**:  The country-level dau is obtained from the `active_users_aggregates` table.  A by-month and by-country ratio for the past year is created in `self.dau_by_country["eligible_share_country"]`, which is then turned into a country-level value by taking the average of the ratio
- **distinguish between Amazon and non-Amazon impressisions**: This is done by taking the fraction of users with impressions from each category from the `events_aggregates` table.  Occurs first as `self.aggregate_data['p_amazon']` and `self.aggregate_data['p_other']` for amazon and non-amazon respectively.  These are aggregated to the country level by taking the average.
- **get clicks per dau by advertiser** these are obtained from the `events_aggregates` table, occuring as `self.aggregate_data['amazon_clicks_per_qdau']` and `self.aggregate_data['other_clicks_per_qdau']` for amazon and non-amazon respectively.  These are aggregated to the country level by taking the average.
- **get country-level cpcs**
- **get estimated dau by country and advertiser**: This is the product of the dau forecast, the dau ratio by country, and the ratio of amazon/non-amazon impressions to total impressions. 
- **get forecasted clicks**: This the the product of the dau forcast and the country-level clicks per dau ratio.
- **get forecasted revenue**: this is the prodcut of the forecasted clicks and the country-level cpcs

## History and Changes
This pipeline was derived from [this notebook](https://colab.research.google.com/drive/1qOsjCY8G6mM91FU3ZiOfsSZJRi5CpLOj).  Several major changes were made to the large query from the notebook, which is broken into several smaller queries here:
- Instead of using `mozfun.bits28.active_in_range(browser_dau.days_seen_bits, 0, 1)` applied to the unified_metrics table, the `daily_users` column from the `active_users_aggregates` table is used
- Originally the time period where tiles were experimental and rolled out to only 25% of the population is included.  This is removed here to simplify the query.  This also effects the filters that determine elgibility in the US, using the post-experiment date of Sept 20 2022 and Oct 4 2022 of Android and iOS respectively.