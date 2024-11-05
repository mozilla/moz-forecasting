# Ads Tiles Revenue Forecast

## Overview
This flow forecasts the number of tiles available for desktop. Rather than forecasting everything on its own, it scales the KPI desktop DAU forecast to get the desired metrics. Specifically, the inventory forecast is combined with fill rates, to get future impressions. These are then used to produce revenue estimates.  There are three forecasted values created by this pipeline:

- `inventory_forecast`: Forecasted inventory by month and country
- `expected_impressions`: the forecasted number of impressions
- `revenue`: the expected revenue

The output table is indexed by country, month (`submission_month`), and whether or not the revenue column represents including or excluding direct sales (indicated by the values `with_direct_sales` and `no_direct_sales`, repsectively, in the `forecast_type` column).

### `inventory_forecast` derivation
The idea behind the inventory forecast is to execute the following steps:
1. **get by-country dau forecast** The by-country dau forecast is obtained by multipling the global dau forecast by two scalar factors, a `share_by_market` factor and an `elgbility_factor`:
   - global dau forecast: The global dau forecast comes from the table `moz-fx-data-shared-prod.telemetry_derived.kpi_forecasts_v0`.  We use the monthly value, which is the cumulative DAU for the month.
   - `share_by_market`:  The purpose of this factor is to convert the global forcast to a by-country forecast.  It is derived by taking the monthly cumulative DAU from `moz-fx-data-shared-prod.telemetry.active_users_aggregates` for a given country and dividing it by the total across all countries, yielding the fraction of the total dau contributed by that country.  This is then averaged over the number of months of past data specified in `observed_months` in the config file to get a scalar value for each country.
   -  `elgibility_factor`: The elgibility is specified for each product (IE tiles) individually for mobile and desktop as a sql function in the config file for each flow.  It is determined by which app version that the product became available in each country.  The monthly cumulative number of eligibile, active users **within a country** is calculated using this function on `moz-fx-data-shared-prod.telemetry.active_users_aggregates`, and divided by the total number of active users **within a country** to get a monthly elgbility factor for that country.  These are then averaged over the number of months specified in `observed_months` in the config to get a scalar value for each country.
2. **turn that into country-level inventory by multiplying a factor representing the ratio of the inventory to the dau by country**: The country-level inventory by month is obtained from the derived table `desktop_tiles_forecast_inputs`.  It is first stored in `self.dau_by_country["total_inventory_1and2"]`.  The ratio between this and the by month country-level dau is taken and averaged over a year to get a time-independent value.

## History and Changes
This pipeline was derived from [this notebook](https://colab.research.google.com/drive/1qOsjCY8G6mM91FU3ZiOfsSZJRi5CpLOj).  The only major change is that the invenetory is now obtained from a derived table rather than from looker, the latter method is described in the notebook.  Other non-functional changes were made for readability and efficiancy.  The outputs of the flow were validated against the notebook outputs to be accurate to within 3%.