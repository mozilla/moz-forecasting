# Ads Tiles Revenue Forecast

## Overview
This flow forecasts the number of tiles available for desktop. Rather than forecasting everything on its own, it scales the KPI desktop DAU forecast to get the desired metrics. Specifically, the inventory forecast is combined with fill rates, to get future impressions. These are then used to produce revenue estimates.  There are three forecasted values created by this pipeline:

- `inventory_forecast`: Forecasted inventory by month and country
- `expected_impressions`: the forecasted number of impressions
- `revenue`: the expected revenue

The output table is indexed by country, month (`submission_month`), and whether or not the revenue column represents including or excluding direct sales (indicated by the values `with_direct_sales` and `no_direct_sales`, repsectively, in the `forecast_type` column).

### `inventory_forecast` derivation
The idea behind the inventory forecast is to execute the following steps:
- **get by-country dau forecast** The basis of the forecast is the global dau forecast in `moz-fx-data-shared-prod.telemetry_derived.kpi_forecasts_v0`.  It is turned into a country-level forecast by multiplying on two factors, the `share_by_market` and the elgibility_factor.  Both factors are calculated at the monthly level for each country.  The share by market for a country is the number of daily users in that country dividen by the total users in that month.  The elgiblility factors is the number of elgibile users within a country divided by the total number of active users within that country.  The elgibility is specified for each product (IE tiles) individually for mobile and desktop as a sql function in the config file for each flow.  It is determined by which app version that the product became available in each country.
- **turn that into country-level inventory by multiplying a factor representing the ratio of the inventory to the dau by country**: The country-level inventory by month is obtained from the derived table `desktop_tiles_forecast_inputs`.  It is first stored in `self.dau_by_country["total_inventory_1and2"]`.  The ratio between this and the by month country-level dau is taken and averaged over a year to get a time-independent value.

## History and Changes
This pipeline was derived from [this notebook](https://colab.research.google.com/drive/1qOsjCY8G6mM91FU3ZiOfsSZJRi5CpLOj).  The only major change is that the invenetory is now obtained from a derived table rather than from looker, the latter method is described in the notebook.  Other non-functional changes were made for readability and efficiancy.  The outputs of the flow were validated against the notebook outputs to be accurate to within 3%.