# Ad Tiles Forecast

## Overview
This flow forecasts the number of tiles available for desktop.  It does not do any forecasting on it's own, but rather scales the kpi desktop dau forecast to get the desired metrics.  There are three forecasted values created by this pipeline:

- `inventory_forecast`: Forecasted inventory by month and country
- `expected_impressions`: the forecasted number of impressions
- `revenue`: the expected revenue

The output table is indexed by country, month (`submission_month`), and whether or not the revenue column represents including or excluding direct sales (indicated by the values `with_direct_sales` and `no_direct_sales`, repsectively, in the `forecast_type` column).

### `inventory_forecast` derivation
The idea behind the inventory forecast is to execute the following steps:
- **take the (global) monthly dau forecast**:  This currently lives in [docker-etl/kpi_forecasting](https://github.com/mozilla/docker-etl/tree/main/jobs/kpi-forecasting).  This first occurs in the `cdau` column of the dataframe in the `kpi_forecasting` attribute.  Note that in the table this data is pulled from, the forecast is in the `value` column and is represented by many aggregate metrics and the observed value when available.  Part of the pull is keeping only the observed data for data that occurs before the forecast starts (`self.observed_end_date`), and for data after that date getting the median of the simulated values from the prophet forecast (`p50`).
- **turn that into a country-level dau forecast by multiplying a factor representing the fraction of the dau represented by a set of desired countries**:  The country-level dau is obtained from the `active_users_aggregates` table.  A by-month and by-country ratio for the past year is created in `self.hist_dau["share_by_market"]`, which is then turned into a country-level value by taking the average of the ratio
- **turn that into country-level inventory by multiplying a factor representing the ratio of the inventory to the dau by country**: The country-level inventory by month is obtained from the derived table `desktop_tiles_forecast_inputs`.  It is first stored in `self.dau_by_country["total_inventory_1and2"]`.  The ratio between this and the by month country-level dau is taken and averaged over a year to get a time-independent value.

## History and Changes
This pipeline was derived from [this notebook](https://colab.research.google.com/drive/1qOsjCY8G6mM91FU3ZiOfsSZJRi5CpLOj).  The only major change is that the invenetory is now obtained from a derived table rather than from looker, the latter method is described in the notebook.  Other non-functional changes were made for readability and efficiancy.  The outputs of the flow were validated against the notebook outputs to be accurate to within 3%.