# Native Forecast

## Overview
This flow forecasts total pocket impressions by position.  Currently, there are a static set of positions on the newtab page that can be used for sponsored pocket stories (SPOCs), specifically (counting from zero) 1,5,7,11,18,20. For these positions, the maximum number of possible SPOC impressions for that position is the same as the number of **total** pocket impressions (as oppposed to just sponsored) by tile.  The sum of total forecasted impressions over just these tiles will be the total SPOC inventory on the newtab page.

The output table is indexed by country, month (`submission_month`), and position with the forecast in the `forecast_spoc_inventory` column

Currently the data only runs on Firefox desktop

### `inventory_forecast` derivation
![inventory forecast flow diagram](native.drawio.png)
The idea behind the inventory forecast is to execute the following steps, illustrated in the diagram above:
- **get by-country dau forecast**: This is done by running monthly cumulative DAU since 2019-01 through the darts `StatsForecastAutoARIMA` model by country.  The  `season_length` is set to 12 (to represent yearly seasonality) but all other parameters are set to the default values.  More details on this object can be found [here](https://unit8co.github.io/darts/generated_api/darts.models.forecasting.sf_auto_arima.html).
- **turn that into country-level newtab visits with sponsored stories by multiplying a factor representing the ratio of visits to the dau by country**: The country-level inventory by month is obtained from the `mozdata.telemetry.newtab_clients_daily` table.  Visits are counted by month and country when the `pocket_sponsored_stories_enabled` and `pocket_enabled` flags are true.  The ratio is calculated by month and rolled up to the country level by averaging over one year.  It is first stored in `self.impressions_to_newtab_with_spocs_factor["ratio_newtab_visits_with_spocpocket_to_dou"]`.
- **turn this into SPOC inventory by multiplying by a factor representing the number of impressions per newtab visit by position and country**: The by-country, by-position value is obtained from `mozdata.telemetry.newtab_visits`.  Visits are counted by position, month and country when the `pocket_sponsored_stories_enabled` and `pocket_enabled` flags are true. It is averaged over 12 months.  It is stored in `self.spocs_to_newtab_visits_factor["ratio_pocket_impressions_to_newtab_visits"]`

## Scheduled Production Run
The monthly update it scheduled to run 1 AM UTC on the 3rd of every month. The deployment can be viewed in Outerbounds [here](https://ui.desertowl.obp.outerbounds.com/dashboard/flows/p/revenue/nativeforecast.prod.nativeforecastflow).  The deployment is updated automatically via the circleci job `update_native_scheduled_job` whenever a commit is made into main.

## History and Changes
Code was updated to calculate SPOC inventory by position.

This pipeline was derived from [this notebook](https://colab.research.google.com/drive/1zP1e02wp-ufv0lAR0PdUddILwF-9k-YI#scrollTo=SW7oxckRn0ov).  The notebook uses tables associated with deprecated PingCentre data, which was replaced in this code with data from the Newtab tables.