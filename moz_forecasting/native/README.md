# Native Forecast

## Overview
This flow forecasts the number of newtab impressions for which sponsored stories are enabled.

The output table is indexed by country, month (`submission_month`), with the forecast in the `native_forecast` column

Currently the data only runs on Firefox desktop

### `inventory_forecast` derivation
The idea behind the inventory forecast is to execute the following steps:
- **get by-country dau forecast**: This is done by running monthly cumulative DAU since 2019-01 through the darts `StatsForecastAutoARIMA` model by country.  The  `season_length` is set to 12 (to represent yearly seasonality) but all other parameters are set to the default values.  More details on this object can be found [here](https://unit8co.github.io/darts/generated_api/darts.models.forecasting.sf_auto_arima.html).
- **turn that into country-level newtab impressions with sponsored stories by multiplying a factor representing the ratio of impressions to the dau by country**: The country-level inventory by month is obtained from the `mozdata.telemetry.newtab_clients_daily` table.  Impressions are counted by month and country when the `pocket_sponsored_stories_enabled` flag is true.  The ratio is calculated by month and rolled up to the country level by averaging over one year.  It is first stored in `self.impressions_to_spoc["ratio_newtab_impressions_with_spocpocket_to_dou"]`.

## History and Changes
This pipeline was derived from [this notebook](https://colab.research.google.com/drive/1zP1e02wp-ufv0lAR0PdUddILwF-9k-YI#scrollTo=SW7oxckRn0ov).  The notebook uses outdated tables so the flow replicated the logic without explicilty following it.  It also uses the kpi global forcast rather than creating a bespoke, country-level forecast.