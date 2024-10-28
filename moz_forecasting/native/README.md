# Native Forecast

## Overview
This flow forecasts the number of newtab impressions for which sponsored stories are enabled.

The output table is indexed by country, month (`submission_month`), with the forecast in the `native_forecast` column

### `inventory_forecast` derivation
The idea behind the inventory forecast is to execute the following steps:
- **get by-country dau forecast**: The basis of the forecast is the global dau forecast in `moz-fx-data-shared-prod.telemetry_derived.kpi_forecasts_v0`.  It is turned into a country-level forecast by multiplying on two factors, the `share_by_market` and the `elgibility_factor`.  Both factors are calculated at the monthly level for each country.  The share by market for a country is the number of daily users in that country divided by the total users in that month.  The `eligiblility_factor` is the number of elgibile users within a country divided by the total number of active users within that country.  The elgibility is specified for each product (IE tiles) individually for mobile and desktop as a SQL function in the config file for each flow.  It is determined by which app version that the product became available in each country.
- **turn that into country-level newtab impressions with sponsored stories by multiplying a factor representing the ratio of impressions to the dau by country**: The country-level inventory by month is obtained from the `mozdata.telemetry.newtab_clients_daily` table.  Impressions are counted by month and country when the `pocket_sponsored_stories_enabled` flag is true.  The ratio is calculated by month and rolled up to the country level by averaging over one year.  It is first stored in `self.impressions_to_spoc["ratio_newtab_impressions_with_spocpocket_to_dou"]`.

## History and Changes
This pipeline was derived from [this notebook](https://colab.research.google.com/drive/1zP1e02wp-ufv0lAR0PdUddILwF-9k-YI#scrollTo=SW7oxckRn0ov).  The notebook uses outdated tables so the flow replicated the logic without explicilty following it.  It also uses the kpi global forcast rather than creating a bespoke, country-level forecast.