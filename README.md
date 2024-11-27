# moz-forecasting
Forecasting Framework for Mozilla

## Overview
Each forecast is executed via a metaflow flow in the `flows` directory.  Currently supported forecasts are:
- Ads Tiles Revenue Forecast (ad_tiles_forecast)

There are additionally helper functions in (TBD when kpi_forecasting branch gets merged)

## Running Flows
### Local Machine

This project uses [uv](https://docs.astral.sh/uv/) for project and dependency management.  To be able to use it you'll need to have it installed on the system you're using.  This is very easy to do with pip (`pip install uv`).  Once it's installed, you can install the new dependencies in a virtualenv with `uv sync` in the root directory of the project.  This will create a new virtual environment in a new `.venv` directory.

To use darts, there some additionaly dependencies you may need.  For example, installing libomp on a mac with homebrew with `brew install libomp`

Once the virtualenv is set up, a pipeline can be run locally with `uv run <PATH TO FLOW FILE> run`

To run locally the `GCP_PROJECT_NAME` environment variable must be set to a profile that the user can create a client from. For most people data scientists `mozdata` will work.  This can be set via an rc file with the command `export GCP_PROJECT_NAME=mozdata`, or it can be put in front of the command to run the job like `METAFLOW_PROFILE=local GCP_PROJECT_NAME=mozdata uv run flows/ad_tiles_forecast.py run`

To run a notebook that can access the local metaflow, put `%env METAFLOW_PROFILE=local` in a cell **before** loading the metaflow package.

#### Running on local after setting up outerbounds
When you set up outerbounds (see next section), a new metaflow config file is created.  This means you'll be using Outerbounds' perimeters for authentication by default from then on.  To use your local authentication, you want to make sure there is a file at `~/.metaflowconfig/config_local.json` that only has an empty json object in it (this might be created by default, if not you can create it yourself). You can then run with this local profile by doing: `METAFLOW_PROFILE=local uv run flows/ad_tiles_forecast.py run`.  See: (https://outerbounds.com/docs/use-multiple-metaflow-configs/)

### On Outerbounds
[Outerbounds](https://ui.desertowl.obp.outerbounds.com/dashboard/workspace) is used to run metaflow flows in the cloud.  The code is run with a docker image via [kubernetes](https://outerbounds.com/engineering/deployment/gcp-k8s/deployment/).  An image is created by circleci and stored in GCR for use in Outerbounds at `us-docker.pkg.dev/moz-fx-mfouterbounds-prod-f98d/mfouterbounds-prod/moz-forecasting`.  See the `Development.Docker` section below for more details.

If you are new to Outerbounds, you'll need to be added to the `revenue` permimeter.  If you do not have access, reach out to Chelsea Troy or another member of the MLOps team. You can see which perimeters you have access to with the `outerbounds perimeter list` command. You will also need to configure metaflow to use Outerbounds.  Instructions for doing this can be found in the [mlops template repo README](github.com/mozilla/mozmlops/tree/main/src/mozmlops/templates#most-importantly-you-need-an-account-with-outerbounds-do-not-make-this-yourself)

Once this is set up, make sure you are in the `revenue` perimeter locally with the command `outerbounds perimeter switch --id revenue`.  The whole pipeline can then be run in Outerbounds from the command line using the `--with kubernetes` flag like:
```uv run <PATH TO FLOW FILE> run --with kubernetes:image=us-docker.pkg.dev/moz-fx-mfouterbounds-prod-f98d/mfouterbounds-prod/moz-forecasting:latest```

One nice feature of metaflow is that a specific step can be configured to run in the cloud.  This is done via the `@kubernetes` decorator.  As with the command line argument, the docker image needs to be specified.  It would go before the step decorator and would look somethign like `@kubernetes(image="us-docker.pkg.dev/moz-fx-mfouterbounds-prod-f98d/mfouterbounds-prod/moz-forecasting:latest", cpu=1)`

### Backfills
Backfills can be executed via `backfill.py`.  It is run via `uv run` from the root of the project. The script iterates over all the months between `start_month` and `end_month` inclusive, setting `forecast_month` to each month and running the pipeline.  It will raise an error if you attempt to write and at least one of the months included already has data (meaning that there is a `forecast_month` in the table equal to the month you are including). It is run from the root directly with the following arguments:
- test_mode: whether or not to write to the test or prod table.  This also controls whether it runs locall (False) or in Outerbounds (True)
- start_month: the earliest month to include in the backfill
- end_month: the latest month to include in the backfill.  
- config: path to the config file from the root of the project.  This is required so that the output table can be checked
- flow: path to the flow file from the root of the project

An example of the command to run the `ad_tiles_forecast` pipeline for only `2024-02`: `uv run backfill.py  --start_month=2024-02 --end_month=2024-02 --config=moz_forecasting/ad_tiles_forecast/config_2025_planning_baseline.yaml  --flow=moz_forecasting/ad_tiles_forecast/flow.py`


## Development
### Tests
Tests can be found in the `tests` directory and run with `uv run pytest`

### Linting
Linting is done via ruff.  In the CI it is currently pinned to version 0.6.5.  It does not need to be added as a dependency to the project and can be run as a tool via `uv ruff@0.6.5 check` and `uv ruff@0.6.5 format`

Note that docstrings are included in the rules and must be in numpy format.

### CI
Linting and Testing is run via circleci

### Docker
A docker image is built for this project and is used by Outerbounds to run it.  The image is created by the CI and is only accessible to the service account associated with this project, `moz-fx-mfouterbounds-prod-f98d`.  The image is stored in GCR, which the CI interacts through via the `gcp-gcr` orb.  The image is only updated when a commit is made on the `main` branch, which (due to the repo's branch protection rules) will only happen when a PR gets merged.

### Adding new flows
New flows should be added to a directory under the `moz_forecasting` project directory.  Each flow should read in a config file which should parameterize as much about the flow as possible.  In order to be compatible with the backfill code in `backfill.py`,
the config should have an `output` section containing `test` and `prod` sections, each specifying the `table`, `dataset` and `project` associated with the output using keys with those names.  The output dataset should have a `product` column that matches the products listed under the `product` key in the config.  For example:
```
# specify values used for product column
product:
  - "tile"
  - "tile direct sales"
# specify the output dataset and table
# to write the data to
output:
  test:
    project: moz-fx-data-bq-data-science
    dataset: jsnyder
    table: amp_rpm_forecasts
  prod:
    project: moz-fx-data-shared-prod
    dataset: ads_derived
    table: tiles_monthly_v1
```