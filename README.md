# moz-forecasting
Forecasting Framework for Mozilla

## Overview
Each forecast is executed via a metaflow flow in the `flows` directory.  Currently supported forecasts are:
- ad tiles forecast (ad_tiles_forecast.py)

There are additionally helper functions in (TBD when kpi_forecasting branch gets merged)

## Running Flows
### Local Machine

This project uses [uv](https://docs.astral.sh/uv/) for project and dependency management.  To be able to use it you'll need to have it installed on the system you're using.  This is very easy to do with pip (`pip install uv`).  Once it's installed, you can install the new dependencies in a virtualenv with `uv sync` in the root directory of the project.  This will create a new virtual environment in a new `.venv` directory.

Once the virtualenv is set up, a pipeline can be run locally with `uv run <PATH TO FLOW FILE> run`

#### Running on local after setting up outerbounds
When you set up outerbounds (see next section), a new metaflow config file is created.  This means you'll be using Outerbounds' perimeters for authentication by default from then on.  To use your local authentication, you want to make sure there is a file at `~/.metaflowconfig/config_local.json` that only has an empty json object in it (this might be created by default, if not you can create it yourself). You can then run with this local profile by doing: `METAFLOW_PROFILE=local uv run flows/ad_tiles_forecast.py run`.  See: (https://outerbounds.com/docs/use-multiple-metaflow-configs/)

### On Outerbounds
[Outerbounds](https://ui.desertowl.obp.outerbounds.com/dashboard/workspace) is used to run metaflow flows in the cloud.  The code is run with a docker image via [kubernetes](https://outerbounds.com/engineering/deployment/gcp-k8s/deployment/).  Currently an image that works with uv and metaflow can be found [here](https://hub.docker.com/repository/docker/jsnydermoz/moz-forecasting/general)  (TODO: build and push image in CI, have it go to mozilla GCR rather than Jared's dockerhub). 

If you are new to Outerbounds, you'll need to be added to the `revenue` permimeter (TODO: ask Chelsea how to add new people.  If you are reading this you likely already have access).  You will also need to configure metaflow to use Outerbounds.  Instructions for doing this can be found in the [mlops template repo README](github.com/mozilla/mozmlops/tree/main/src/mozmlops/templates#most-importantly-you-need-an-account-with-outerbounds-do-not-make-this-yourself)

Once this is set up, the whole pipeline can be run in Outerbounds from the command linke using the `--with kubernetes` flag like:
```uv run <PATH TO FLOW FILE> run --with kubernetes:image=registry.hub.docker.com/jsnydermoz/moz-forecasting:latest```

One nice feature of metaflow is that a specific step can be configured to run in the cloud.  This is done via the `@kubernetes` decorator.  As with the command line argument, the docker image needs to be specified.  It would go before the step decorator and would look somethign like `@kubernetes(image="registry.hub.docker.com/jsnydermoz/moz-forecasting:latest", cpu=1)`

## Development
### Tests
Tests can be found in the `tests` directory and run with `uv run pytest`

### Linting
Linting is done via ruff.  In the CI it is currently pinned to version 0.6.5.  It does not need to be added as a dependency to the project and can be run as a tool via `uv ruff@0.6.5 check` and `uv ruff@0.6.5 format`

Note that docstrings are included in the rules and must be in numpy format.

### CI
Linting and Testing is run via circleci

### Docker
TODO: move this to CI
Currently built locally and pushed to docker hub. Needs to be built for linux to work on outerbounds:
`docker build -t  jsnydermoz/moz-forecasting .  --platform=linux/amd64`