FROM python:3.10-slim-bullseye
# LABEL maintainer="Jared Snyder <jsnyder@mozilla.com>"

# # https://github.com/mozilla-services/Dockerflow/blob/master/docs/building-container.md
# ARG USER_ID="10001"
# ARG GROUP_ID="app"
# ARG HOME="/app"

# ENV HOME=${HOME}
# RUN groupadd --gid ${USER_ID} ${GROUP_ID} && \
#     useradd --create-home --uid ${USER_ID} --gid ${GROUP_ID} --home-dir ${HOME} ${GROUP_ID}

# # The installer requires curl (and certificates) to download the release archive
# RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates

# # Download the latest installer
# ADD https://astral.sh/uv/0.4.8/install.sh /uv-installer.sh

# # Run the installer then remove it
# RUN sh /uv-installer.sh && rm /uv-installer.sh

# # Ensure the installed binary is on the `PATH`
# ENV PATH="${HOME}/.cargo/bin/:$PATH"

# # Copy the project into the image
# ADD moz_forecasting /app/moz_forecasting
# ADD README.md /app
# ADD tests /app/tests
# ADD pyproject.toml /app
# ADD uv.lock /app

# # Sync the project into a new environment, using the frozen lockfile
# WORKDIR /app
# RUN uv sync --frozen

# # Drop root and change ownership of the application folder to the user
# RUN chown -R ${USER_ID}:${GROUP_ID} ${HOME}
# USER ${USER_ID}

# # put the virtualenv on the path
# ENV PATH="/app/.venv/bin:$PATH"
