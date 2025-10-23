# Code Location
"""All the base code for Scraping and API is within the 'poc' folder. All individual scraping and api pipelines are under poc/dagster/dagster_integration/scraping_pipelines folder. All these scripts are integrated into the vip_dagster_pipeline.py which the main file that launches dagster app for orchestration and maintaining the pipelines."""

# Launching Dagster
Launch dagster by navigating to poc/dagster/dagster_integration and running "dagster dev -m vip_dagster_pipeline"

# dagster_pipeline_template

- rename entrypoint

# Using uv for modern package management
you could also use Poetry, pyenv, or pipx. uv is preferred because it is blazing fast and has 

### Download uv
``` bash
pip install uv # install uv
```
for more info, see https://docs.astral.sh/uv/getting-started/installation/

### Initialize the project
if you were working from scratch you would use the following command to initialize a project and create a blank `pyproject.toml`
``` bash
uv init
```
however here we have already don so. instead you can load the existing template hosted on git.
``` bash
git clone https://github.com/RevealGC/dagster_pipeline_template.git
uv run hello.py # install project into .venv and run hello.py
```

### Add and remove packages and python versions
additional packages can be added and removed. 
``` bash
uv add requests
# or uv remove requests
```
after adding or removing packages, it's important to syncronize the changes with the `uv.lock` file:
``` bash
uv sync
```
``` bash
uv lock --upgrade-package requests
```
``` bash
uv python install 3.12


```
