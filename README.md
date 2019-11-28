# sales_journal

This project processes sales Journal data to Study of the added value brought to corporate customers by digital commerce platforms for travel retail 

[Dagster](https://dagster.readthedocs.io/) is utilised to:

* Extract the data from csv files, process it and upload to a Postgres server
* Generate data plots

## Installation
Please see https://packaging.python.org/tutorials/installing-packages/ for general information on installation methods.

The following packages are required:
* [dagster](https://github.com/dagster-io/dagster)
* [dagster-pandas](https://pypi.org/project/dagster-pandas/)
* [pandas](https://pandas.pydata.org/)
* [db_toolkit](https://github.com/ib-da-ncirl/db_toolkit)
* [dagster_toolkit](https://github.com/ib-da-ncirl/db_toolkit)
* [Menu](https://pypi.org/project/Menu/)

Install dependencies via

    pip install -r requirements.txt

## Setup

The application may be supplied by one of the following methods:
* Create a YAML configuration file named config.yaml, based on [sample.yaml](doc/sample.yaml), in the project root.
* Specify the path to the YAML configuration file, in the environment variable **SJ_CFG**.
* Enter the path to the YAML configuration file via the console.

## Execution

From the project root directory, in a terminal window run 

    python sales_journal.py