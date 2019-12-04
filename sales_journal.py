# The MIT License (MIT)
# Copyright (c) 2019 Ian Buttimer

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from dagster import execute_pipeline, pipeline, ModeDefinition
from menu import Menu

from db_toolkit.misc import (
    get_dir_path,
    get_file_path,
    test_file_path,
    load_yaml
)
from pipelines import (
    execute_csv_to_postgres_pipeline,
    execute_postgres_to_plot_pipeline
)
from pipelines.clean_pipelines import execute_clean_sales_data_postgres_pipeline

"""
"""

if __name__ == '__main__':

    # get path to config file
    app_cfg_path = 'config.yaml'    # default in project root
    if not test_file_path(app_cfg_path):
        # no default so look for in environment or from console
        app_cfg_path = get_file_path('SJ_CFG', 'SalesJournal configuration file')
        if app_cfg_path is None:
            exit(0)

    app_cfg = load_yaml(app_cfg_path)

    if app_cfg is not None:
        # check some basic configs exist
        for key in ['sales_journal', 'postgresdb']:     # required root level keys
            if key not in app_cfg.keys():
                raise EnvironmentError(f'Missing {key} configuration key')
    else:
        raise EnvironmentError(f'Missing configuration')

    plotly_cfg = None
    cfg = app_cfg
    for key in ['plotly', 'orca', 'executable']:
        if key in cfg.keys():
            cfg = cfg[key]
            if key == 'executable':
                plotly_cfg = cfg

    # resource entries for environment_dict
    postgres_warehouse = {'config': {'postgres_cfg': app_cfg['postgresdb']}}

    def call_execute_csv_to_postgres_pipeline():
        execute_csv_to_postgres_pipeline(app_cfg['sales_journal'], postgres_warehouse)

    def call_execute_postgres_to_plot_pipeline():
        execute_postgres_to_plot_pipeline(app_cfg['sales_journal'], plotly_cfg, postgres_warehouse,
                                          'entitybaseamount_by_salesdate')

    def call_execute_clean_sales_data_postgres_pipeline():
        execute_clean_sales_data_postgres_pipeline(app_cfg['sales_journal'], postgres_warehouse)


    pipeline = app_cfg['sales_journal']['pipeline']
    if pipeline == 'menu':
        menu = Menu()
        menu.set_options([
            ("Upload sales data to Postgres", call_execute_csv_to_postgres_pipeline),
            ("Plot entitybaseamount_by_salesdate", call_execute_postgres_to_plot_pipeline),
            ("Clean sales_data from server", call_execute_clean_sales_data_postgres_pipeline),
            ("Exit", Menu.CLOSE)
        ])
        menu.set_title("SalesJournal Data Processing Menu")
        menu.set_title_enabled(True)
        menu.open()
    elif pipeline == 'csv_to_postgres_pipeline':
        call_execute_csv_to_postgres_pipeline()
    elif pipeline == 'postgres_to_plot_pipeline':
        call_execute_postgres_to_plot_pipeline()
    elif pipeline == 'clean_sales_data_postgres_pipeline':
        call_execute_clean_sales_data_postgres_pipeline()


