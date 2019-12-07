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

from dagster import execute_pipeline, pipeline, ModeDefinition, String, Dict
from menu import Menu
import os.path as path

from db_toolkit.misc import (
    get_dir_path,
    get_file_path,
    test_file_path,
    load_yaml
)
from pipelines import (
    execute_csv_to_postgres_pipeline,
    execute_postgres_to_plot_pipeline,
    execute_file_ip_postgres_to_plot_pipeline,
    execute_create_sales_data_postgres_pipeline,
    execute_clean_sales_data_postgres_pipeline,
)

"""
"""


def get_user_input(prompt: str, current_setting: str):
    """
    Get user input
    :param prompt: prompt to display
    :param current_setting: current value
    :return:
    """
    if current_setting != '':
        print(f'-- Current setting: {current_setting}')
        use_current = '/return to use current'
    else:
        use_current = ''
    user_ip = ''
    while user_ip == '':
        user_ip = input(f'{prompt} [q to quit{use_current}]: ')
        if user_ip.lower() == 'q':
            break
        if user_ip == '' and current_setting != '':
            user_ip = current_setting
    return user_ip


def interactive_plot(sj_config: dict, plotly_config: String, postgres_warehouse_resrc: Dict):
    """
    Process interactive plot
    :param sj_config: app configuration
    :param plotly_config: plotly configuration
    :param postgres_warehouse_resrc: postgres server resource
    """
    plot_cfg_path = ''
    plot_name = ''
    loop = True
    while loop:
        entering = True
        while entering:
            plot_cfg_path = get_user_input('Enter path to plot configuration file', plot_cfg_path)
            loop = plot_cfg_path.lower() != 'q'
            if loop:
                if not path.exists(plot_cfg_path):
                    print(f'>> Invalid path: {plot_cfg_path}')
                    plot_cfg_path = ''
                elif not test_file_path(plot_cfg_path):
                    print(f'>> Not a file path: {plot_cfg_path}')
                    plot_cfg_path = ''
                else:
                    entering = False
            else:
                break

        if loop:
            entering = True
            while entering:
                plot_name = get_user_input('Enter plot name', plot_name)
                loop = plot_name.lower() != 'q'
                if loop:
                    execute_file_ip_postgres_to_plot_pipeline(sj_config, plotly_config, postgres_warehouse_resrc,
                                                              plot_cfg_path, plot_name)
                entering = False


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

    def call_execute_interactive_plot_pipeline():
        interactive_plot(app_cfg['sales_journal'], plotly_cfg, postgres_warehouse)

    def call_execute_create_sales_data_postgres_pipeline():
        execute_create_sales_data_postgres_pipeline(app_cfg['sales_journal'], postgres_warehouse)

    def call_execute_clean_sales_data_postgres_pipeline():
        execute_clean_sales_data_postgres_pipeline(app_cfg['sales_journal'], postgres_warehouse)


    pipeline = 'menu'
    if 'pipeline' in app_cfg['sales_journal']:
        pipeline = app_cfg['sales_journal']['pipeline'].lower()
    if pipeline == 'menu':
        menu = Menu()
        menu.set_options([
            ("Upload sales data to Postgres", call_execute_csv_to_postgres_pipeline),
            ("Plot entitybaseamount_by_salesdate", call_execute_postgres_to_plot_pipeline),
            ("Interactive plot", call_execute_interactive_plot_pipeline),
            ("Create sales data tables in Postgres", call_execute_create_sales_data_postgres_pipeline),
            ("Clean sales data tables in Postgres", call_execute_clean_sales_data_postgres_pipeline),
            ("Exit", Menu.CLOSE)
        ])
        menu.set_title("SalesJournal Data Processing Menu")
        menu.set_title_enabled(True)
        menu.open()
    elif pipeline == 'csv_to_postgres_pipeline':
        call_execute_csv_to_postgres_pipeline()
    elif pipeline == 'postgres_to_plot_pipeline':
        call_execute_postgres_to_plot_pipeline()
    elif pipeline == 'create_sales_data_postgres_pipeline':
        call_execute_create_sales_data_postgres_pipeline()
    elif pipeline == 'clean_sales_data_postgres_pipeline':
        call_execute_clean_sales_data_postgres_pipeline()
    elif pipeline == 'interactive_plot_pipeline':
        call_execute_interactive_plot_pipeline()


