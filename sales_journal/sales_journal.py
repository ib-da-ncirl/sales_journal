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
import yaml
from dagster import String, Dict
from menu import Menu
import os.path as path
import sys
import getopt
from collections import namedtuple

from db_toolkit.misc import (
    get_file_path,
    test_file_path,
    load_yaml
)
from sales_journal.pipelines import (
    execute_csv_to_postgres_pipeline,
    execute_csv_currency_to_postgres_pipeline,
    execute_file_ip_sql_to_plot_pipeline,
    execute_create_sales_data_postgres_pipeline,
    execute_create_currency_data_postgres_pipeline,
    execute_clean_sales_data_postgres_pipeline,
    execute_clean_currency_data_postgres_pipeline,
    execute_currency_to_postgres_pipeline,
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
    plot_cfg_path = sj_config['plots_cfg']
    plot_name = ''

    # dev HACK
    # if 'interactive_plots_cfg' in sj_config.keys():
    #     plot_cfg_path = sj_config['interactive_plots_cfg']
    # else:
    #     plot_cfg_path = ''
    # if 'interactive_plot_name' in sj_config.keys():
    #     plot_name = sj_config['interactive_plot_name']
    # else:
    #     plot_name = ''

    loop = True
    while loop:
        entering = True
        while entering:
            if len(plot_cfg_path) == 0:
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
            plots_config = load_yaml(plot_cfg_path)
            print(f'The following plots are available:')
            width = 0
            details = []
            for name in plots_config.keys():
                title = ''
                cfg = plots_config[name]
                if 'title' in cfg.keys():
                    title = cfg['title']
                    if isinstance(title, dict):
                        cfg = title
                        if 'text' in cfg.keys():
                            title = cfg['text']
                if len(name) > width:
                    width = len(name)
                details.append((name, title))

            fmt = f'  \x7b0:<{width}\x7d -- \x7b1\x7d'
            for name, title in details:
                print(fmt.format(name, title))

            entering = True
            while entering:
                plot_name = get_user_input('Enter plot name', plot_name)
                loop = plot_name.lower() != 'q'
                if loop:
                    try:
                        plots_config = load_yaml(plot_cfg_path, plot_name)

                        execute_file_ip_sql_to_plot_pipeline(sj_config, plotly_config, postgres_warehouse_resrc,
                                                             plot_cfg_path, plot_name)
                        # execute_file_ip_postgres_to_plot_pipeline(sj_config, plotly_config, postgres_warehouse_resrc,
                        #                                           plot_cfg_path, plot_name)
                        entering = False
                    except yaml.parser.ParserError as pe:
                        print(f'>> Error in configuration: {pe}')
                    except KeyError as ke:
                        print(f">> Error in configuration: plot '{plot_name}' not found")
                        plot_name = ''
                else:
                    break


def get_config_options():
    ConfigOpt = namedtuple('ConfigOpt', ['short', 'long', 'desc'])
    __OPTS = {
        'h': ConfigOpt('h', 'help', 'Display usage'),
        'c': ConfigOpt('c:', 'cfg_path=', 'Specify path to configuration script'),
        'o': ConfigOpt('o:', 'plotly_exe=', 'Specify path to the plotly orca executable'),
        'p': ConfigOpt('p:', 'plot_path=', 'Specify path to plots configuration'),
        'n': ConfigOpt('n:', 'plot_name=', 'Specify plot to render from plots script'),
        's': ConfigOpt('s:', 'file_set=', 'Comma-separated list of file set(s) to load for processing'),
        'd': ConfigOpt('d:', 'data_dir=', 'Directory from which to load file set(s) for processing'),
        'm': ConfigOpt('m:', 'mode=', 'File set processing mode; normal (run once) or loop (run until all file sets '
                                      'processed'),
    }
    return __OPTS;


def get_short_opts() -> str:
    opts_lst = ''
    options = get_config_options()
    for o_key in options.keys():
        opts_lst += options[o_key].short
    return opts_lst


def get_long_opts() -> list:
    opts_lst = []
    options = get_config_options()
    for o_key in options.keys():
        if options[o_key].long is not None:
            opts_lst.append(options[o_key].long)
    return opts_lst


def get_short_opt(o_key) -> str:
    short_opt = ''
    options = get_config_options()
    if o_key in options.keys():
        short_opt = '-' + options[o_key].short
        if short_opt.endswith(':'):
            short_opt = short_opt[:-1]
    return short_opt


def get_long_opt(o_key) -> str:
    long_opt = ''
    options = get_config_options()
    if o_key in options.keys():
        long_opt = '--' + options[o_key].long
        if long_opt.endswith('='):
            long_opt = long_opt[:-1]
    return long_opt


def usage(name):
    print(f'Usage: {name}')
    options = get_config_options()
    for o_key in options:
        opt_info = options[o_key]
        if opt_info.short.endswith(':'):
            short_opt = opt_info.short + '<value>'
        else:
            short_opt = opt_info.short
        if opt_info.long.endswith('='):
            long_opt = opt_info.long + '<value>'
        else:
            long_opt = opt_info.long
        print(f' -{short_opt:10.10s}|--{long_opt:18.18s} : {opt_info.desc}')
    print()


def get_app_config(name, args):
    try:
        opts, args = getopt.getopt(args, get_short_opts(), get_long_opts())
    except getopt.GetoptError as err:
        print(err)
        usage(name)
        sys.exit(2)

    app_cfg_path = '../config.yaml'  # default in project root
    cmd_line_args = {
        'o': None,
        'p': None,
        'p': None,
        's': None,
        'd': None,
        'm': None
    }
    for opt, arg in opts:
        if opt == get_short_opt('h') or opt == get_long_opt('h'):
            usage(name)
            sys.exit()
        elif opt == get_short_opt('c') or opt == get_long_opt('c'):
            app_cfg_path = arg
        else:
            for key in ['o', 'p', 'n', 's', 'd', 'm']:
                if opt == get_short_opt(key) or opt == get_long_opt(key):
                    cmd_line_args[key] = arg
                    break

    # get path to config file
    if not test_file_path(app_cfg_path):
        # no default so look for in environment or from console
        app_cfg_path = get_file_path('SJ_CFG', 'SalesJournal configuration file')
        if app_cfg_path is None:
            exit(0)

    # load app config
    app_cfg = load_yaml(app_cfg_path)

    if app_cfg is not None:
        # check some basic configs exist
        for key in ['sales_journal', 'postgresdb']:  # required root level keys
            if key not in app_cfg.keys():
                raise EnvironmentError(f'Missing {key} configuration key')
    else:
        raise EnvironmentError(f'Missing configuration')

    sj_config = app_cfg['sales_journal']

    # get plotly config
    plotly_cfg = None
    if cmd_line_args['o'] is None:
        cfg = app_cfg
        for key in ['plotly', 'orca', 'executable']:
            if key in cfg.keys():
                cfg = cfg[key]
                if key == 'executable':
                    plotly_cfg = cfg
    # TODO disabled cmd line args for now not fully tested
    # else:
    #     app_cfg['plotly']['orca']['executable'] = cmd_line_args['o']
    #
    # if cmd_line_args['n'] is not None:
    #     sj_config['plot_name'] = cmd_line_args['n']
    if cmd_line_args['p'] is not None:
        sj_config['plots_cfg'] = cmd_line_args['p']
    # if cmd_line_args['s'] is not None:
    #     sj_config['load_file_sets'] = cmd_line_args['s']
    # if cmd_line_args['d'] is not None:
    #     sj_config['db_data_path'] = cmd_line_args['d']
    # if cmd_line_args['m'] is not None:
    #     sj_config['csv_pipeline_run_mode'] = cmd_line_args['m']

    return app_cfg, plotly_cfg


def make_call_execute_csv_to_postgres_pipeline(sj_cfg, postgres_warehouse):
    def call_execute_csv_to_postgres_pipeline():
        execute_csv_to_postgres_pipeline(sj_cfg, postgres_warehouse)
    return call_execute_csv_to_postgres_pipeline


def make_call_execute_csv_currency_to_postgres_pipeline(sj_cfg, postgres_warehouse):
    def call_execute_csv_currency_to_postgres_pipeline():
        raise NotImplementedError('All plots functionality is not yet fully supported')
        execute_csv_currency_to_postgres_pipeline(sj_cfg, postgres_warehouse)
    return call_execute_csv_currency_to_postgres_pipeline


def make_call_execute_interactive_plot_pipeline(sj_cfg, plotly_cfg, postgres_warehouse):
    def call_execute_interactive_plot_pipeline():
        interactive_plot(sj_cfg, plotly_cfg, postgres_warehouse)
    return call_execute_interactive_plot_pipeline


def make_call_execute_currency_to_postgres_pipeline(sj_cfg, postgres_warehouse):
    def call_execute_currency_to_postgres_pipeline():
        execute_currency_to_postgres_pipeline(sj_cfg, postgres_warehouse)
    return call_execute_currency_to_postgres_pipeline


def make_call_execute_create_sales_data_postgres_pipeline(sj_cfg, postgres_warehouse):
    def call_execute_create_sales_data_postgres_pipeline():
        execute_create_sales_data_postgres_pipeline(sj_cfg, postgres_warehouse)
    return call_execute_create_sales_data_postgres_pipeline


def make_call_execute_create_currency_data_postgres_pipeline(cur_cfg, postgres_warehouse):
    def call_execute_create_currency_data_postgres_pipeline():
        execute_create_currency_data_postgres_pipeline(cur_cfg, postgres_warehouse)
    return call_execute_create_currency_data_postgres_pipeline


def make_call_execute_clean_sales_data_postgres_pipeline(sj_cfg, postgres_warehouse):
    def call_execute_clean_sales_data_postgres_pipeline():
        execute_clean_sales_data_postgres_pipeline(sj_cfg, postgres_warehouse)
    return call_execute_clean_sales_data_postgres_pipeline


def make_call_execute_clean_currency_data_postgres_pipeline(cur_cfg, postgres_warehouse):
    def call_execute_clean_currency_data_postgres_pipeline():
        execute_clean_currency_data_postgres_pipeline(cur_cfg, postgres_warehouse)
    return call_execute_clean_currency_data_postgres_pipeline


def main():

    # load app config
    (app_cfg, plotly_cfg) = get_app_config(sys.argv[0], sys.argv[1:])

    sj_config = app_cfg['sales_journal']

    # resource entries for environment_dict
    postgres_warehouse = {'config': {'postgres_cfg': app_cfg['postgresdb']}}

    call_execute_csv_to_postgres_pipeline = \
        make_call_execute_csv_to_postgres_pipeline(sj_config, postgres_warehouse)

    call_execute_csv_currency_to_postgres_pipeline = \
        make_call_execute_csv_currency_to_postgres_pipeline(sj_config, postgres_warehouse)

    call_execute_interactive_plot_pipeline = \
        make_call_execute_interactive_plot_pipeline(sj_config, plotly_cfg, postgres_warehouse)

    call_execute_currency_to_postgres_pipeline = \
        make_call_execute_currency_to_postgres_pipeline(sj_config, postgres_warehouse)

    call_execute_create_sales_data_postgres_pipeline = \
        make_call_execute_create_sales_data_postgres_pipeline(sj_config, postgres_warehouse)

    call_execute_create_currency_data_postgres_pipeline = \
        make_call_execute_create_currency_data_postgres_pipeline(sj_config['currency'], postgres_warehouse)

    call_execute_clean_sales_data_postgres_pipeline = \
        make_call_execute_clean_sales_data_postgres_pipeline(sj_config, postgres_warehouse)

    call_execute_clean_currency_data_postgres_pipeline = \
        make_call_execute_clean_currency_data_postgres_pipeline(sj_config['currency'], postgres_warehouse)


    pipeline = 'menu'
    if 'pipeline' in sj_config:
        pipeline = sj_config['pipeline'].lower()
    if pipeline == 'menu':
        menu = Menu()
        menu.set_options([
            ("Upload sales data to Postgres", call_execute_csv_to_postgres_pipeline),
            # ("Upload sales data with currency to Postgres", call_execute_csv_currency_to_postgres_pipeline),
            ("Interactive plot", call_execute_interactive_plot_pipeline),
            ("Upload currency data to Postgres", call_execute_currency_to_postgres_pipeline),
            ("Create sales data tables in Postgres", call_execute_create_sales_data_postgres_pipeline),
            ("Clean sales data tables in Postgres", call_execute_clean_sales_data_postgres_pipeline),
            ("Create currency data tables in Postgres", call_execute_create_currency_data_postgres_pipeline),
            ("Clean currency data tables in Postgres", call_execute_clean_currency_data_postgres_pipeline),
            ("Exit", Menu.CLOSE)
        ])
        menu.set_title("SalesJournal Data Processing Menu")
        menu.set_title_enabled(True)
        menu.open()
    elif pipeline == 'csv_to_postgres_pipeline':
        call_execute_csv_to_postgres_pipeline()
    elif pipeline == 'csv_currency_to_postgres_pipeline':
        call_execute_csv_currency_to_postgres_pipeline()
    elif pipeline == 'interactive_plot_pipeline':
        call_execute_interactive_plot_pipeline()
    elif pipeline == 'currency_to_postgres_pipeline':
        call_execute_currency_to_postgres_pipeline()
    elif pipeline == 'create_sales_data_postgres_pipeline':
        call_execute_create_sales_data_postgres_pipeline()
    elif pipeline == 'clean_sales_data_postgres_pipeline':
        call_execute_clean_sales_data_postgres_pipeline()
    elif pipeline == 'create_currency_data_postgres_pipeline':
        call_execute_create_currency_data_postgres_pipeline()
    elif pipeline == 'clean_currency_data_postgres_pipeline':
        call_execute_clean_currency_data_postgres_pipeline()


if __name__ == '__main__':
    main()
