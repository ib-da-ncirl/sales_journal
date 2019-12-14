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

from dagster import (
    execute_pipeline,
    pipeline,
    ModeDefinition,
    Dict,
    String,
)
from dagster_toolkit.postgres import (
    postgres_warehouse_resource,
    query_table,
)
from dagster_toolkit.environ import (
    EnvironmentDict,
)
from sales_journal.plot import (
    initialise_plot,
    transform_plot_data,
    process_plot,
    process_sql_plot
)
import pprint


@pipeline(
    mode_defs=[
        ModeDefinition(
            # attach resources to pipeline
            resource_defs={
                'postgres_warehouse': postgres_warehouse_resource,
            }
        )
    ]
)
def sql_to_plot_pipeline():
    """
    Definition of the pipeline to plot
    """


    # plot_config = initialise_plot()
    plot_config, plot_sql = initialise_plot()

    process_sql_plot(plot_config)


def execute_sql_to_plot_pipeline(sj_config: Dict, plotly_cfg: String, postgres_warehouse: Dict, plot_name: String):
    """
    Execute the pipeline to create a plot
    :param sj_config: app configuration
    :param plotly_cfg: plotly configuration
    :param postgres_warehouse: postgres server resource
    :param plot_name: name of plot to produce
    """
    execute_file_ip_sql_to_plot_pipeline(sj_config, plotly_cfg, postgres_warehouse,
                                              sj_config['plots_cfg'], plot_name)


def execute_file_ip_sql_to_plot_pipeline(sj_config: Dict, plotly_cfg: String, postgres_warehouse: Dict,
                                              plot_cfg_path: String, plot_name: String):
    """
    Execute the pipeline to create a plot
    :param sj_config: app configuration
    :param plotly_cfg: plotly configuration
    :param postgres_warehouse: postgres server resource
    :param plot_cfg_path: path to plot configuration file
    :param plot_name: name of plot to produce
    """
    # environment dictionary
    env_dict = EnvironmentDict() \
        .add_solid_input('initialise_plot', 'yaml_path', plot_cfg_path) \
        .add_solid_input('initialise_plot', 'plot_name', plot_name) \
        .add_solid_input('process_sql_plot', 'plotly_cfg', plotly_cfg) \
        .add_resource('postgres_warehouse', postgres_warehouse) \
        .build()

    pp = pprint.PrettyPrinter(indent=2)
    pp.pprint(env_dict)

    result = execute_pipeline(sql_to_plot_pipeline, environment_dict=env_dict)
    assert result.success


@pipeline(
    mode_defs=[
        ModeDefinition(
            # attach resources to pipeline
            resource_defs={
                'postgres_warehouse': postgres_warehouse_resource,
            }
        )
    ]
)
def dataframe_to_plot_pipeline():
    """
    Definition of the pipeline to plot
    """

    # plot_config = initialise_plot()
    plot_config, plot_sql = initialise_plot()

    process_plot(
        transform_plot_data(
            query_table(plot_sql),
            plot_config
        ),
        plot_config
    )


def execute_postgres_to_plot_pipeline(sj_config: Dict, plotly_cfg: String, postgres_warehouse: Dict, plot_name: String):
    """
    Execute the pipeline to create a plot
    :param sj_config: app configuration
    :param plotly_cfg: plotly configuration
    :param postgres_warehouse: postgres server resource
    :param plot_name: name of plot to produce
    """
    execute_file_ip_postgres_to_plot_pipeline(sj_config, plotly_cfg, postgres_warehouse,
                                              sj_config['plots_cfg'], plot_name)


def execute_file_ip_postgres_to_plot_pipeline(sj_config: Dict, plotly_cfg: String, postgres_warehouse: Dict,
                                              plot_cfg_path: String, plot_name: String):
    """
    Execute the pipeline to create a plot
    :param sj_config: app configuration
    :param plotly_cfg: plotly configuration
    :param postgres_warehouse: postgres server resource
    :param plot_cfg_path: path to plot configuration file
    :param plot_name: name of plot to produce
    """
    # environment dictionary
    env_dict = EnvironmentDict() \
        .add_solid_input('initialise_plot', 'yaml_path', plot_cfg_path) \
        .add_solid_input('initialise_plot', 'plot_name', plot_name) \
        .add_solid('query_table') \
        .add_solid_input('process_plot', 'plotly_cfg', plotly_cfg) \
        .add_resource('postgres_warehouse', postgres_warehouse) \
        .build()

    pp = pprint.PrettyPrinter(indent=2)
    pp.pprint(env_dict)

    result = execute_pipeline(dataframe_to_plot_pipeline, environment_dict=env_dict)
    assert result.success

