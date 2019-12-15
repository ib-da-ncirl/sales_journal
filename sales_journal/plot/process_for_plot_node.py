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
import os
from typing import Union

import pandas as pd
from dagster_pandas import DataFrame
from dagster import (
    solid,
    Field,
    String,
    List,
    Dict,
    OutputDefinition,
    Output,
    Any)
import plotly
import plotly.express as px
import plotly.graph_objects as go

from db_toolkit.misc import test_dir_path, test_file_path


@solid
def transform_plot_data(context, df: DataFrame, plot_config: Dict) -> Dict:
    """
    Perform any necessary transformations on the plot data
    :param context: execution context
    :param df: pandas DataFrame of plot data
    :param plot_config: dict of plot configurations
    :return: dict of pandas DataFrame of plot data
    """
    plot_info = {}
    if 'all' in plot_config.keys():
        plot_details = plot_config['all']
        raise NotImplementedError('All plots functionality is not yet fully supported')
    else:
        plot_details = plot_config

    for plot_key in plot_details.keys():
        plot_cfg = plot_details[plot_key]
        df.columns = plot_cfg['header']  # add the column names to the dataframe

        for column in plot_cfg['header']:
            if column in plot_cfg.keys():
                column_config = plot_cfg[column]
                if 'to_datetime' in column_config.keys():
                    # convert to a date time
                    df[column] = pd.to_datetime(df[column], format=column_config['to_datetime'])

        plot_info[plot_key] = {
            'df': df,
            'config': plot_cfg
        }

    return plot_info


def get_config_key(config: Dict, *args):
    chk_val = None
    chk_dict = config
    for key in args:
        if key in chk_dict.keys():
            chk_val = chk_dict[key]
        else:
            chk_val = None
            break
    return chk_val


def config_key_is_true(config: Dict, *args):
    chk_val = get_config_key(config, *args)
    if not isinstance(chk_val, bool):
        chk_val = False
    return chk_val


def config_key_lower(config: Dict, *args):
    chk_val = get_config_key(config, *args)
    if isinstance(chk_val, str):
        chk_val = chk_val.lower()
    else:
        chk_val = None
    return chk_val


def pickle_cfg(plot_config: Dict, plot_name: String) -> dict:
    pickle_it = config_key_is_true(plot_config, 'pickle')
    clear_it = config_key_is_true(plot_config, 'pickle_clear')
    multiplot = get_config_key(plot_config, 'multiplot')

    pkl_name = f"{plot_name}.pkl"
    pickle_path = config_key_lower(plot_config, 'pickle_path')
    if pickle_path:
        if not test_dir_path(pickle_path):
            pickle_path = ''

        # generate a dict of pickle paths for a multiplot
        if multiplot:
            pickles = {}
            for trace in multiplot.keys():
                pkl_name = f"{plot_name}_{trace}.pkl"
                pickles[trace] = os.path.join(pickle_path, pkl_name)
            pkl_name = pickles

        else:   # or just a name for a single plot
            pkl_name = os.path.join(pickle_path, pkl_name)

    return {'pickle_it': pickle_it, 'clear_it': clear_it, 'multiplot': multiplot is not None, 'pkl_name': pkl_name}


def unpickle(pkl_cfg: Dict, plot_name: String) -> dict:
    pkl_name = pkl_cfg['pkl_name']
    if isinstance(pkl_name, str):
        to_process = {plot_name: pkl_name}
    else:
        to_process = pkl_name

    df_dict = {}
    for key, value in to_process.items():
        if pkl_cfg['clear_it']:
            if test_file_path(value):
                os.remove(value)
        if pkl_cfg['pickle_it']:
            if test_file_path(value):
                df = pd.read_pickle(value)
                df_dict[key] = df

    if len(df_dict) == 0:
        df_dict = None

    return df_dict


def pickle_for_later(for_pickling: dict) -> bool:
    pickled = False
    for path, value in for_pickling.items():
        value.to_pickle(path)
        pickled = True
    return pickled


def get_sql(plot_config: Dict):
    sql = get_config_key(plot_config, 'sql')
    startdate = get_config_key(plot_config, 'startdate')
    enddate = get_config_key(plot_config, 'enddate')

    if startdate is not None:
        sql = sql.replace("'startdate'", f"'{startdate}'")
    if enddate is not None:
        sql = sql.replace("'enddate'", f"'{enddate}'")

    return sql


@solid(required_resource_keys={'postgres_warehouse'})
def process_sql_plot(context, plot_info: Dict, plotly_cfg: String):
    """
    Perform the plot
    :param context: execution context
    :param plot_info: dict of plot configuration
    :param plotly_cfg: dict of plotly configuration
    """

    client = context.resources.postgres_warehouse.get_connection(context)

    if client is not None:

        for plot_name in plot_info.keys():

            context.log.info(f"Retrieving data for '{plot_name}'")

            plot_config = plot_info[plot_name]
            pkl_cfg = pickle_cfg(plot_config, plot_name)

            dfs = unpickle(pkl_cfg, plot_name)
            if dfs is not None:
                context.log.info(f"Unpickled '{plot_name}'")
            else:
                dfs = {}
                if pkl_cfg['pickle_it']:
                    for_pickling = {}
                else:
                    for_pickling = None
                multiplot = get_config_key(plot_config, 'multiplot')
                if multiplot:
                    # multiple queries to run
                    for trace in multiplot.keys():

                        context.log.info(f"Retrieving data for '{trace}' for '{plot_name}'")

                        dfs[trace] = pd.read_sql(
                            get_sql(multiplot[trace]), client
                        )

                        # transform values if required
                        transform = get_config_key(plot_config, 'transform')
                        if transform:
                            for column, transformer in transform.items():
                                for from_val, to_val in transformer.items():
                                    dfs[trace][column] = dfs[trace][column].where(dfs[trace][column] != from_val, to_val)

                        if for_pickling is not None:
                            for_pickling[pkl_cfg['pkl_name'][trace]] = dfs[trace]

                else:
                    # single query to run
                    dfs[plot_name] = pd.read_sql(
                        get_sql(plot_config), client
                    )
                    for_pickling[pkl_cfg['pkl_name']] = dfs[plot_name]

                if for_pickling is not None:
                    if pickle_for_later(for_pickling):
                        context.log.info(f"Pickled '{plot_name}'")

            enable_static = True
            if plotly_cfg is not None and len(plotly_cfg):
                plotly.io.orca.config.executable = plotly_cfg
            else:
                enable_static = False

            context.log.info(f"Generating plot '{plot_name}'")

            plot_type = config_key_lower(plot_config, 'type')
            if plot_type in ['line', 'bar']:
                # fig = process_line_plot_express(df, plot_config)
                fig = process_line_plot_figure(dfs, plot_config)
            elif plot_type == 'pie':
                # fig = process_line_plot_express(df, plot_config)
                fig = process_pie_plot_figure(dfs, plot_config)
            else:
                raise ValueError(f'Unknown plot type {plot_type}')

            output_to = config_key_lower(plot_config, 'output_to')
            output_cnt = 0
            target_cnt = 0
            if 'file' in output_to:
                target_cnt += 1
                if not enable_static:
                    context.log.info(f"Static plots are currently disabled")
                else:
                    output_path = config_key_lower(plot_config, 'output_path')
                    if '<plot_name>' in output_path:
                        output_path = output_path.replace('<plot_name>', plot_name)

                    context.log.info(f"Saving '{plot_name}' to '{output_path}'")

                    fig.write_image(output_path)
                    output_cnt += 1
            if 'show' in output_to:
                target_cnt += 1
                fig.show()
                output_cnt += 1

            if output_cnt == 0 and target_cnt == 0:
                context.log.info(f"Unknown output destination ({output_to}) for '{plot_name}'")


@solid
def process_plot(context, plot_info: Dict, plot_config: Dict, plotly_cfg: String):
    """
    Perform the plot
    :param context: execution context
    :param df: pandas DataFrame of plot data
    :param plot_config: dict of plot configurations
    :param plotly_cfg: dict of plotly configuration
    """

    for key in plot_info.keys():

        context.log.info(f"Generating plot '{key}'")

        df = plot_info[key]['df']
        plot_config = plot_info[key]['config']

        # group by date and sum resulting in a series with date as index
        data = df.groupby([df[plot_config['x']].dt.date])[plot_config['y']].sum()

        # convert series to dataframe
        grouped_df = pd.DataFrame(data.tolist(), columns=[plot_config['y']])
        # and add the dates as another column
        grouped_df[plot_config['x']] = data.index

        enable_static = True
        if plotly_cfg is not None and len(plotly_cfg):
            plotly.io.orca.config.executable = plotly_cfg
        else:
            enable_static = False

        plot_type = plot_config['type'].lower()
        if plot_type == 'line':
            fig = process_line_plot_express(grouped_df, plot_config)
        else:
            raise ValueError(f'Unknown plot type {plot_type}')

        output_to = plot_config['output_to'].lower()
        output_cnt = 0
        target_cnt = 0
        if 'file' in output_to:
            target_cnt += 1
            if not enable_static:
                context.log.info(f"Static plots are currently disabled")
            else:
                context.log.info(f"Saving '{key}' to '{plot_config['output_path']}'")
                fig.write_image(plot_config['output_path'])
                output_cnt += 1
        if 'show' in output_to:
            target_cnt += 1
            fig.show()
            output_cnt += 1
        if output_cnt == 0 and target_cnt == 0:
            context.log.info(f"Unknown output destination ({output_to}) for '{plot_config['title']}'")


def process_line_plot_express(grouped_df: DataFrame, plot_config: Dict) -> Any:
    """
    Perform a line plot
    :param grouped_df: pandas DataFrame of plot data
    :param plot_config: dict of plot configurations
    """

    x_param = plot_config['x']
    y_param = plot_config['y']
    colour = None
    if isinstance(plot_config['y'], list):
        plot_data = None
        y_param = 'y_data'
        colour = 'vars'
        for column in plot_config['y']:
            # a  b  c   ->  a  y_data vars
            # a1 b1 c1      a1 b1     b
            # a2 b2 c2      a2 b2     b
            #               a1 c1     c
            #               a2 c2     c
            if plot_data is None:
                plot_data = grouped_df.loc[:, [plot_config['x'], column]]
                plot_data.columns = [plot_data.columns[0], y_param]
                plot_data['vars'] = column
            else:
                df2 = grouped_df.loc[:, [plot_config['x'], column]]
                df2['vars'] = column
                df2.columns = plot_data.columns
                plot_data = plot_data.append(df2, ignore_index=False)
    else:
        plot_data = grouped_df

    plot_type = plot_config['type'].lower()
    if plot_type == 'line':
        # https://plot.ly/python-api-reference/generated/plotly.express.line.html
        fig = px.line(data_frame=plot_data, x=x_param, y=y_param, title=plot_config['title'],
                      labels=plot_config['labels'], color=colour)
    elif plot_type == 'bar':
        # https://plot.ly/python-api-reference/generated/plotly.express.bar.html
        fig = px.line(data_frame=plot_data, x=x_param, y=y_param, title=plot_config['title'],
                      labels=plot_config['labels'], color=colour)
    else:
        raise ValueError(f'Unknown plot type {plot_type}')

    return fig


def figure_layout_updates(layout_updates: dict, plot_config: dict, keys: list) -> dict:
    """
    Get figure layout updates
    :param layout_updates:
    :param plot_config: dict of plot configurations
    :param keys:
    """
    for key in keys:
        value = get_config_key(plot_config, key)
        if value is not None:
            layout_updates[key] = value

    return layout_updates


def common_figure_layout_updates(plot_config: dict) -> dict:
    """
    Get common figure layout updates
    :param plot_config: dict of plot configurations
    """
    return figure_layout_updates({}, plot_config, ['title'])


def process_line_plot_figure(dfs: Union[DataFrame, Dict], plot_config: Dict) -> Any:
    """
    Perform a line/bar plot
    :param dfs: pandas DataFrame of plot data
    :param plot_config: dict of plot configurations
    """
    plot_type = config_key_lower(plot_config, 'type')
    y_data = get_config_key(plot_config, 'y')
    legend = get_config_key(plot_config, 'legend')
    labels = get_config_key(plot_config, 'labels')  # express nomenclature, figure is xaxis_title/yaxis_title

    if plot_type not in ['line', 'bar']:
        raise ValueError(f'Unknown plot type {plot_type}')

    # https://plot.ly/python-api-reference/generated/plotly.graph_objects.Figure.html
    fig = go.Figure()
    layout_updates = common_figure_layout_updates(plot_config)

    if isinstance(dfs, DataFrame):
        process_dict = {'df': dfs}
    else:
        process_dict = dfs

    if plot_type in ['line', 'bar']:
        for name, df in process_dict.items():
            x = df[plot_config['x']].to_list()
            if isinstance(y_data, list):
                y_list = y_data
            else:
                y_list = list(y_data)

            if plot_type == 'bar':
                figure_layout_updates(layout_updates, plot_config, ['barmode'])

            for column in y_list:
                y = df[column].to_list()

                arg = {
                    'x': x,
                    'y': y
                }
                text_on_bar = config_key_is_true(plot_config, 'text_on_bar')
                if text_on_bar:
                    arg['text'] = y
                    arg['textposition'] = 'auto'
                    text_on_bar_template = get_config_key(plot_config, 'text_on_bar_template')
                    if text_on_bar_template is not None:
                        arg['texttemplate'] = text_on_bar_template


                if legend is not None:
                    if isinstance(legend[column], dict):
                        arg['name'] = legend[column][name]
                    else:
                        arg['name'] = legend[column]

                if plot_type == 'line':
                    # https://plot.ly/python-api-reference/generated/plotly.graph_objects.Scatter.html#plotly.graph_objects.Scatter
                    trace = go.Scatter(arg=arg)
                elif plot_type == 'bar':
                    # https://plot.ly/python-api-reference/generated/plotly.graph_objects.Bar.html#plotly.graph_objects.Bar
                    trace = go.Bar(arg=arg)
                else:
                    trace = None

                if trace is not None:
                    fig.add_trace(trace)

    if labels is not None:
        entries = ['xaxis_title', 'yaxis_title']
        idx = 0
        for key in labels.keys():
            layout_updates[entries[idx]] = labels[key]
            idx += 1

    if len(layout_updates.keys()) > 0:
        fig.update_layout(layout_updates)

    return fig


def process_pie_plot_figure(dfs: Dict, plot_config: Dict) -> Any:
    """
    Perform a line/bar plot
    :param df: pandas DataFrame of plot data
    :param plot_config: dict of plot configurations
    """
    plot_type = config_key_lower(plot_config, 'type')

    if plot_type not in ['pie']:
        raise ValueError(f'Unknown plot type {plot_type}')

    # https://plot.ly/python-api-reference/generated/plotly.graph_objects.Figure.html
    fig = go.Figure()
    layout_updates = common_figure_layout_updates(plot_config)

    if plot_type in ['pie']:
        for name, df in dfs.items():
            labels = get_config_key(plot_config, 'labels')
            values = get_config_key(plot_config, 'values')

            if plot_type == 'pie':
                arg = {
                    'labels': df[labels].to_list(),
                    'values': df[values].to_list()
                }
                for key in ['hoverinfo', 'textinfo', 'sort']:
                    value = get_config_key(plot_config, key)
                    if value is not None:
                        arg[key] = value

                # https://plot.ly/python-api-reference/generated/plotly.graph_objects.Pie.html#plotly.graph_objects.Pie
                trace = go.Pie(arg=arg)
            else:
                trace = None

            if trace is not None:
                fig.add_trace(trace)

    if len(layout_updates.keys()):
        fig.update_layout(layout_updates)

    return fig
