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

import pandas as pd
from dagster_pandas import DataFrame
from dagster import (
    solid,
    Field,
    String,
    List,
    Dict,
    OutputDefinition,
    Output
)
import plotly
import plotly.express as px


@solid
def transform_plot_data(context, df: DataFrame, plot_config: Dict) -> DataFrame:
    """
    Perform any necessary transformations on the plot data
    :param context: execution context
    :param df: pandas DataFrame of plot data
    :param plot_config: dict of plot configurations
    :return: dict of pandas DataFrame of plot data
    """
    df.columns = plot_config['header']  # add the column names to the dataframe

    for column in plot_config['header']:
        if column in plot_config.keys():
            column_config = plot_config[column]
            if 'to_datetime' in column_config.keys():
                # convert to a date time
                df[column] = pd.to_datetime(df[column], format=column_config['to_datetime'])

    return df


@solid
def process_plot(context, df, plot_config, plotly_cfg):
    """
    Perform the plot
    :param context: execution context
    :param df: pandas DataFrame of plot data
    :param plot_config: dict of plot configurations
    :param plotly_cfg: dict of plotly configuration
    """

    context.log.info(f'Generating plot')

    # group by date and sum resulting in a series with date as index
    data = df.groupby([df[plot_config['x']].dt.date])[plot_config['y']].sum()

    # convert series to dataframe
    grouped_df = pd.DataFrame(data.tolist(), columns=[plot_config['y']])
    # and add the dates as another column
    grouped_df[plot_config['x']] = data.index

    if plotly_cfg is not None:
        plotly.io.orca.config.executable = plotly_cfg

    # https://plot.ly/python-api-reference/generated/plotly.express.line.html
    fig = px.line(data_frame=grouped_df, x=plot_config['x'], y=plot_config['y'], title=plot_config['title'],
                  labels=plot_config['labels'])
    # fig.show()
    fig.write_image(plot_config['output_path'])
