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

import re
import numpy as np
import pandas as pd
import os.path as path
from os import listdir
from os.path import isfile, join

from db_toolkit.misc import (
    test_file_path,
    load_yaml
)

from dagster import (
    solid,
    Field,
    String,
    List,
    Dict,
    OutputDefinition,
    Output
)

import yaml


@solid(
    output_defs=[
        OutputDefinition(dagster_type=Dict, name='plot_config', is_optional=False),
        OutputDefinition(dagster_type=String, name='plot_sql', is_optional=False),
    ],
)
def initialise_plot(context, yaml_path, plot_name):
    """
    Load yaml file and return the configuration dictionary for the specified plot
    :param context: execution context
    :param yaml_path: path to the yaml configuration file
    :param plot_name: name of plot to retrieve config for
    :return: configuration dictionary
    :rtype: dict
    """
    # verify path
    if not path.exists(yaml_path):
        raise ValueError(f'Invalid path: {yaml_path}')
    if not test_file_path(yaml_path):
        raise ValueError(f'Not a file path: {yaml_path}')

    plots_config = load_yaml(yaml_path)

    plot_config = None
    if plot_name in plots_config.keys():
        plot_config = plots_config[plot_name]
        context.log.info(f'Loaded configuration for {plot_name} from {yaml_path}')
    else:
        context.log.warn(f'No configuration for {plot_name} found in {yaml_path}')

    yield Output(plot_config, 'plot_config')
    yield Output(plot_config['sql'], 'plot_sql')

