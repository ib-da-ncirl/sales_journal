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

import numpy as np
import pandas as pd
from dagster import (
    solid,
    lambda_solid,
    composite_solid,
    Field,
    String,
    List,
    Dict,
    OutputDefinition,
    Output
)
from dagster_pandas import DataFrame


@lambda_solid()
def get_table_desc_by_type(table_desc: DataFrame) -> Dict:
    """
    Perform any necessary transformations on the sets panda DataFrames
    :param table_desc: pandas DataFrame containing details of the database table
    :return: dict of pandas DataFrames of data types in database table
    :rtype: dict
    """
    return {
        'date': table_desc[table_desc['datatype'].str.lower() == 'date'],
        'timestamp': table_desc[table_desc['datatype'].str.lower() == 'timestamp'],
        'int': table_desc[table_desc['datatype'].str.lower().isin(['integer', 'smallint'])],
        'real': table_desc[table_desc['datatype'].str.lower().isin(['real'])],
        'text': table_desc[table_desc['datatype'].str.lower().isin(['text'])],
        'varchar': table_desc[table_desc['datatype'].str.contains('varchar', case=False, regex=False)]
    }


@solid
def transform_sets_df(context, sets_df: Dict, table_desc_by_type: Dict) -> Dict:
    """
    Perform any necessary transformations on the sets panda DataFrames
    :param context: execution context
    :param sets_df: dict of pandas DataFrames with set ids as key
    :param table_desc_by_type: dict of pandas DataFrames of data types in database table
    :return: dict of pandas DataFrames with set ids as key
    :rtype: dict
    """
    for set_id in sets_df.keys():
        set_df = sets_df[set_id]

        # generate a list of the names of fields with the date types
        # also a list of formats for the date fields
        date_fields = np.concatenate((table_desc_by_type['date']['field'].values,
                                     table_desc_by_type['timestamp']['field'].values)).tolist()
        date_fields_formats = np.concatenate((table_desc_by_type['date']['format'].values,
                                              table_desc_by_type['timestamp']['format'].values)).tolist()
        # generate numpy arrays of the names of fields with the same type
        int_fields = table_desc_by_type['int']['field'].values
        real_fields = table_desc_by_type['real']['field'].values
        text_fields = np.concatenate((table_desc_by_type['text']['field'].values,
                                     table_desc_by_type['varchar']['field'].values))

        for label, content in set_df.items():   # Iterator over (column name, Series) pairs
            if label in date_fields:
                # transform date strings to dates
                try:
                    fidx = date_fields.index(label)
                    set_df[label] = pd.to_datetime(set_df[label], format=date_fields_formats[fidx])
                except ValueError:
                    pass    # ignore, no format was found
            elif label in int_fields:
                # transform empty integer fields
                content.fillna(0, inplace=True)
            elif label in real_fields:
                # transform empty real field
                content.fillna(0, inplace=True)
            elif label in text_fields:
                # transform empty text field
                content.fillna('', inplace=True)

    return sets_df


@solid()
def transform_table_desc_df(context, table_desc: DataFrame) -> DataFrame:
    """
    Transform the DataFrame of data types in database table
    :param context: execution context
    :param table_desc: panda DataFrame containing details of the Postgres database table
    :return: panda DataFrame containing details of the Postgres database table
    :rtype: panda.DataFrame
    """

    table_desc.fillna('', inplace=True)

    # get names of indices for which column ignore contains #. i.e. comment lines
    index_names = table_desc[table_desc['ignore'].str.contains('#')].index
    # drop comment row indexes from dataFrame
    table_desc.drop(index_names, inplace=True)
    # drop column ignore from dataFrame
    table_desc.drop(['ignore'], axis=1, inplace=True)

    return table_desc


