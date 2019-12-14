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
from datetime import date, datetime

import math
import numpy as np
import pandas as pd
import sys
from dagster import (
    solid,
    lambda_solid,
    Dict,
    Failure)
from dagster_pandas import DataFrame


@lambda_solid()
def get_table_desc_by_type(table_desc: DataFrame) -> Dict:
    """
    Group the entries in the table description by field type
    :param table_desc: pandas DataFrame containing details of the database table
    :return: dict of pandas DataFrames of data types in database table with data type as the key
    """
    return {
        'date': table_desc[table_desc['datatype'].str.lower() == 'date'],
        'timestamp': table_desc[table_desc['datatype'].str.lower() == 'timestamp'],
        'int': table_desc[table_desc['datatype'].str.lower().isin(['smallint', 'integer', 'smallserial', 'serial'])],
        'long': table_desc[table_desc['datatype'].str.lower().isin(['bigint', 'bigserial'])],
        'real': table_desc[table_desc['datatype'].str.lower().isin(['real'])],
        'double precision': table_desc[table_desc['datatype'].str.lower().isin(['double precision'])],
        'text': table_desc[table_desc['datatype'].str.lower().isin(['text'])],
        'varchar': table_desc[table_desc['datatype'].str.contains('varchar', case=False, regex=False)]
    }


@lambda_solid()
def get_table_desc_type_limits(table_desc: DataFrame) -> Dict:
    """
    Get the type limits for the entries in the table description
    :param table_desc: pandas DataFrame containing details of the database table
    :return: dict of type limits with field name as the key
    """
    type_values = {
        'date': {'min_val': date.min, 'max_val': date.max, 'max_size': sys.getsizeof(date.min)},
        'timestamp': {'min_val': datetime.min, 'max_val': datetime.max, 'max_size': sys.getsizeof(datetime.min)},
        'smallint': {'min_val': -2 ** 15, 'max_val': 2 ** 15 - 1, 'max_size': 2},
        'smallserial': {'min_val': -2 ** 15, 'max_val': 2 ** 15 - 1, 'max_size': 2},
        'int': {'min_val': -2 ** 31, 'max_val': 2 ** 31 - 1, 'max_size': 2},
        'serial': {'min_val': -2 ** 31, 'max_val': 2 ** 31 - 1, 'max_size': 2},
        'bigint': {'min_val': -2 ** 63, 'max_val': 2 ** 63 - 1, 'max_size': 8},
        'bigserial': {'min_val': -2 ** 63, 'max_val': 2 ** 63 - 1, 'max_size': 8},
        'real': {'min_val': math.exp(-37), 'max_val': math.exp(37), 'max_size': 4},
        'double precision': {'min_val': math.exp(-307), 'max_val': math.exp(308), 'max_size': 8},
        'text': {'min_val': 0, 'max_val': 0, 'max_size': 0},
    }
    regex = re.compile(r'.*\((\d+)\)')
    type_limits = {}
    for idx in range(len(table_desc)):
        row = table_desc.iloc[idx]
        row_type = row['datatype'].lower()
        if row_type in type_values:
            type_limits[row['field']] = type_values[row_type]
        elif row_type.startswith('varchar'):
            match = regex.search(row_type)
            if match:
                max_size = int(match.group(1))
            else:
                max_size = 0
            type_limits[row['field']] = {'min_val': 0, 'max_val': 0, 'max_size': max_size}
    return type_limits


@solid
def transform_sets_df(context, sets_df: Dict, table_desc: DataFrame, table_desc_by_type: Dict,
                      table_type_limits: Dict) -> Dict:
    """
    Perform any necessary transformations on the sets panda DataFrames
    :param context: execution context
    :param sets_df: dict of DataSet with set ids as key
    :param table_desc: pandas DataFrame containing details of the database table
    :param table_desc_by_type: dict of pandas DataFrames of data types in database table with data type as the key
    :param table_type_limits: dict of type limits with field name as the key
    :return: dict of pandas DataFrames with set ids as key
    :rtype: dict
    """
    for set_id in sets_df.keys():

        context.log.info(f"Transform data set {set_id}'")

        set_df = sets_df[set_id].df

        # generate a list of the names of fields with the date types
        # also a list of formats for the date fields
        date_fields = np.concatenate((table_desc_by_type['date']['field'].values,
                                      table_desc_by_type['timestamp']['field'].values)).tolist()
        date_fields_formats = np.concatenate((table_desc_by_type['date']['format'].values,
                                              table_desc_by_type['timestamp']['format'].values)).tolist()
        # generate numpy arrays of the names of fields with the same type
        int_fields = table_desc_by_type['int']['field'].values
        long_fields = table_desc_by_type['long']['field'].values
        real_fields = table_desc_by_type['real']['field'].values
        double_precision_fields = table_desc_by_type['double precision']['field'].values
        text_fields = np.concatenate((table_desc_by_type['text']['field'].values,
                                      table_desc_by_type['varchar']['field'].values))

        # TODO needs work, type coercion is really only for the types known to need it rather than a general method
        for label, content in set_df.items():  # Iterator over (column name, Series) pairs
            row = table_desc[table_desc['field'] == label].iloc[0]  # will only be one
            load_type = row['loadtype'].lower()
            data_type = row['datatype'].lower()
            if load_type != '':
                # loaded as different type to required
                new_type = data_type
            else:
                new_type = ''

            if label in date_fields:
                if new_type == 'timestamp':
                    # transform date strings to dates
                    try:
                        fidx = date_fields.index(label)
                        set_df[label] = pd.to_datetime(set_df[label], format=date_fields_formats[fidx])
                    except ValueError:
                        pass  # ignore, no format was found
            elif label in int_fields or label in long_fields:
                if new_type != '' and load_type == 'text':
                    # replace nan and coerce to int
                    content.fillna('0', inplace=True)
                    set_df[label] = set_df[label].astype(int)  # no diff between int & long in python 3
                # transform empty integer fields
                content.fillna(0, inplace=True)
            elif label in real_fields or label in double_precision_fields:
                # transform empty real field
                content.fillna(0, inplace=True)
            elif label in text_fields:
                # transform empty text field
                content.fillna('', inplace=True)

            # do check on data to ensure doesn't exceed type limits
            if data_type.startswith('varchar'):
                limit = table_type_limits[label]
                failed = content.str.len() > limit['max_size']
                if failed.any():
                    raise Failure(f"Type limit check failure for '{label}': {failed.value_counts()[True]} "
                                  f"entries exceeded max size {limit['max_size']}")
            elif data_type != 'text':
                # TODO min max value checks
                pass

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


@solid
def currency_transform_sets_df(context, sets_df: Dict, table_desc: DataFrame, table_desc_by_type: Dict,
                               table_type_limits: Dict, currency_eq_usd_df: DataFrame,
                               dtypes_by_root: Dict, regex_patterns: Dict, currency_cfg: Dict) -> Dict:
    """
    Perform any necessary transformations on the sets panda DataFrames
    :param context: execution context
    :param sets_df: dict of DataSet with set ids as key
    :param table_desc: pandas DataFrame containing details of the database table
    :param table_desc_by_type: dict of pandas DataFrames of data types in database table with data type as the key
    :param table_type_limits: dict of type limits with field name as the key
    :return: dict of pandas DataFrames with set ids as key
    :rtype: dict
    """

    raise NotImplementedError('Currency transform functionality is not yet fully supported')

    entity_usd_mapping = currency_cfg['value']['entity_usd_mapping']

    for set_id in sets_df.keys():

        context.log.info(f"Currency transform data set {set_id}'")

        set_df = sets_df[set_id].df

        regex_patterns_dict = regex_patterns['value']
        regex_item = re.compile(regex_patterns_dict['set_usd_pattern'])

        # .add_solid_input('read_sj_csv_file_sets', 'regex_patterns', regex_patterns) \

        usd_columns = table_desc[table_desc['root'].str.lower() == 'usd']

        def add_column(field):
            set_df[field] = 0.0

        usd_columns['field'].apply(add_column)

        # currency_eq_usd_df['Date'] = datetime
        # set_df['usd_rate'] = currency_eq_usd_df.loc[currency_eq_usd_df['Date'] == set_df['SALESDATE'].date(), set_df['ENTITYCURRENCY_CODE']]

        for mapping in entity_usd_mapping:
            set_df[mapping[1]] = set_df[mapping[0]]

        set_df['DateM'] = set_df['SALESDATE'].apply(lambda x: x.date())

        currency_eq_usd_df['DateM'] = currency_eq_usd_df['Date'].apply(lambda x: x.date())

        xx = set_df.merge(currency_eq_usd_df, left_on=['DateM'],
                         right_on=['DateM'],
                         how='left', suffixes=('_left', '_right'))

        xx.at[0, 'ENTITYCURRENCYCODE'] = 'GBP'

        entities = xx[xx['ENTITYCURRENCYCODE'] != 'USD']
        for entity in entities:
            xx = set_df[set_df['ENTITYCURRENCYCODE'] == entity]
            xx[mapping[1]] = xx[mapping[0]] * xx[entity]


            # for idx in range(len(set_df)):
            #     row = set_df.iloc[idx]
            #     if row['ENTITYCURRENCYCODE'] != 'USD' or idx == 0:
            #         salesdate = set_df.loc[idx, 'SALESDATE'].date()
            #         entity_code = set_df.loc[idx, 'ENTITYCURRENCYCODE']
            #         set_df.loc[idx, mapping[1]] = (set_df.loc[idx, mapping[0]] * currency_eq_usd_df.loc[
            #             currency_eq_usd_df['Date'] == salesdate, entity_code
            #         ]).values[0]



            # # YUCK definitely not 'pandaish'!
            # for idx in range(len(set_df)):
            #     xx.loc[idx, mapping[1]] = xx[idx, mapping[0]] * xx.loc[idx, 'ENTITYCURRENCYCODE']
            # YUCK definitely not 'pandaish'!
            # for idx in range(len(set_df)):
            #     row = set_df.iloc[idx]
            #     if row['ENTITYCURRENCYCODE'] != 'USD' or idx == 0:
            #         salesdate = set_df.loc[idx, 'SALESDATE'].date()
            #         entity_code = set_df.loc[idx, 'ENTITYCURRENCYCODE']
            #         set_df.loc[idx, mapping[1]] = (set_df.loc[idx, mapping[0]] * currency_eq_usd_df.loc[
            #             currency_eq_usd_df['Date'] == salesdate, entity_code
            #         ]).values[0]

    return sets_df
