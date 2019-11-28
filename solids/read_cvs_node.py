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

from db_toolkit.misc import test_dir_path

from dagster import (
    solid,
    Field,
    String,
    List,
    Dict,
    InputDefinition,
    OutputDefinition,
    Output
)


@solid()
def load_list_of_csv_files(context, db_data_path: String) -> List:
    """
    Load csv file and convert into a panda DataFrame
    :param context: execution context
    :param db_data_path: directory containing database csv files
    :return: list of dictionaries of file details; {'name': filename, 'path': path including filename}
    :rtype: list
    """
    # verify csv path
    if not path.exists(db_data_path):
        raise ValueError(f'Invalid io directory path: {db_data_path}')
    if not test_dir_path(db_data_path):
        raise ValueError(f'Not a directory path: {db_data_path}')

    # use list comprehension to get listing of files
    files_lst = [{'name': f, 'path': join(db_data_path, f)}
                 for f in listdir(db_data_path)
                 if isfile(join(db_data_path, f))]

    context.log.info(f'Loaded {len(files_lst)} filenames from {db_data_path}')

    return files_lst


@solid()
def create_csv_file_sets(context, files_lst: List, set_pattern: String, set_link_pattern: String) -> List:
    """
    Group all files into import sets
    :param context: execution context
    :param files_lst: list of dictionaries of file details;
                  {'name': filename, 'path': path including filename}
    :param set_pattern: regex representing a filename that is part of a set
    :param set_link_pattern: regex representing the part of the filename which links files to a set
    :return: list of, dictionaries of dictionaries of all the files in an import set;
             [ {set_id1: [{'name': filename1_set1, 'path': path including filename1_set1},
                         {'name': filename2_set1, 'path': path including filename2_set1}, ...]},
               {set_id2: [{'name': filename1_set2, 'path': path including filename1_set2},
                          {'name': filename2_set2, 'path': path including filename2_set2}, ...]}, ... ]
    :rtype: list
    """

    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.from_records.html#pandas.DataFrame.from_records
    df = pd.DataFrame.from_records(files_lst)

    # only files matching set pattern
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.str.contains.html
    all_set_entries = df[df['name'].str.contains(set_pattern)]

    # add a column with the set link
    all_set_entries['set'] = all_set_entries['name'].str.extract(set_link_pattern)

    # get unique set identifiers
    set_links = all_set_entries['set'].unique()
    all_set_entries = all_set_entries.drop(['set'], axis=1)  # drop set link column no longer required

    context.log.info(f'{len(set_links)} data set(s) identified')

    sets_list = []
    for set_id in set_links:
        # df of all entries with same set link
        set_entity = all_set_entries[all_set_entries['name'].str.contains(set_id, regex=False)]
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_dict.html
        sets_list.append({set_id: set_entity.to_dict('records')})

    return sets_list


@solid(
    output_defs=[
        OutputDefinition(dagster_type=List, name='sets_list', is_optional=False),
        OutputDefinition(dagster_type=Dict, name='sets_df', is_optional=False),
    ],
)
def read_csv_file_sets(context, sets_list: List, table_desc_by_type: Dict, set_item_pattern: String):
    """
    Group all files into import sets
    :param context: execution context
    :param sets_list: list of, dictionaries of dictionaries of all the files in an import set;
                 [ {set_id1: [{'name': filename1_set1, 'path': path including filename1_set1},
                             {'name': filename2_set1, 'path': path including filename2_set1}, ...]},
                   {set_id2: [{'name': filename1_set2, 'path': path including filename1_set2},
                              {'name': filename2_set2, 'path': path including filename2_set2}, ...]}, ... ]
    :param table_desc_by_type: dict of pandas DataFrames of data types in database table
    :param set_item_pattern: regex representing the part of the filename which identifies a file within a set
    :return: dict of pandas DataFrames with set ids as key
    """
    regex = re.compile(set_item_pattern)
    sets_df = {}
    for set_entry in sets_list:  # dict in list
        for set_id in set_entry.keys():  # key in dict.keys (there's only one)
            for entry in set_entry[set_id]:  # dict in list
                if regex.search(entry['name']):
                    # found matching file, read it as DataFrame in a dict with set_id as key

                    # generate the dtypes values to use when reading the csv
                    dtypes = {}
                    for field_type in table_desc_by_type.keys():
                        type_df = table_desc_by_type[field_type]
                        if field_type == 'date' or field_type == 'timestamp':
                            dtype = np.str  # as str for now
                        elif field_type == 'int':
                            dtype = np.int32
                        elif field_type == 'real':
                            dtype = np.float32
                        else:
                            dtype = np.str

                        for idx in range(len(type_df)):
                            row = type_df.iloc[idx]
                            dtypes[row['field']] = dtype

                    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html#pandas.read_csv
                    df = pd.read_csv(entry['path'], dtype=dtypes)
                    df.rename(columns=str.strip, inplace=True)  # remove any whitespace
                    sets_df[set_id] = df
                    break

    context.log.info(f'{len(sets_df)} DataFrames loaded from {len(sets_list)} data sets')

    yield Output(sets_list, 'sets_list')
    yield Output(sets_df, 'sets_df')


@solid(
    output_defs=[
        OutputDefinition(dagster_type=List, name='sets_list', is_optional=False),
        OutputDefinition(dagster_type=Dict, name='sets_df', is_optional=False),
    ],
)
def merge_promo_csv_file_sets(context, sets_list: List, sets_df: Dict, set_item_pattern: String):
    """
    Group all files into import sets
    :param context: execution context
    :param sets_list: list of, dictionaries of dictionaries of all the files in an import set;
                 [ {set_id1: [{'name': filename1_set1, 'path': path including filename1_set1},
                             {'name': filename2_set1, 'path': path including filename2_set1}, ...]},
                   {set_id2: [{'name': filename1_set2, 'path': path including filename1_set2},
                              {'name': filename2_set2, 'path': path including filename2_set2}, ...]}, ... ]
    :param sets_df: dict of pandas DataFrames with set ids as key
    :param set_item_pattern: regex representing the part of the filename which identifies a file within a set
    :return: list of pandas DataFrames containing all the files in an import set
    :rtype: list
    """
    count = 0
    regex = re.compile(set_item_pattern)
    for set_entry in sets_list:  # dict in list
        for set_id in set_entry.keys():  # key in dict.keys
            for entry in set_entry[set_id]:  # dict in list
                if regex.search(entry['name']):
                    # found matching file, read it as DataFrame
                    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html#pandas.read_csv
                    df = pd.read_csv(entry['path'])

                    # SJ has the following header
                    # ID,SOURCE,SALESDATE,RESVCODE,RESVCOMPSEQUENCE,ENTRYTYPE,SEQUENCE,RESVCOMPTYPE,RESVCOMPSUBTYPE,DESCRIPTION,REMOTEREFTYPE,REMOTEREFCODE,DOCUMENTED,FARECONSTRUCTION,PROVIDERCODE, CUSTOMERPROFILE,AGENCY,AGENT,DOCTYPE,TRANSACTIONCURRENCYCODE,TRANSACTIONBASEAMOUNT,TRANSACTIONTOTALTAXAMOUNT,ENTITYCURRENCYCODE,ENTITYBASEAMOUNT,ENTITYTOTALTAXAMOUNT, TRANSACTIONMILESAMOUNTPAID,TRANSACTIONMONEYAMOUNTPAID,CUSTOMERTYPE,MARKET,PAYMENTTYPE,TRAVELERTYPE,ENTITYTOTALPROMOTIONAMOUNT,INTERNALAGENT, FIRSTDATEOFTRAVEL,LASTDATEOFTRAVEL,PROVIDERNAME,FEETYPEDESCRIPTION, FEESUBTYPEDESCRIPTION,NONREFUNDABLE,REFERENCEDCOMPONENTTYPE,TRANSACTIONBASEREDEMPTIONAMT,TRANSACTIONBASEREDEMPTIONEQUIV,INVOICED, LOYALTYNUMBER
                    # SJPromo has the following header
                    # ID,SALESJOURNALID,SEQUENCE,TRANSACTIONPROMOTIONAMOUNT,ENTITYPROMOTIONAMOUNT,PROMOCODE,EXTERNALPROMOCODE,CERTIFICATE

                    # ID is not required
                    df = df.drop(['ID'], axis=1)

                    # SALESJOURNALID,SEQUENCE represents a unique sequence of a row within a SalesJournal, so will be
                    # used to match SJ entries but is not required in merged DataFrame
                    # Other column names do not conflict with existing SJ column names

                    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.merge.html#pandas.DataFrame.merge
                    # merge DataFrame using only keys from left frame, similar to a SQL left outer join &
                    # preserve key order.
                    merged = sets_df[set_id].merge(df, left_on=['ID', 'SEQUENCE'],
                                                   right_on=['SALESJOURNALID', 'SEQUENCE'],
                                                   how='left', suffixes=('_left', '_right'))
                    # in result SEQUENCE from the right is automatically dropped as its exactly the same as
                    # SEQUENCE from the left, but SALESJOURNALID from the right is included but not needed
                    # (same as ID from the left), so drop
                    merged = merged.drop(['SALESJOURNALID'], axis=1)
                    sets_df[set_id] = merged

                    count += 1
                    break

    context.log.info(f'{count} promo DataFrames merged from {len(sets_list)} data sets')

    yield Output(sets_list, 'sets_list')
    yield Output(sets_df, 'sets_df')
