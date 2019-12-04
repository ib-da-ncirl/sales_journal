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
from datetime import datetime

from gzip import GzipFile

from dagster_pandas import DataFrame

from db_toolkit.misc import test_dir_path
from misc_sj import DataSet
from dagster import (
    solid,
    Field,
    String,
    List,
    Dict,
    InputDefinition,
    OutputDefinition,
    Output,
    lambda_solid, Optional)


@solid()
def load_list_of_csv_files(context, db_data_path: String, date_in_name_pattern: String,
                           date_in_name_format: String) -> List:
    """
    Load csv file and convert into a panda DataFrame
    :param context: execution context
    :param db_data_path: directory containing database csv files
    :param date_in_name_pattern: regex to match dates in csv filenames
    :param date_in_name_format: datetime format to convert dates in csv filenames
    :return: list of dictionaries of file details; {
                'name': filename, 'path': path including filename,
                'start_date': start date in filename, 'end_date': end date in filename
                }
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

    regex = re.compile(date_in_name_pattern)
    for file in files_lst:
        match = regex.search(file['name'])
        if match:
            file['start_date'] = datetime.strptime(match.group(1), date_in_name_format)
            file['end_date'] = datetime.strptime(match.group(2), date_in_name_format)

    context.log.info(f'Loaded {len(files_lst)} filenames from {db_data_path}')

    return files_lst


@solid()
def create_csv_file_sets(context, files_lst: List, prev_uploaded: Optional[DataFrame], regex_patterns: Dict) -> List:
    """
    Group all files into import sets
    :param context: execution context
    :param files_lst: list of dictionaries of file details;
                {'name': filename, 'path': path including filename,
                 'start_date': start date in filename, 'end_date': end date in filename}
    :param regex_patterns: dict of regex pattern representing filenames and file sets
    :return: list of, dictionaries of dictionaries of all the files in an import set;
             [ {set_id1: [{'name': filename1_set1, 'path': path including filename1_set1, ...},
                         {'name': filename2_set1, 'path': path including filename2_set1, ...}, ...]},
               {set_id2: [{'name': filename1_set2, 'path': path including filename1_set2, ...},
                          {'name': filename2_set2, 'path': path including filename2_set2, ...}, ...]}, ... ]
    :rtype: list
    """

    regex_patterns_dict = regex_patterns['value']

    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.from_records.html#pandas.DataFrame.from_records
    df = pd.DataFrame.from_records(files_lst)

    # only files matching set pattern
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.str.contains.html
    all_set_entries = df[df['name'].str.contains(regex_patterns_dict['set_common_pattern'])]

    # TODO doesn't currently handle duplicate csv & gz file sets
    # add a column with the set link
    all_set_entries['set'] = all_set_entries['name'].str.extract(regex_patterns_dict['set_link_pattern'])

    # get unique set identifiers
    set_links = all_set_entries['set'].unique()
    all_set_entries = all_set_entries.drop(['set'], axis=1)  # drop set link column no longer required

    context.log.info(f'{len(set_links)} data set(s) identified')

    sets_list = []
    for set_id in set_links:
        # df of all entries with same set link
        set_entity = all_set_entries[all_set_entries['name'].str.contains(set_id, regex=False)]

        counts = {'total': 0}
        for pattern in regex_patterns_dict['set_required_elements']:
            counts[pattern] = len(set_entity[set_entity['name'].str.contains(regex_patterns_dict[pattern])])
            counts['total'] += counts[pattern]

        if counts['total'] == len(regex_patterns_dict['set_required_elements']):
            set_ok = True
            for pattern in regex_patterns_dict['set_required_elements']:
                if counts[pattern] != 1:
                    set_ok = False
                    break

            if set_ok:
                if prev_uploaded[0].str.contains(set_id, regex=False).all():
                    context.log.info(f'Ignoring previously loaded data set: {set_id}')
                else:
                    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_dict.html
                    sets_list.append({set_id: set_entity.to_dict('records')})
            else:
                context.log.info(f'Discarding inconsistent data set {set_id}: {counts}')
        else:
            context.log.info(f'Discarding incomplete data set {set_id}: {counts}')

    return sets_list


@lambda_solid
def generate_dtypes(table_desc: DataFrame, table_desc_by_type: Dict) -> Dict:
    """
    Generate a dictionary of dtypes dictionaries with the root table identifier as the key
    :param table_desc: pandas DataFrame containing details of the database table
    :param table_desc_by_type: dict of pandas DataFrames of data types in database table with data type as the key
            { 'loadtype': { 'date': date columns dataframe, 'timestamp': timestamp columns dataframe, ... }
              'datatype': { 'date': date columns dataframe, 'timestamp': timestamp columns dataframe, ... } }
    :return: dict of dtypes dicts with root table identifier as the key
    """
    dtypes = {}

    def field_type_to_dtype(fld_type):
        if fld_type == 'date' or field_type == 'timestamp':
            dtype = np.str  # as str for now
        elif fld_type == 'int':
            dtype = np.int32
        elif fld_type == 'real':
            dtype = np.float32
        else:
            dtype = np.str
        return dtype

    roots = table_desc['root'].unique()
    for root in roots:
        dtypes[root] = {}
        for field_type in table_desc_by_type.keys():
            type_df = table_desc_by_type[field_type]    # df of all of a type
            if len(type_df) > 0:
                type_df = type_df[type_df['root'].str.contains(root)]   # df of type with required root

                if len(type_df) > 0:
                    for idx in range(len(type_df)):
                        row = type_df.iloc[idx]

                        if row['loadtype'] == '':
                            dtype = field_type_to_dtype(field_type)
                        else:
                            # required different loadtype before conversion later
                            dtype = field_type_to_dtype(row['loadtype'].lower())
                        dtypes[root][row['field']] = dtype

    return dtypes


@solid(
    output_defs=[
        OutputDefinition(dagster_type=List, name='sets_list', is_optional=False),
        OutputDefinition(dagster_type=Dict, name='sets_df', is_optional=False),
    ],
)
def read_sj_csv_file_sets(context, sets_list: List, dtypes_by_root: Dict, regex_patterns: Dict):
    """
    Read the sales journal file in all import sets
    :param context: execution context
    :param sets_list: list of, dictionaries of dictionaries of all the files in an import set;
                 [ {set_id1: [{'name': filename1_set1, 'path': path including filename1_set1, ...},
                             {'name': filename2_set1, 'path': path including filename2_set1, ...}, ...]},
                   {set_id2: [{'name': filename1_set2, 'path': path including filename1_set2, ...},
                              {'name': filename2_set2, 'path': path including filename2_set2, ...}, ...]}, ... ]
    :param dtypes_by_root: dict of dtypes dicts with root table identifier as the key
    :param regex_patterns: dict of regex pattern representing filenames and file sets
    :return: dict of data with set ids as key and DataSet as value
    """
    regex_patterns_dict = regex_patterns['value']
    regex_item = re.compile(regex_patterns_dict['set_sj_pattern'])
    regex_csv_set = re.compile(regex_patterns_dict['set_pattern'])
    regex_gz_set = re.compile(regex_patterns_dict['gz_set_pattern'])
    sets_df = {}
    for set_entry in sets_list:  # dict in list
        for set_id in set_entry.keys():     # key in dict.keys (there's only one)
            for entry in set_entry[set_id]:  # dict in list
                if regex_item.search(entry['name']):
                    # found matching file, read it as DataFrame in a dict with set_id as key

                    # get the dtypes values to use when reading the csv
                    dtypes = {}
                    for key in dtypes_by_root.keys():
                        if regex_item.match(key):
                            dtypes = dtypes_by_root[key]
                            break

                    csv_match = regex_csv_set.search(entry['name'])
                    gz_match = regex_gz_set.search(entry['name'])
                    try:
                        if csv_match or gz_match:
                            if gz_match:
                                filepath_or_buffer = GzipFile(entry['path'])
                            else:
                                filepath_or_buffer = entry['path']
                            # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html#pandas.read_csv
                            df = pd.read_csv(filepath_or_buffer, dtype=dtypes)

                            df.rename(columns=str.strip, inplace=True)  # remove any whitespace
                            sets_df[set_id] = DataSet(entry['name'], entry['path'],
                                                      start_date=entry['start_date'], end_date=entry['end_date'], df=df)
                        else:
                            context.log.warn(f'No type match for {entry["path"]}')
                    except IOError as ioe:
                        context.log.warn(f'Error loading {entry["path"]}: {ioe}')
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
def merge_promo_csv_file_sets(context, sets_list: List, sets_df: Dict, dtypes_by_root: Dict, regex_patterns: Dict):
    """
    Merge the promo file in all import sets
    :param context: execution context
    :param sets_list: list of, dictionaries of dictionaries of all the files in an import set;
                 [ {set_id1: [{'name': filename1_set1, 'path': path including filename1_set1},
                             {'name': filename2_set1, 'path': path including filename2_set1}, ...]},
                   {set_id2: [{'name': filename1_set2, 'path': path including filename1_set2},
                              {'name': filename2_set2, 'path': path including filename2_set2}, ...]}, ... ]
    :param sets_df: dict of DataSet with set ids as key
    :param dtypes_by_root: dict of dtypes dicts with root table identifier as the key
    :param regex_patterns: dict of regex pattern representing filenames and file sets
    :return: dict of data with set ids as key and DataSet as value
    """
    count = 0
    regex_patterns_dict = regex_patterns['value']
    regex_item = re.compile(regex_patterns_dict['set_sjpromo_pattern'])
    for set_entry in sets_list:  # dict in list
        for set_id in set_entry.keys():  # key in dict.keys
            for entry in set_entry[set_id]:  # DataSet in list
                if regex_item.search(entry['name']):

                    dtypes = {}
                    for key in dtypes_by_root.keys():
                        if regex_item.match(key):
                            dtypes = dtypes_by_root[key]
                            break

                    # found matching file, read it as DataFrame
                    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html#pandas.read_csv
                    df = pd.read_csv(entry['path'], dtype=dtypes)

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
                    merged = sets_df[set_id].df.merge(df, left_on=['ID', 'SEQUENCE'],
                                                      right_on=['SALESJOURNALID', 'SEQUENCE'],
                                                      how='left', suffixes=('_left', '_right'))
                    # in result SEQUENCE from the right is automatically dropped as its exactly the same as
                    # SEQUENCE from the left, but SALESJOURNALID from the right is included but not needed
                    # (same as ID from the left), so drop
                    merged = merged.drop(['SALESJOURNALID'], axis=1)
                    sets_df[set_id].df = merged

                    count += 1
                    break

    context.log.info(f'{count} promo DataFrames merged from {len(sets_list)} data sets')

    yield Output(sets_list, 'sets_list')
    yield Output(sets_df, 'sets_df')
