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
from datetime import datetime, timedelta, date
import xml.etree.ElementTree as eT

from dagster_pandas import DataFrame

from db_toolkit.misc import test_file_path
from dagster import (
    solid,
    String,
    List,
    Dict,
    Int,
    OutputDefinition,
    Output,
    lambda_solid,
    Optional,
    composite_solid, Bool)


@solid()
def read_currency_codes(context, cur_config: Dict, xml_path: String) -> DataFrame:
    """
    Read IS0 4217 currency codes
    :param context: execution context
    :param cur_config: currency configuration
    :param xml_path: path to file to read
    """
    cfg = cur_config['value']
    currency_codes_cfg = cfg['currency_codes']
    table_node = currency_codes_cfg['table_node']
    entry_node = currency_codes_cfg['entry_node']
    country_attrib = currency_codes_cfg['country_attrib']
    currency_name_attrib = currency_codes_cfg['currency_name_attrib']
    currency_code_attrib = currency_codes_cfg['currency_code_attrib']
    currency_nbr_attrib = currency_codes_cfg['currency_nbr_attrib']
    currency_minor_units_attrib = currency_codes_cfg['currency_minor_units_attrib']

    if not test_file_path(xml_path):
        raise ValueError(f"Invalid xml_path: {xml_path}")

    context.log.info(f"Reading '{xml_path}'")

    tree = eT.parse(xml_path)
    root = tree.getroot()

    columns = [country_attrib, currency_name_attrib, currency_code_attrib,
               currency_nbr_attrib, currency_minor_units_attrib]
    df = pd.DataFrame(dtype=str, columns=columns)

    # load into an array of dictionaries ignoring first row as it's not a user
    for entry in root.iter(entry_node):
        value = {}
        for xml_val in entry:
            value[xml_val.tag] = xml_val.text
        df = df.append(value, ignore_index=True)

    context.log.info(f"Read details of {len(df)} currencies")

    return df


@solid()
def read_imf_per_currency(context, cur_config: Dict, tsv_path: String) -> DataFrame:
    """
    Read an IMF SDR per currency file
    :param context: execution context
    :param cur_config: currency configuration
    :param tsv_path: path to file to read
    """
    cfg = cur_config['value']

    if not test_file_path(tsv_path):
        raise ValueError(f"Invalid tsv_path: {tsv_path}")

    context.log.info(f"Reading '{tsv_path}'")

    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html#pandas.read_csv
    df = pd.read_csv(tsv_path, encoding=cfg['encoding'], sep='\t', skiprows=2)

    # drop unnamed columns
    df = drop_unnamed_columns(df)

    return df


def drop_unnamed_columns(df: DataFrame, inplace: bool = False) -> DataFrame:
    """
    Drop columns beginning with 'Unnamed' from a DataFrame
    :param df: DataFrame to remove columns from
    :param inplace: Remove inplace flag
    :return: Updated DataFrame
    """
    # drop empty columns
    unnamed = []
    for column in df.columns:
        if column.startswith('Unnamed'):
            unnamed.append(column)

    if inplace:
        df.drop(unnamed, axis=1, inplace=True)
    else:
        df = df.drop(unnamed, axis=1, inplace=False)
    return df


@solid(
    output_defs=[
        OutputDefinition(dagster_type=DataFrame, name='currency_df', is_optional=False),
        OutputDefinition(dagster_type=Dict, name='currency_names', is_optional=False),
    ],
)
def transform_imf_currency_tsv(context, currency_df: DataFrame, cur_config: Dict):
    """
    Transform an IMF SDR per currency DataFrame
    :param context: execution context
    :param currency_df: DataFrame to process
    :param cur_config: currency configuration
    """
    cfg = cur_config['value']
    date_col_name = cfg['date_col_name']

    # clear whitespace
    currency_df.rename(columns=str.strip, inplace=True)  # remove any whitespace in column names
    for column in currency_df.columns:
        currency_df[column] = currency_df[column].str.strip()

    # make sure no dates are missing
    currency_date = datetime.strptime(cfg['currency_start_date'], cfg['to_datetime'])
    currency_end_date = datetime.strptime(cfg['currency_end_date'], cfg['to_datetime'])
    delta = timedelta(days=1)
    while currency_date <= currency_end_date:
        date_text = currency_date.strftime(cfg['to_datetime'])
        if date_text not in currency_df[date_col_name].values:
            currency_df = currency_df.append({date_col_name: date_text}, ignore_index=True)
        currency_date += delta

    # drop non-data rows in data column
    currency_df[[date_col_name]] = currency_df[[date_col_name]].fillna(value='')
    currency_df = currency_df[currency_df[date_col_name].str.contains(cfg['date_pattern'])]

    # convert dates and sort
    currency_df[date_col_name] = pd.to_datetime(currency_df[date_col_name], format=cfg['to_datetime'])
    currency_df = currency_df.sort_values(by=[date_col_name])

    # fill gaps with previous value
    for column in currency_df.columns:
        if column != date_col_name:
            currency_df[column] = currency_df[column].fillna(method='ffill')
    # and if the gap is on the first line, the next valid value
    for column in currency_df.columns:
        if column != date_col_name:
            currency_df[column] = currency_df[column].fillna(method='bfill')

    # convert floats
    for column in currency_df.columns:
        if column != date_col_name:
            currency_df[column] = currency_df[column].astype(float)

    # rename columns to currency code
    columns = []
    currency_names = {}
    regex = re.compile(cfg['currency_name_pattern'])
    for column in currency_df.columns:
        match = regex.search(column)
        if match:
            currency_names[match.group(2)] = match.group(1)
            columns.append(match.group(2))
        else:
            columns.append(column)

    currency_df.columns = columns

    yield Output(currency_df, 'currency_df')
    yield Output(currency_names, 'currency_names')


@composite_solid()
def read_sdr_per_currency() -> DataFrame:
    read_sdr_currencies = read_imf_per_currency.alias('read_sdr_currencies')

    return read_sdr_currencies()


@composite_solid(
    output_defs=[
        OutputDefinition(dagster_type=DataFrame, name='currency_df', is_optional=False),
        OutputDefinition(dagster_type=Dict, name='currency_names', is_optional=False),
    ],
)
def transform_sdr_per_currency(currency_df: DataFrame):
    transform_sdr_currencies = transform_imf_currency_tsv.alias('transform_sdr_currencies')

    return transform_sdr_currencies(currency_df)


@composite_solid()
def read_sdr_per_usd() -> DataFrame:
    read_usd_sdr = read_imf_per_currency.alias('read_usd_sdr')

    return read_usd_sdr()


@composite_solid(
    output_defs=[
        OutputDefinition(dagster_type=DataFrame, name='currency_df', is_optional=False),
        OutputDefinition(dagster_type=Dict, name='currency_names', is_optional=False),
    ],
)
def transform_usd_sdr(currency_df: DataFrame):
    transform_usd_sdr_currency = transform_imf_currency_tsv.alias('transform_usd_sdr_currency')

    return transform_usd_sdr_currency(currency_df)


@solid()
def read_sdr_valuation(context, cur_config: Dict, sdrv_path: String):
    # Doh! not required just use SDR's per USD

    # "Special drawing rights (abbreviated SDR, ISO 4217 currency code XDR (numeric: 960)[1]) are supplementary foreign
    #  exchange reserve assets defined and maintained by the International Monetary Fund (IMF).[2] SDRs are units of
    #  account for the IMF, and not a currency per se."
    # https://en.wikipedia.org/wiki/Special_drawing_rights

    # "The currency value of the SDR is determined by summing the values in U.S. dollars, based on market exchange
    #  rates, of a basket of major currencies (the U.S. dollar, Euro, Japanese yen, pound sterling and the Chinese
    #  renminbi). The SDR currency value is calculated daily (except on IMF holidays or whenever the IMF is closed
    #  for business) and the valuation basket is reviewed and adjusted every five years."
    # https://www.imf.org/external/np/fin/data/rms_sdrv.aspx

    cfg = cur_config['value']

    if not test_file_path(sdrv_path):
        raise ValueError(f"Invalid sdrv_path: {sdrv_path}")

    context.log.info(f"Reading '{sdrv_path}'")

    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html#pandas.read_csv
    df = pd.read_csv(sdrv_path, encoding=cfg['encoding'], sep='\t', skiprows=2, skip_blank_lines=True,
                     skipfooter=cfg['skipfooter'], engine='python')

    columns = cfg['sdrv_columns']
    df.columns = columns

    # layout of std info block
    # 02-Jan-1981	Deutsche mark	0.46	1.974	        0.233029	0
    # 02-Jan-1981	French franc	0.74	4.56	        0.162281	0
    # 02-Jan-1981	Japanese yen	34	    202.87	        0.167595	0
    # 02-Jan-1981	U.K. pound	    0.071	2.378	        0.168838	0
    # 02-Jan-1981	U.S. dollar	    0.54	1	            0.54	    0
    #                                                       1.271743
    #                                       U.S.$1.00 = SDR	0.786322
    #                                       SDR1 = US$	    1.27174

    # drop currency_unit, currency_amt & percent_change columns as not required
    # 02-Jan-1981	1.974	        0.233029
    # 02-Jan-1981	4.56	        0.162281
    # 02-Jan-1981	202.87	        0.167595
    # 02-Jan-1981	2.378	        0.168838
    # 02-Jan-1981	1	            0.54
    #                               1.271743
    #               U.S.$1.00 = SDR	0.786322
    #               SDR1 = US$	    1.27174
    df.drop([columns[1], columns[2], columns[5]], axis=1, inplace=True)

    # drop rows all nan or only 1 non-nan
    # 02-Jan-1981	1.974	        0.233029
    # 02-Jan-1981	4.56	        0.162281
    # 02-Jan-1981	202.87	        0.167595
    # 02-Jan-1981	2.378	        0.168838
    # 02-Jan-1981	1	            0.54
    #               U.S.$1.00 = SDR	0.786322
    #               SDR1 = US$	    1.27174
    df.dropna(thresh=2, inplace=True)
    df.reset_index(inplace=True, drop=True)

    # fill forward date nan's
    # 02-Jan-1981	1.974	        0.233029
    # 02-Jan-1981	4.56	        0.162281
    # 02-Jan-1981	202.87	        0.167595
    # 02-Jan-1981	2.378	        0.168838
    # 02-Jan-1981	1	            0.54
    # 02-Jan-1981	U.S.$1.00 = SDR	0.786322
    # 02-Jan-1981	SDR1 = US$	    1.27174
    date_col = columns[0]
    df[date_col] = df[date_col].fillna(method='ffill')

    # get the rows required
    # 02-Jan-1981	U.S.$1.00 = SDR	0.786322
    exchange_rate_col = columns[3]
    usd_eq_srdv = df[df[exchange_rate_col].str.contains(cfg['usd_eq_srdv'], regex=False)]

    # drop unrequired column
    usd_eq_srdv.drop([exchange_rate_col], axis=1, inplace=True)

    return usd_eq_srdv


@solid()
def read_ex_rates_per_usd(context, cur_config: Dict, ex_per_usd_path: String) -> DataFrame:
    """
    Read an 'IMF National Currency per U.S. Dollar, period average' file
    :param context: execution context
    :param cur_config: currency config
    :param ex_per_usd_path: path to file to read
    """
    cfg = cur_config['value']

    if not test_file_path(ex_per_usd_path):
        raise ValueError(f"Invalid ex_per_usd_path: {ex_per_usd_path}")

    context.log.info(f"Reading '{ex_per_usd_path}'")

    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html
    df = pd.read_excel(ex_per_usd_path, skiprows=5, header=1)

    # drop non-required columns
    df = drop_unnamed_columns(df, inplace=True)
    df.drop(['Scale', 'Base Year'], axis=1, inplace=True)

    return df


@solid()
def transform_ex_rates_per_usd(context, ex_rates_per_usd: DataFrame, currency_eq_usd_df: DataFrame,
                               currency_codes_df: DataFrame, cur_config: Dict):
    """

    :param context: execution context
    :param ex_rates_per_usd: DataFrame from an 'IMF National Currency per U.S. Dollar, period average' file
    :param currency_eq_usd_df: panda Dataframe of currency to USD rates
    :param currency_codes_df: IS0 4217 currency codes DataFrame
    :param cur_config: currency configuration
    :return:
    """
    cfg = cur_config['value']
    date_col_name = cfg['date_col_name']
    supplementary_currency_rates = cfg['supplementary_currency_rates']
    currency_codes_cfg = cfg['currency_codes']
    country_attrib = currency_codes_cfg['country_attrib']
    currency_code_attrib = currency_codes_cfg['currency_code_attrib']

    context.log.info(f'Generating list of currencies missing USD rates')

    # make list of missing currencies and add columns to the currency equivalent usd's dataframe
    missing_currencies = []
    for code in cfg['currencies_required']:
        if code not in currency_eq_usd_df.columns:
            currency_eq_usd_df[code] = np.nan
            missing_currencies.append({
                currency_code_attrib: code
            })

    # add temp columns with values to match ex_rates_per_usd column
    currency_eq_usd_df['year'] = currency_eq_usd_df[date_col_name].apply(lambda x: x.strftime('%Y'))
    currency_eq_usd_df['year_mth'] = currency_eq_usd_df[date_col_name].apply(lambda x: x.strftime('%YM%m'))
    currency_eq_usd_df['year_qtr'] = currency_eq_usd_df[date_col_name].apply(
        lambda x: x.strftime('%Y') + 'Q' + str(int((x.month / 3) + 1)))
    temp_period_columns = ['year_mth', 'year_qtr', 'year']

    context.log.info(f'Loading supplementary currency information')

    # add supplementary currency info to exchange rate per usd
    cidx_ex_rates_per_usd = ex_rates_per_usd.set_index(ex_rates_per_usd['Country'].str.lower())  # country name as index
    for code in supplementary_currency_rates.keys():
        suplm_currency = supplementary_currency_rates[code]
        for suplm_time in suplm_currency.keys():
            suplm_currency_value = suplm_currency[suplm_time]
            if suplm_time not in cidx_ex_rates_per_usd.columns:
                cidx_ex_rates_per_usd[suplm_time] = '...'

            country = currency_codes_df[currency_codes_df[currency_code_attrib] == code]
            if len(country) > 0:
                country = country.reset_index(drop=True)
                country_name = country.at[0, country_attrib].lower()

                if country_name not in cidx_ex_rates_per_usd.index:
                    # add new country and set index (as append resets previous set)
                    cidx_ex_rates_per_usd = cidx_ex_rates_per_usd.append({'Country': country_name}, ignore_index=True)
                    cidx_ex_rates_per_usd = cidx_ex_rates_per_usd. \
                        set_index(cidx_ex_rates_per_usd['Country'].str.lower())

                cidx_ex_rates_per_usd.at[country_name, suplm_time] = suplm_currency_value

    context.log.info(f'Updating list of currencies with missing USD rates')

    for missing in missing_currencies:
        currency_code = missing[currency_code_attrib]
        currency = currency_codes_df[currency_codes_df[currency_code_attrib] == currency_code]
        if len(currency) > 0:
            currency = currency.reset_index(drop=True)
            country_name = currency.at[0, country_attrib].lower()

            for alias in currency_codes_cfg['currency_name_aliases']:
                alias_lower = [x.lower() for x in alias]
                if country_name in alias_lower:
                    idx = alias_lower.index(country_name)
                    country_name = alias_lower[(idx + 1) % 2]     # 2 entries in list, get the one its not

            ex_rate_country = cidx_ex_rates_per_usd.loc[country_name]   # series of country ex rates

            # set currency values
            def get_time_col_value(col):
                value = np.nan
                if col in ex_rate_country.index:
                    value = ex_rate_country.at[col]
                    if not isinstance(value, float) and not isinstance(value, int):
                        value = np.nan
                return value

            not_filled_mask = None
            for time_col in temp_period_columns:
                # set values to value from time column
                if not_filled_mask is None:
                    currency_eq_usd_df[currency_code] = currency_eq_usd_df[time_col].apply(get_time_col_value)
                else:
                    currency_eq_usd_df.loc[currency_eq_usd_df[currency_code] == np.nan, currency_code] = \
                        currency_eq_usd_df[time_col].apply(get_time_col_value)

                not_filled_mask = currency_eq_usd_df[currency_code].isna()
                if not not_filled_mask.any():
                    break

    currency_eq_usd_df.drop(temp_period_columns, axis=1, inplace=True)

    return currency_eq_usd_df


@solid(
    output_defs=[
        OutputDefinition(dagster_type=DataFrame, name='currency_df', is_optional=False),
        OutputDefinition(dagster_type=Dict, name='currency_names', is_optional=False),
    ],
)
def transform_rates(context, usd_srd_df: DataFrame, currency_df: DataFrame, usd_name: Dict, currency_names: Dict,
                    usd_eq_srdv: DataFrame, cur_config: Dict):
    """

    :param context:
    :param usd_srd_df: SDR's per US$
    :param currency_df: SDR's per various currencies
    :param usd_name:
    :param currency_names:
    :param usd_eq_srdv:
    """
    cfg = cur_config['value']

    # convert to usd per sdr
    currency_df['USD'] = 1 / currency_df['USD']

    # convert SRD's per currency unit to USD per currency unit
    for column in currency_df.columns:
        if column != cfg['date_col_name'] and column != 'USD':
            currency_df[column] = currency_df[column] * currency_df['USD']

    yield Output(currency_df, 'currency_df')
    yield Output(currency_names, 'currency_names')
