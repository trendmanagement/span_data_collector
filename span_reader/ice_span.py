"""
This is the main class for importing data from the CME SPAN FILES
"""
import os.path
import ntpath
'''import logging
import json
from pprint import pprint
from span_reader.settings import *
from span_reader.datasource_mongo import DataSourceMongo
from datetime import datetime
from span_reader.span_input_constants import *
from pathlib import Path'''

import pandas as pd
from span_reader.instrument_info import InstrumentInfo
from span_reader.span_objects import *
from span_reader.option_calcs import calculateOptionVolatilityNR
from span_reader.mongo_queries import MongoQueries
from span_reader.datasource_mongo import DataSourceMongo
from span_reader.settings import *

class IceFutureInfo:
    """
    Mock class used to communicate with MongoQueries, has dict_base attributes
    """
    def __init__(self, info_dict):
        self.__dict__ = info_dict

class IceSpanImport(object):
    """
        The object that contains methods to import span data into mongodb
    """

    def __init__(self, args = None):

        if args != None:
            self.args = args
            self.idinstrument = args['optionenabled']
            self.risk_free_rate = args['risk_free_rate']

        else:
            self.idinstrument = 36
            self.risk_free_rate = 0.01

        self.mongo_queries = MongoQueries()
        self.datasource = DataSourceMongo(MONGO_CONNSTR, MONGO_EXO_DB)

        self.instrumentInfo = InstrumentInfo(idinstrument=self.idinstrument).instrument_list[0]

        # Raw data dataframe for futures
        self.futures_df = None
        self.options_df = None

        # Futures and options contracts information
        self.futures_info = None
        self.options_info = None

    def get_month_by_code(self, month_letter):
        """
        http://www.cmegroup.com/month-codes.html
        January	F
        February	G
        March	H
        April	J
        May	K
        June	M
        July	N
        August	Q
        September	U
        October	V
        November	X
        December	Z
        :param month_letter:
        :return:
        """
        month_letters = {
            'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6, 'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12}
        return month_letters[month_letter.upper()]

    def get_code_by_month(self, month_year_str):
        """
        Get month code from 'StripName' field May18 -> 'K'
        :param month_year_str:
        :return:
        """
        month_name = month_year_str[:3]
        month_letters = {
            'Jan': 'F',
            'Feb': 'G',
            'Mar': 'H',
            'Apr': 'J',
            'May': 'K',
            'Jun': 'M',
            'Jul': 'N',
            'Aug': 'Q',
            'Sep': 'U',
            'Oct': 'V',
            'Nov': 'X',
            'Dec': 'Z'}

        return month_letters[month_name]

    def get_year(self, month_year_str):
        """
        Get year from 'StripName' field May18 -> 2018
        :param month_year_str:
        :return:
        """
        yr = int(month_year_str[-2:])
        if yr > 50:
            return 1900 + yr
        else:
            return 2000 + yr

    def get_cqg_name_futures(self, df_row):
        """
        Produces CQG name of the futures
        """
        return 'F.{0}{1}{2}'.format(self.instrumentInfo['cqgsymbol'],
                                    df_row['month'],
                                    df_row['year'] % 100
                                    )

    def get_contract_name_futures(self, df_row):
        """
        Produces contract name of the future
        """
        return 'F.US.{0}{1}{2}'.format(self.instrumentInfo['cqgsymbol'],
                                       df_row['month'],
                                       df_row['year'] % 100
                                       )

    def get_cqg_name_options(self, df_row):
        """
        Produces contract and CQG (the same) name for the option
        :param df_row:
        :return:
        """
        return '{0}.US.{1}{2}{3}{4}'.format(df_row['OptionType'].upper(),
                                            self.instrumentInfo['cqgsymbol'],
                                            df_row['month'],
                                            df_row['year'] % 100,
                                            ConversionAndRounding.convertToStrikeForCQGSymbol(df_row['StrikePrice'],
                                                                                              self.instrumentInfo[
                                                                                                  'optionstrikeincrement'],
                                                                                              self.instrumentInfo[
                                                                                                  'spanstrikedisplay'],
                                                                                              self.instrumentInfo[
                                                                                                  'idinstrument']),
                                            )

    def get_expiration_date(self, df_row, contr_type):
        """
        Fill expirations for option or future contract
        :param df_row:
        :param contr_type:
        :return:
        """
        return self.datasource.get_expiration_date(self.instrumentInfo['idinstrument'],
                                                   df_row['year'],
                                                   df_row['monthint'],
                                                   contr_type)


    def process_futures_info(self, futures_df):
        """
        Aggregate information for futures contracts to meta-data dataframe
        :param futures_df:
        :return:
        """
        grp = futures_df.groupby(by='MarketID').last().sort_values('Date')
        grp['year'] = grp['StripName'].apply(self.get_year)
        grp['month'] = grp['StripName'].apply(self.get_code_by_month)
        grp['monthint'] = grp['month'].apply(self.get_month_by_code)
        grp['expiration'] = grp.apply(self.get_expiration_date, axis=1, args=(1,))
        grp['cqgname'] = grp.apply(self.get_cqg_name_futures, axis=1)
        grp['contractname'] = grp.apply(self.get_contract_name_futures, axis=1)
        grp = grp.dropna()
        return grp

    def process_options_info(self, options_df):
        """
        Aggregate information for options contracts to meta-data dataframe
        :param options_df:
        :return:
        """
        grp_opt = options_df.groupby(by='OptionMarketID').last().sort_values('Date')
        grp_opt['year'] = grp_opt['StripName'].apply(self.get_year)
        grp_opt['month'] = grp_opt['StripName'].apply(self.get_code_by_month)
        grp_opt['monthint'] = grp_opt['month'].apply(self.get_month_by_code)
        grp_opt['expiration'] = grp_opt.apply(self.get_expiration_date, axis=1, args=(2,))
        grp_opt['contractname'] = grp_opt.apply(self.get_cqg_name_options, axis=1)
        grp_opt['cqgname'] = grp_opt.apply(self.get_cqg_name_options, axis=1)
        return grp_opt

    def save_futures_info(self):
        if self.futures_info is None:
            raise Exception('Run process_futures_info() first')



    def load_span_file(self, futures_filepath, options_filepath):
        """
            Reads and loads the span file into the mongodb
        """
        self.futures_filepath = futures_filepath
        self.options_filepath = options_filepath

        if os.path.exists(self.futures_filepath) and os.path.exists(self.options_filepath):

            self.futures_df = pd.read_csv(self.futures_filepath, error_bad_lines=False, parse_dates=[0], usecols=[0,1,2,3])
            self.options_df = pd.read_csv(self.options_filepath, error_bad_lines=False, parse_dates=[0])

            self.futures_info = self.process_futures_info(self.futures_df)
            self.options_info = self.process_options_info(self.options_df)
            pass





