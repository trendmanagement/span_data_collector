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
from span_reader.option_calcs import calculateOptionVolatilityNR, to_expiration_years
from span_reader.mongo_queries import MongoQueries
from span_reader.datasource_mongo import DataSourceMongo
from span_reader.settings import *
import warnings
import numpy as np


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

    def __init__(self, args=None):

        #if args != None:
        #    self.args = args
        #    self.idinstrument = args['optionenabled']

        #else:
        #    self.idinstrument = 36

        assert (ICE_INSTRUMENT_ID != None)

        self.idinstrument = ICE_INSTRUMENT_ID

        self.mongo_queries = MongoQueries()
        self.datasource = DataSourceMongo(MONGO_CONNSTR, MONGO_EXO_DB)

        self.instrumentInfo = InstrumentInfo(idinstrument=self.idinstrument).instrument_list[0]

        self.serial_months_map = {}
        """Map unknown futures MarketIDs (for serial options) to next available future month MarketID"""

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
                                            ConversionAndRounding.convertToStrikeForCQGSymbol(self,
                                                                                              df_row['StrikePrice'],
                                                                                              self.instrumentInfo[
                                                                                                  'optionstrikeincrement'],
                                                                                              self.instrumentInfo[
                                                                                                  'optionstrikedisplay'],
                                                                                              self.instrumentInfo[
                                                                                                  'idinstrument'],
                                                                                              use_json_cfg=True)
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
        futures_df['MarketID'] = futures_df['MarketID'].astype(str)
        grp = futures_df.groupby(by='MarketID').last()
        grp['year'] = grp['StripName'].apply(self.get_year)
        grp['month'] = grp['StripName'].apply(self.get_code_by_month)
        grp['monthint'] = grp['month'].apply(self.get_month_by_code)
        grp['expiration'] = grp.apply(self.get_expiration_date, axis=1, args=(1,))
        grp['cqgname'] = grp.apply(self.get_cqg_name_futures, axis=1)
        grp['contractname'] = grp.apply(self.get_contract_name_futures, axis=1)
        grp = grp.dropna(subset=['StripName'])
        return grp

    def save_futures_info(self, futures_info):
        """
        {
            "contractname" : "F.US.CLEM83",
            "expirationdate" : ISODate("1983-05-20T00:00:00.000Z"),
            "month" : "M",
            "idinstrument" : 21,
            "cqgsymbol" : "F.CLEM83",
            "year" : 1983,
            "monthint" : 6
            #"idcontract" : 1, # Not required
        }
        """
        for fdf_idx, fdict in futures_info.iterrows():
            if pd.isnull(fdict['expiration']):
                continue
            info_dict = {
                "contractname": fdict['contractname'],
                "expirationdate": fdict['expiration'],
                "month": fdict['month'],
                "idinstrument": self.instrumentInfo['idinstrument'],
                "cqgsymbol": fdict['cqgname'],
                "year": fdict['year'],
                "monthint": fdict['monthint'],
            }
            contract_id = self.mongo_queries.save_future_info(info_dict)

            assert contract_id is not None
            futures_info.at[fdf_idx, 'idcontract'] = contract_id

    def process_options_info(self, options_df):
        """
        Aggregate information for options contracts to meta-data dataframe
        :param options_df:
        :return:
        """

        def option_id_fix(df_row):
            """Fixed absent OptionMarketID for some products """
            return "{0}_{1}_{2}_{3}".format(df_row['ProductID'], df_row['OptionType'], df_row['StripName'],
                                            df_row['StrikePrice'])

        options_df['UnderlyingMarketID'] = options_df['UnderlyingMarketID'].astype(str)
        options_df['OptionMarketID'] = options_df.apply(option_id_fix, axis=1)

        grp_opt = options_df.groupby(by='OptionMarketID').last()
        grp_opt['year'] = grp_opt['StripName'].apply(self.get_year)
        grp_opt['month'] = grp_opt['StripName'].apply(self.get_code_by_month)
        grp_opt['monthint'] = grp_opt['month'].apply(self.get_month_by_code)
        grp_opt['expiration'] = grp_opt.apply(self.get_expiration_date, axis=1, args=(2,))
        grp_opt['contractname'] = grp_opt.apply(self.get_cqg_name_options, axis=1)
        grp_opt['cqgname'] = grp_opt.apply(self.get_cqg_name_options, axis=1)
        return grp_opt

    def save_options_info(self, options_info, futures_info):

        for odf_idx, odict in options_info.iterrows():
            try:
                fut_contract_data = futures_info.loc[odict['UnderlyingMarketID']]
                fut_contract_id = fut_contract_data['idcontract']
            except KeyError:
                try:
                    fut_contract_data = futures_info[(futures_info.year >= odict.year) & (futures_info.monthint > odict.monthint)].iloc[0]
                    fut_contract_id = fut_contract_data['idcontract']
                    if odict['UnderlyingMarketID'] in self.serial_months_map:
                        assert self.serial_months_map[odict['UnderlyingMarketID']] == fut_contract_data.name, 'Unexpected UnderlyingMarketID map'
                    else:
                        self.serial_months_map[odict['UnderlyingMarketID']] = fut_contract_data.name

                    warnings.warn("Apply closest futures series for {2} Option: {0} to Future: {1}".format(odict['StripName'],
                                                                                                   fut_contract_data[
                                                                                                       'StripName'],
                                                                                                   fut_contract_data["ProductName"]))
                except:
                    warnings.warn("Can't find closest future month for option: {0}".format(odict['contractname']))
                    continue

            options_info.at[odf_idx, 'idcontract'] = fut_contract_id

            if pd.isnull(odict['expiration']):
                continue

            info_dict = {
                "expirationdate": odict['expiration'],
                "idinstrument": self.instrumentInfo['idinstrument'],
                "strikeprice": odict['StrikePrice'],
                "callorput": odict['OptionType'].upper(),
                "optionname": odict['contractname'],
                "optionmonthint": odict['monthint'],
                "cqgsymbol": odict['cqgname'],
                "idcontract": fut_contract_id,
                "optionmonth": odict['month'],
                "optionyear": odict['year'],
            }
            option_id = self.mongo_queries.save_option_info(info_dict)

            assert option_id is not None
            options_info.at[odf_idx, 'idoption'] = option_id

    def save_futures_settlements(self, futures_df, futures_info):
        """
        Process futures dataframe and save settlements prices
        :param futures_df: raw quotes DataFrame
        :param futures_info: futures info DataFrame
        :return:
        """
        for fidx, fdict in futures_df.iterrows():
            settle_px = fdict['SettlementPrice']
            if np.isnan(settle_px):
                continue

            oi = fdict['OpenInterest']
            oi = 0 if np.isnan(oi) else oi
            volume = fdict['Volume']
            volume = 0 if np.isnan(volume) else volume
            dt = fidx
            if pd.isnull(dt):
                continue

            contract_id = futures_info.loc[fdict['MarketID']]['idcontract']
            assert contract_id is not None

            info_dict = {'idcontract': contract_id,
                         'settlement': settle_px,
                         'openinterest': oi,
                         'volume': volume,
                         'date': dt}
            self.mongo_queries.save_futures_settlement(info_dict)

    def save_options_quotes(self, options_df, options_info, futures_df):
        """
        Save options quotes
        :param options_df: options quotes DataFrame
        :param options_info: options info DataFrame
        :param futures_df: futures quotes DataFrame (used for fetching underlying prices)
        :return:
        """
        optionTickSize = self.instrumentInfo['spanoptionticksize']
        if self.instrumentInfo['secondaryoptionticksizerule'] > 0:
            optionTickSize = self.instrumentInfo['secondaryoptionticksize']

        for opt_current_date, odict in options_df.iterrows():
            opt_settle_px = odict['SettlementPrice']
            if np.isnan(opt_settle_px):
                continue

            option_info_data = options_info.loc[odict['OptionMarketID']]
            if 'idoption' not in option_info_data:
                warnings.warn("CRITICAL: Stopped... Couldn't find any single options' expiration. Try to check availability of "
                              "'contractsexpirations' collection in MongoDB: {0} at {1}".format(MONGO_EXO_DB, MONGO_CONNSTR))
                break
            idoption = option_info_data['idoption']
            option_expiration = option_info_data['expiration']
            if np.isnan(idoption):
                # Skip options without underlying futures
                continue

            assert idoption > 0

            if pd.isnull(option_expiration) or pd.isnull(opt_current_date):
                continue

            try:
                # Get serial options ID from serial_months_map otherwise use odict['UnderlyingMarketID']
                ulmktid = self.serial_months_map.get(odict['UnderlyingMarketID'], odict['UnderlyingMarketID'])
                fut_contract_data = futures_df[futures_df['MarketID'] == ulmktid].ix[
                    opt_current_date]
                underlying_px = fut_contract_data['SettlementPrice']
                if np.isnan(underlying_px):
                    continue
            except KeyError:
                continue

            '''get the interest rate from the database after date is extracted from rowtype_0'''

            try:
                risk_free_rate = self.mongo_queries.get_risk_free_rate(opt_current_date)
            except:
                risk_free_rate = 0.01
                warnings.warn("Can't find risk free rate for: {0}".format(opt_current_date))
                continue

            opt_timetoexp_in_years = to_expiration_years(option_expiration, opt_current_date)
            iv = calculateOptionVolatilityNR(odict['OptionType'],
                                             underlying_px,
                                             odict['StrikePrice'],
                                             opt_timetoexp_in_years,
                                             risk_free_rate,
                                             opt_settle_px,
                                             optionTickSize
                                             )

            assert iv >= 0
            assert not np.isnan(iv)

            info_dict = {
                "timetoexpinyears": opt_timetoexp_in_years,
                "idoption": idoption,
                "price": opt_settle_px,
                "datetime": opt_current_date,
                "impliedvol": iv,
            }
            self.mongo_queries.save_options_data(info_dict)

    def load_span_file(self, futures_filepath, options_filepath):
        """
            Reads and loads the span file into the mongodb
        """
        self.futures_filepath = futures_filepath
        self.options_filepath = options_filepath

        if os.path.exists(self.futures_filepath) and os.path.exists(self.options_filepath):

            futures_df = pd.read_csv(self.futures_filepath, error_bad_lines=False, parse_dates=[0],
                                     usecols=range(19), index_col=0)
            options_df = pd.read_csv(self.options_filepath, error_bad_lines=False, parse_dates=[0], index_col=0)

            # Process futures and options meta-information
            futures_info = self.process_futures_info(futures_df)
            options_info = self.process_options_info(options_df)
            # Saving info to mongo
            # and populating contracts IDs
            self.save_futures_info(futures_info)
            self.save_options_info(options_info, futures_info)

            # Processing settlements prices
            self.save_options_quotes(options_df, options_info, futures_df)
            self.save_futures_settlements(futures_df, futures_info)

        else:
            raise Exception("Files not found")
