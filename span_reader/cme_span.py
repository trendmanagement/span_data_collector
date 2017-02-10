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

from span_reader.instrument_info import InstrumentInfo
from collections import namedtuple
from span_reader.span_objects import *
from span_reader.option_calcs import calculateOptionVolatilityNR
from span_reader.mongo_queries import MongoQueries


class CmeSpanImport(object):
    """
        The object that contains methods to import span data into mongodb
    """

    def __init__(self, args = None):

        if args != None:
            self.args = args
            self.filepath = args.filepath
            self.optionenabled = args.optionenabled
        else:
            self.filepath = "C:\\Users\\Steve Pickering\\Desktop\\span_data_collector\\cme.20170106.c.pa2"
            self.optionenabled = 2

        self.short_file_name = ntpath.basename(self.filepath)

        self.mongo_queries = MongoQueries()

        print(self.short_file_name)

        print(self.filepath)

        self.instrumentInfo = InstrumentInfo(self.optionenabled)

        self.risk_free_rate = 0.01


    def load_span_file(self):
        """
            Reads and loads the span file into the mongodb
        """



        if os.path.exists(self.filepath):

            file_object = open(self.filepath, 'r')

            file_lines = file_object.readlines()

            print('test ', self.filepath)

            data_row_type = SPAN_FILE_ROW_TYPES.TYPE_NULL

            for instrument in self.instrumentInfo.instrument_list:

                rowListTypeB = [];
                rowListTypeB_Future_dict = {}; #used as dictionary for finding future by month and year
                rowListType8_F = [];

                rowListTypeB_Option_dict = {};  # used as dictionary for finding future by month and year
                rowListType8_OOF = [];
                #rowListType8_P = [];

                rowListTypeMAIN = [];

                print(instrument['symbol'])

                for line in file_lines:

                    #print(line)

                    line_row_type = self.get_cme_line_type(line)

                    #print(line_row_type)

                    if line_row_type == SPAN_FILE_ROW_TYPES.TYPE_0:
                        #print(line)

                        if len(line) > 0:

                            self.extract_rowtype_0(line_in = line, instrument_symbol = instrument)

                        '''get the interest rate from the database after date is extracted from rowtype_0'''
                        self.interest_rate = 0

                        '''update the instrument info specific to the current date'''
                        self.instrumentInfo.update_instrument_list(instrument,self.span_file_date_time)

                        data_row_type = self.decide_data_rowtype_based_on_file_prefix()

                        #print("data_row_type " + str(data_row_type))

                    elif line_row_type == SPAN_FILE_ROW_TYPES.TYPE_B:
                        """
                            Extracts the B row type data out of the line
                        """
                        if len(line) > 108:

                            dstBe = DataSpanType_B_Extract(line, instrument)
                            dstBe.extract_identifiers()

                            if dstBe.row_exchg == instrument['exchange']['spanexchangesymbol'] and \
                                dstBe.underlying == instrument['spanfuturecode']:

                                rowListTypeB.append(dstBe)

                                #print("extract_rowtype_B ",
                                #      instrument['exchange']['spanexchangesymbol'],
                                #      instrument['spanfuturecode'],
                                #     line)

                    elif line_row_type == data_row_type:

                        if len(line) > 0:#CME_SPAN_TYPE_8.product_type_end - 1:

                            dst8e = DataSpanType_8_Extract(line, self.span_file_date_time, instrument)
                            dst8e.extract_identifiers()

                            #print("@@@" + dst8e.row_exchg)

                            if dst8e.row_exchg == instrument['exchange']['spanexchangesymbol'] and \
                                            dst8e.commodity_product_code == instrument['spanfuturecode']:

                                if dst8e.product_type == SPAN_FILE_PRODUCT_TYPE_CODES.fut:
                                    rowListType8_F.append(dst8e)

                                    #print("@@@" + dst8e.row_exchg)

                                elif dst8e.product_type == SPAN_FILE_PRODUCT_TYPE_CODES.oof:

                                    dst8e.extract_option_identifiers_type8()

                                    rowListType8_OOF.append(dst8e)

                                    #if dst8e.option_type == SPAN_FILE_CONTRACT_TYPE.call:


                                    #elif dst8e.option_type == SPAN_FILE_CONTRACT_TYPE.put:
                                    #    rowListType8_P.append(dst8e)

                    '''The row types have all been categorized. Now going through all the lists of the different line
                    types'''
                    '''
                    FILE_HEADER,
                    FUTURE_CONTRACT_INFO_IMPORT,
                    FUTURE_CONTRACT_DATA_IMPORT,
                    OPTION_CONTRACT_INFO_IMPORT,
                    OPTION_CALL_IMPORT,
                    OPTION_PUT_IMPORT
                    '''

                '''below imports the future contract info'''
                for row_dstBe_option_info in rowListTypeB:

                    row_dstBe_option_info.extract_commodity_product_code_identifiers()

                    if row_dstBe_option_info.product_type == SPAN_FILE_PRODUCT_TYPE_CODES.fut:

                        row_dstBe_option_info.extract_future_identifiers()

                        row_dstBe_option_info.extract_future_expiration()

                        rowListTypeB_Future_dict[row_dstBe_option_info.future_contract_month,row_dstBe_option_info.future_contract_year] = row_dstBe_option_info

                        if len(row_dstBe_option_info.future_expiration_str) > 0:

                            self.mongo_queries.fill_future_info(row_dstBe_option_info)

                            print('&&&&&&&&&&',
                                  row_dstBe_option_info.idcontract,
                                  row_dstBe_option_info.contract_objectid)


                '''gets future contract settlements'''
                for row_dst_8_F_e_future_data in rowListType8_F:
                    #print('****** row_dst_8_F_e_future_data ' + row_dst_8_F_e_future_data.product_type)
                    #if row_dst_8_F_e_future_data.product_type == SPAN_FILE_PRODUCT_TYPE_CODES.fut:

                    row_dst_8_F_e_future_data.extract_future_identifiers()

                    print('ticksize, display', instrument['spanticksize'], instrument['spantickdisplay'])

                    if data_row_type == SPAN_FILE_ROW_TYPES.TYPE_81:

                        row_dst_8_F_e_future_data.extract_settlement_filetype81( \
                            instrument['spanticksize'], instrument['spantickdisplay'])

                    elif data_row_type == SPAN_FILE_ROW_TYPES.TYPE_82:

                        row_dst_8_F_e_future_data.extract_settlement_filetype82( \
                            instrument['spanticksize'], instrument['spantickdisplay'])


                    row_dstBe_future = \
                        rowListTypeB_Future_dict[row_dst_8_F_e_future_data.future_contract_month, row_dst_8_F_e_future_data.future_contract_year]

                    row_dstBe_future.extracted_future_data_row = row_dst_8_F_e_future_data

                    #update future contract with settlement and date to mongo
                    self.mongo_queries.fill_future_price(row_dst_8_F_e_future_data, row_dstBe_future)


                '''below imports the OPTION contract info'''
                for row_dstBe_option_info in rowListTypeB:

                    row_dstBe_option_info.extract_commodity_product_code_identifiers()

                    if row_dstBe_option_info.commodity_product in \
                            row_dstBe_option_info.instrument['span_cqg_codes_dict']:
                        print('!@#',row_dstBe_option_info.instrument['span_cqg_codes_dict'][row_dstBe_option_info.commodity_product])

                    if row_dstBe_option_info.product_type == SPAN_FILE_PRODUCT_TYPE_CODES.oof and \
                        row_dstBe_option_info.commodity_product in \
                            row_dstBe_option_info.instrument['span_cqg_codes_dict']:

                        row_dstBe_option_info.extract_future_identifiers()

                        row_dstBe_option_info.extract_option_identifiers_typeB()

                        row_dstBe_option_info.extract_option_expiration_and_timetoexp()

                        row_dstBe_option_info.span_underlying_future_contract_props = \
                            rowListTypeB_Future_dict[row_dstBe_option_info.future_contract_month, row_dstBe_option_info.future_contract_year]

                        rowListTypeB_Option_dict[
                            row_dstBe_option_info.option_contract_month, \
                            row_dstBe_option_info.option_contract_year, \
                            row_dstBe_option_info.commodity_product] = row_dstBe_option_info

                        #TimeSpan span = optionContractMonthDate - spanOptionContractProps.optionContractExpiration;
                        #if (span.TotalDays < 150)
                        #        spanOptionContractPropsList.Add(spanOptionContractProps)

                        self.mongo_queries.fill_option_info(row_dstBe_option_info)

                for row_dst_8_OOF_e in rowListType8_OOF:
                    #print('****** row_dst_8_F_e_future_data ' + row_dst_8_F_e_future_data.product_type)
                    #if row_dst_8_F_e_future_data.product_type == SPAN_FILE_PRODUCT_TYPE_CODES.oof:

                    row_dst_8_OOF_e.extract_option_identifiers_type8()

                    if row_dst_8_OOF_e.commodity_product_code in \
                            row_dst_8_OOF_e.instrument['span_cqg_codes_dict']:

                        row_dst_8_OOF_e.extract_future_identifiers()



                        key = (row_dst_8_OOF_e.option_contract_month, \
                                row_dst_8_OOF_e.option_contract_year, \
                                row_dst_8_OOF_e.commodity_product_code)


                        if key in rowListTypeB_Option_dict:

                            row_dstBe_option = \
                                rowListTypeB_Option_dict[key]

                            #if dst8e.option_type == SPAN_FILE_CONTRACT_TYPE.call:

                            optionTickSize = instrument['spanoptionticksize']
                            optionTickDisplay = instrument['spanoptiontickdisplay']

                            if instrument['secondaryoptionticksizerule'] > 0:

                                ratio = instrument['secondaryoptionticksize'] / instrument['spanoptionticksize']

                                optionTickDisplay = ratio * instrument['spanoptiontickdisplay']

                                optionTickSize = instrument['secondaryoptionticksize']


                            if data_row_type == SPAN_FILE_ROW_TYPES.TYPE_81:

                                row_dst_8_OOF_e.extract_settlement_filetype81( \
                                    optionTickSize, \
                                    optionTickDisplay)

                            elif data_row_type == SPAN_FILE_ROW_TYPES.TYPE_82:

                                row_dst_8_OOF_e.extract_settlement_filetype82( \
                                    optionTickSize, \
                                    optionTickDisplay)



                            #row_dstBe_option.span_underlying_future_contract_props.extracted_future_data_row \
                            #    .settlement_price

                            print('$$$$$$$$$$',row_dstBe_option.product_type, row_dst_8_OOF_e.option_type, \
                                                               row_dstBe_option.span_underlying_future_contract_props.extracted_future_data_row \
                                                                .settlement_price, \
                                                               row_dst_8_OOF_e.option_strike_price, \
                                                               row_dstBe_option.option_time_to_exp, \
                                                               self.risk_free_rate, \
                                                               row_dst_8_OOF_e.settlement_price, \
                                                               optionTickSize)

                            #calculate implied vol

                            row_dst_8_OOF_e.implied_vol = \
                                calculateOptionVolatilityNR(row_dst_8_OOF_e.option_type, \
                                                               row_dstBe_option.span_underlying_future_contract_props.extracted_future_data_row \
                                                                .settlement_price, \
                                                               row_dst_8_OOF_e.option_strike_price, \
                                                               row_dstBe_option.option_time_to_exp, \
                                                               self.risk_free_rate, \
                                                               row_dst_8_OOF_e.settlement_price, \
                                                               optionTickSize)

                            print('^^^^^^^^^^^',row_dst_8_OOF_e.implied_vol)


                            '''print('********',
                                  row_dst_8_F_e_future_data.future_contract_month,
                                  '***',
                                  row_dst_8_F_e_future_data.future_contract_year,
                                  '***',
                                  row_dstBe_option_info.future_cqg_symbol,
                                  'settlement price',
                                  row_dst_8_F_e_future_data.settlement_price)'''

                            #update future contract with settlement and date to mongo

















    def get_cme_line_type(self, line = ''):
        """
            Gets the CME line type
            Parameters:
              line - the line of the file
        """
        rowType = SPAN_FILE_ROW_TYPES.TYPE_NULL

        try:

            if len(line) > 2:

                rowTypeString = line[0:2].strip()

                switcher = {
                    "0" : SPAN_FILE_ROW_TYPES.TYPE_0,
                    "81": SPAN_FILE_ROW_TYPES.TYPE_81,
                    "82": SPAN_FILE_ROW_TYPES.TYPE_82,
                    "B": SPAN_FILE_ROW_TYPES.TYPE_B,
                    "83": SPAN_FILE_ROW_TYPES.TYPE_83,
                }

                return switcher.get(rowTypeString, -1)

        except:

            print("rowtype error")

            return rowType



    def extract_rowtype_0(self, line_in = '', instrument_symbol = ''):
        """
            Extracts the 0 row type data out of the line
            Parameters:
              line - the line of the file
              instrument - the instrument that is importing
        """

        try:

            exchangeComplex = line_in[CME_SPAN_TYPE_0.exchange_complex_start:
                CME_SPAN_TYPE_0.exchange_complex_end].strip()

            # ccyyMMdd
            businessDate = line_in[CME_SPAN_TYPE_0.business_date_start:
                CME_SPAN_TYPE_0.business_date_end].strip()

            settlementOrIntraday = line_in[CME_SPAN_TYPE_0.settlement_or_intraday_start:
                CME_SPAN_TYPE_0.settlement_or_intraday_end].strip()

            fileIdentifier = line_in[CME_SPAN_TYPE_0.file_identifier_start:
                CME_SPAN_TYPE_0.file_identifier_end].strip()

            fileFormat = line_in[CME_SPAN_TYPE_0.file_format_start:
                CME_SPAN_TYPE_0.file_format_end].strip()

            self.span_file_date_time = datetime.strptime(businessDate,"%Y%m%d")

            #print(instrument_symbol['symbol'],
            #      self.span_file_date_time,
            #      exchangeComplex,
            #      businessDate,
            #      settlementOrIntraday,
            #      fileIdentifier,
            #      fileFormat)

        except:

            #print("extract_rowtype_0 error")
            logging.exception("extract_rowtype_0 error")


    def decide_data_rowtype_based_on_file_prefix(self):
        """
            Chooses the rowtype to use based on the prefix of the file name
        """

        if self.short_file_name[0:3] == "ccl":

            return SPAN_FILE_ROW_TYPES.TYPE_82

        elif self.short_file_name[0:3] == "cme":

            if self.span_file_date_time >= datetime(2010, 5, 5):

                return SPAN_FILE_ROW_TYPES.TYPE_81

            else:

                return SPAN_FILE_ROW_TYPES.TYPE_82

        elif self.short_file_name[0:3] == "nyb":

            return SPAN_FILE_ROW_TYPES.TYPE_82


