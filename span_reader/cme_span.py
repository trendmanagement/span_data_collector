"""
This is the main class for importing data from the CME SPAN FILES
"""
import os.path
import ntpath
import pprint
import json
import csv
import pandas as pd
from span_reader.instrument_info import InstrumentInfo
from span_reader.span_objects import *
from span_reader.option_calcs import calculateOptionVolatilityNR
from span_reader.mongo_queries import MongoQueries
import warnings


class CmeSpanImport(object):
    """
        The object that contains methods to import span data into mongodb
    """

    def __init__(self, args = None):


        if args != None:
            self.args = args
            self.optionenabled = args['optionenabled']
            self.testing = args['testing']

        else:
            self.optionenabled = 2
            self.testing = True



        self.mongo_queries = MongoQueries()

        self.instrumentInfo = InstrumentInfo(optionenabled=self.optionenabled)


    def load_span_file(self, filepath):
        """
            Reads and loads the span file into the mongodb
        """
        self.filepath = filepath
        self.short_file_name = ntpath.basename(self.filepath)

        #print(self.short_file_name)

        #print(self.filepath)

        if os.path.exists(self.filepath):

            self.filled_risk_free_rate = False

            file_object = open(self.filepath, 'r')

            try:
                file_lines = file_object.readlines()
            except:
                file_lines = []

                while True:
                    try:
                        line = file_object.readline()

                        while line:
                            try:
                                file_lines.append(line)
                                line = file_object.readline()
                            except:
                                warnings.warn("Can't Import File Line")
                                continue

                        break
                    except:
                        continue


            print(file_lines)

            #print('test ', self.filepath)

            data_row_type = SPAN_FILE_ROW_TYPES.TYPE_NULL

            for instrument in self.instrumentInfo.instrument_list:

                if instrument['idinstrument'] > 0:

                    if self.testing:
                        self.test_df = []

                    rowListTypeB = [];
                    rowListTypeB_Future_dict = {}; #used as dictionary for finding future by month and year
                    rowListType8_F = [];

                    rowListTypeB_Option_dict = {};  # used as dictionary for finding future by month and year
                    rowListType8_OOF = [];
                    #rowListType8_P = [];

                    rowListTypeMAIN = [];

                    print('running',instrument['symbol'])

                    for line in file_lines:

                        #print(line)

                        line_row_type = self.get_cme_line_type(line)

                        #print(line_row_type)

                        if line_row_type == SPAN_FILE_ROW_TYPES.TYPE_0:
                            #print(line)

                            if len(line) > 0:

                                self.extract_rowtype_0(line_in = line, instrument_symbol = instrument)

                            '''get the interest rate from the database after date is extracted from rowtype_0'''

                            if not self.filled_risk_free_rate:
                                try:
                                    self.risk_free_rate = self.mongo_queries.get_risk_free_rate(self.span_file_date_time)
                                    self.filled_risk_free_rate = True
                                except:
                                    self.risk_free_rate = 0.01
                                    warnings.warn("Can't find risk free rate for: {0}".format(self.span_file_date_time))
                                    continue

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
                                        (dst8e.commodity_product_code == instrument['spanfuturecode'] or dst8e.commodity_product_code == instrument['spanoptioncode']):

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
                                info_dict = \
                                {
                                    "contractname": row_dstBe_option_info.future_cqg_symbol,
                                    "expirationdate": row_dstBe_option_info.future_contract_expiration,
                                    "month": row_dstBe_option_info.future_contract_month_char,
                                    "idinstrument": row_dstBe_option_info.instrument['idinstrument'],
                                    "cqgsymbol": row_dstBe_option_info.future_cqg_symbol,
                                    "year": row_dstBe_option_info.future_contract_year,
                                    "monthint": row_dstBe_option_info.future_contract_month
                                    # "idcontract" : 1, # Not required
                                }

                                if self.testing:
                                    #pp = pprint.PrettyPrinter(indent=4)
                                    #pp.pprint(info_dict)
                                    row_dstBe_option_info.idcontract = 0

                                    #df = pd.DataFrame.from_dict(info_dict, orient="index")
                                    #if self.test_df is None:
                                    #    self.test_df = df
                                    #else:

                                    #x = str(info_dict)
                                    #print(self.test_df)

                                    self.test_df.append(str(info_dict))

                                    #print(self.test_df)

                                    #df.to_csv(instrument['symbol'] + "_data.csv")

                                else:
                                    contract_info_idcontract = self.mongo_queries.save_future_info(info_dict)

                                    row_dstBe_option_info.idcontract = contract_info_idcontract


                    '''gets future contract settlements'''
                    for row_dst_8_F_e_future_data in rowListType8_F:
                        #print('****** row_dst_8_F_e_future_data ' + row_dst_8_F_e_future_data.product_type)
                        #if row_dst_8_F_e_future_data.product_type == SPAN_FILE_PRODUCT_TYPE_CODES.fut:

                        row_dst_8_F_e_future_data.extract_future_identifiers()

                        #print('ticksize, display', instrument['spanticksize'], instrument['spantickdisplay'])

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
                        #self.mongo_queries.fill_future_price(row_dst_8_F_e_future_data, row_dstBe_future)

                        info_dict = \
                            {'idcontract': row_dstBe_future.idcontract,
                             'settlement': row_dst_8_F_e_future_data.settlement_price,
                             'openinterest': 0,
                             'volume': 0,
                             'date': row_dst_8_F_e_future_data.span_file_date_time}


                        #self.mongo_queries.save_futures_settlement(info_dict)

                        if self.testing:
                            #pp = pprint.PrettyPrinter(indent=4)
                            #pp.pprint(info_dict)

                            #df = pd.DataFrame.from_dict(info_dict, orient="index")
                            #self.test_df = self.test_df.append(df)
                            self.test_df.append(str(info_dict))
                        else:
                            self.mongo_queries.save_futures_settlement(info_dict)


                    '''below imports the OPTION contract info'''
                    for row_dstBe_option_info in rowListTypeB:

                        row_dstBe_option_info.extract_commodity_product_code_identifiers()

                        #if row_dstBe_option_info.commodity_product in \
                        #        row_dstBe_option_info.instrument['span_cqg_codes_dict']:
                        #    print('!@#',row_dstBe_option_info.instrument['span_cqg_codes_dict'][row_dstBe_option_info.commodity_product])

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



                    for row_dst_8_OOF_e_option_data in rowListType8_OOF:
                        #print('****** row_dst_8_F_e_future_data ' + row_dst_8_F_e_future_data.product_type)
                        #if row_dst_8_F_e_future_data.product_type == SPAN_FILE_PRODUCT_TYPE_CODES.oof:

                        row_dst_8_OOF_e_option_data.extract_option_identifiers_type8()

                        if row_dst_8_OOF_e_option_data.commodity_product_code in \
                                row_dst_8_OOF_e_option_data.instrument['span_cqg_codes_dict']:

                            row_dst_8_OOF_e_option_data.extract_future_identifiers()



                            key = (row_dst_8_OOF_e_option_data.option_contract_month, \
                                    row_dst_8_OOF_e_option_data.option_contract_year, \
                                    row_dst_8_OOF_e_option_data.commodity_product_code)


                            if key in rowListTypeB_Option_dict:

                                row_dstBe_option_info = \
                                    rowListTypeB_Option_dict[key]

                                #if dst8e.option_type == SPAN_FILE_CONTRACT_TYPE.call:

                                optionTickSize = instrument['spanoptionticksize']
                                optionTickDisplay = instrument['spanoptiontickdisplay']

                                if instrument['secondaryoptionticksizerule'] > 0:

                                    ratio = instrument['secondaryoptionticksize'] / instrument['spanoptionticksize']

                                    optionTickDisplay = ratio * instrument['spanoptiontickdisplay']

                                    optionTickSize = instrument['secondaryoptionticksize']


                                if data_row_type == SPAN_FILE_ROW_TYPES.TYPE_81:

                                    row_dst_8_OOF_e_option_data.extract_settlement_filetype81( \
                                        optionTickSize, \
                                        optionTickDisplay)

                                elif data_row_type == SPAN_FILE_ROW_TYPES.TYPE_82:

                                    row_dst_8_OOF_e_option_data.extract_settlement_filetype82( \
                                        optionTickSize, \
                                        optionTickDisplay)



                                #row_dstBe_option_info.span_underlying_future_contract_props.extracted_future_data_row \
                                #    .settlement_price
                                '''
                                print('$$$$$$$$$$',row_dstBe_option_info.product_type, row_dst_8_OOF_e_option_data.option_type, \
                                                                   row_dstBe_option_info.span_underlying_future_contract_props.extracted_future_data_row \
                                                                    .settlement_price, \
                                                                   row_dst_8_OOF_e_option_data.option_strike_price, \
                                                                   row_dstBe_option_info.option_time_to_exp, \
                                                                   self.risk_free_rate, \
                                                                   row_dst_8_OOF_e_option_data.settlement_price, \
                                                                   optionTickSize)
                                                                   '''

                                #calculate implied vol

                                row_dst_8_OOF_e_option_data.implied_vol = \
                                    calculateOptionVolatilityNR(row_dst_8_OOF_e_option_data.option_type, \
                                                                   row_dstBe_option_info.span_underlying_future_contract_props.extracted_future_data_row \
                                                                    .settlement_price, \
                                                                   row_dst_8_OOF_e_option_data.option_strike_price, \
                                                                   row_dstBe_option_info.option_time_to_exp, \
                                                                   self.risk_free_rate, \
                                                                   row_dst_8_OOF_e_option_data.settlement_price, \
                                                                   optionTickSize)

                                #print('^^^^^^^^^^^',row_dst_8_OOF_e_option_data.implied_vol)

                                option_info_dict = \
                                {
                                    "expirationdate": row_dstBe_option_info.option_contract_expiration,
                                    "idinstrument": row_dst_8_OOF_e_option_data.instrument['idinstrument'],
                                    "strikeprice": row_dst_8_OOF_e_option_data.option_strike_price,
                                    # "idoption" : 3,  # Not required
                                    "callorput": row_dst_8_OOF_e_option_data.option_type,
                                    "optionname": row_dst_8_OOF_e_option_data.option_cqg_symbol,
                                    "optionmonthint": row_dst_8_OOF_e_option_data.option_contract_month,
                                    "cqgsymbol": row_dst_8_OOF_e_option_data.option_cqg_symbol,
                                    "idcontract": row_dstBe_option_info.span_underlying_future_contract_props.idcontract,
                                    "optionmonth": row_dst_8_OOF_e_option_data.option_contract_month_char,
                                    "optionyear": row_dst_8_OOF_e_option_data.option_contract_year,
                                    "optioncode": row_dst_8_OOF_e_option_data.option_span_cqg_code['optcod']
                                }

                                #id_option = self.mongo_queries.save_option_info(option_info_dict)

                                id_option = 0

                                if self.testing:
                                    #pp = pprint.PrettyPrinter(indent=4)
                                    #pp.pprint(option_info_dict)

                                    #df = pd.DataFrame.from_dict(option_info_dict, orient="index")
                                    #self.test_df = self.test_df.append(df)

                                    option_info_dict_test = \
                                        {
                                            "strike_price_str": row_dst_8_OOF_e_option_data.option_strike_price_str,
                                            "expirationdate": row_dstBe_option_info.option_contract_expiration,
                                            "idinstrument": row_dst_8_OOF_e_option_data.instrument['idinstrument'],
                                            "strikeprice": row_dst_8_OOF_e_option_data.option_strike_price,
                                            # "idoption" : 3,  # Not required
                                            "callorput": row_dst_8_OOF_e_option_data.option_type,
                                            "optionname": row_dst_8_OOF_e_option_data.option_cqg_symbol,
                                            "optionmonthint": row_dst_8_OOF_e_option_data.option_contract_month,
                                            "cqgsymbol": row_dst_8_OOF_e_option_data.option_cqg_symbol,
                                            "idcontract": row_dstBe_option_info.span_underlying_future_contract_props.idcontract,
                                            "optionmonth": row_dst_8_OOF_e_option_data.option_contract_month_char,
                                            "optionyear": row_dst_8_OOF_e_option_data.option_contract_year,
                                            "optioncode": row_dst_8_OOF_e_option_data.option_span_cqg_code['optcod']
                                        }

                                    self.test_df.append(str(option_info_dict_test))
                                else:
                                    id_option = self.mongo_queries.save_option_info(option_info_dict)

                                #self.mongo_queries.fill_option_info_and_data(row_dst_8_OOF_e_option_data, \
                                #                                             row_dstBe_option_info)

                                option_data_dict = \
                                    {
                                        "timetoexpinyears" : row_dstBe_option_info.option_time_to_exp,
                                        "idoption" : id_option,
                                        "price" : row_dst_8_OOF_e_option_data.settlement_price,
                                        "datetime" : row_dst_8_OOF_e_option_data.span_file_date_time,
                                        "impliedvol" : row_dst_8_OOF_e_option_data.implied_vol
                                    }

                                #id_option = self.mongo_queries.save_options_data(option_data_dict)

                                if self.testing:
                                    #pp = pprint.PrettyPrinter(indent=4)
                                    #pp.pprint(option_data_dict)

                                    #df = pd.DataFrame.from_dict(option_data_dict, orient="index")
                                    #self.test_df = self.test_df.append(df)
                                    self.test_df.append(str(option_data_dict))
                                else:
                                    self.mongo_queries.save_options_data(option_data_dict)

                    #self.test_df.to_csv(instrument['symbol'] + "_data.csv")

                    #x = pd.DataFrame(self.test_df)
                    #x.to_csv(instrument['symbol'] + "_data.csv")

                    if self.testing:
                        thefile = open(instrument['symbol'] + "_data.txt", 'w')
                        for item in self.test_df:
                            print(item)
                            thefile.write("%s\n" % item)


                    print('finished', instrument['symbol'])



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



