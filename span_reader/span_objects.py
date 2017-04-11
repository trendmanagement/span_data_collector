from span_reader.span_input_constants import *
from span_reader.conversion import ConversionAndRounding
from datetime import datetime
#from decimal import Decimal
import logging

class SpanUnderlyingFutureProps:
    futureval = 1



class DataSpanType_8_Extract:
    '''this class is used to extract and store the line identifiers etc from row type 8'''

    def __init__(self, line, span_file_date_time, instrument):
        self.line = line
        self.span_file_date_time = span_file_date_time
        self.instrument = instrument
        '''
        self.row_exchg
        self.commodity_product_code
        self.underlying
        self.product_type
        self.option_type
        '''

        self.implied_vol = 0

    def extract_identifiers(self):
        '''extract row type 8 identifiers'''

        try:
            self.row_exchg = self.line[ \
                CME_SPAN_TYPE_8.row_exchange_start:CME_SPAN_TYPE_8.row_exchange_end].strip()

            self.commodity_product_code = self.line[ \
                CME_SPAN_TYPE_8.commodity_product_code_start: \
                    CME_SPAN_TYPE_8.commodity_product_code_end].strip()

            self.underlying = self.line[ \
                CME_SPAN_TYPE_8.underlying_start:CME_SPAN_TYPE_8.underlying_end].strip()

            self.product_type = self.line[ \
                CME_SPAN_TYPE_8.product_type_start:CME_SPAN_TYPE_8.product_type_end].strip()

        except:
            print("extract_rowtype_8 error")
            logging.exception("extract_rowtype_8  error")

    def extract_option_identifiers_type8(self):
        '''extracts option contract info'''

        try:
            self.option_type = self.line[
                CME_SPAN_TYPE_8.option_type_start:CME_SPAN_TYPE_8.option_type_end].strip()

            ##########################
            self.option_contract_month_str = self.line[ \
                                             CME_SPAN_TYPE_8.option_contract_month_start: \
                                                 CME_SPAN_TYPE_8.option_contract_month_end].strip()

            self.option_strike_price_str = self.line[ \
                                             CME_SPAN_TYPE_8.option_strike_price_start: \
                                                 CME_SPAN_TYPE_8.option_strike_price_end].strip()

            self.option_contract_month_date = datetime.strptime(self.option_contract_month_str, "%Y%m")

            self.option_contract_month = self.option_contract_month_date.month

            self.option_contract_month_char = SPAN_CONSTANTS.option_months[self.option_contract_month - 1]

            self.option_contract_year = self.option_contract_month_date.year

            #print(self.commodity_product_code)

            self.option_span_cqg_code = self.instrument['span_cqg_codes_dict'][self.commodity_product_code]





            self.option_strike_price = ConversionAndRounding.convertToStrikeFromSpanData(self, \
                    self.option_strike_price_str, \
                    self.instrument['optionstrikeincrement'], \
                    self.instrument['spanstrikedisplay'], \
                    self.instrument['idinstrument'], self.span_file_date_time)

            #print("&&&&&&&&&&&&&&&&&&& " + str(self.option_strike_price))



            '''self.option_cqg_symbol = self.option_type + ".US." + self.instrument['cqgsymbol'] \
                                     + self.option_contract_month_char + str(self.option_contract_year % 100) \
                                     + str(ConversionAndRounding.convertToStrikeForCQGSymbol(self, \
                                                              self.option_strike_price, \
                                                              self.instrument['optionstrikeincrement'], \
                                                              self.instrument['spanstrikedisplay'], \
                                                              self.instrument['idinstrument']))
'''

            self.option_cqg_symbol = '{0}.US.{1}{2}{3}{4}'.format(self.option_type, self.option_span_cqg_code['cqg'], #self.instrument['cqgsymbol'],
                                     self.option_contract_month_char, str(self.option_contract_year % 100),
                                     str(ConversionAndRounding.convertToStrikeForCQGSymbol(self,
                                                              self.option_strike_price,
                                                              self.instrument['optionstrikeincrement'],
                                                              self.instrument['optionstrikedisplay'],
                                                              self.instrument['idinstrument'], is_cme_data = True)))

        except:
            print("extract_rowtype_8 error")
            logging.exception("extract_rowtype_8  error")

    def extract_future_identifiers(self):
        '''extracts future contract info'''

        try:
            self.future_contract_month_str = self.line[ \
                                          CME_SPAN_TYPE_8.future_contract_month_start: \
                                              CME_SPAN_TYPE_8.future_contract_month_end].strip()

            #############################
            self.future_contract_month_date = datetime.strptime(self.future_contract_month_str, "%Y%m")

            self.future_contract_month = self.future_contract_month_date.month

            self.future_contract_month_char = SPAN_CONSTANTS.option_months[self.future_contract_month - 1]

            self.future_contract_year = self.future_contract_month_date.year
        except:
            print("extract_rowtype_8 error")
            logging.exception("extract_rowtype_8  error")

    def extract_settlement_filetype81(self, tick_size, tick_display):
        '''extracts future price from file type 81'''

        try:
            settlement_price_str = self.line[ \
                                   CME_SPAN_TYPE_8_1.settlement_price_start: \
                                       CME_SPAN_TYPE_8_1.settlement_price_end].strip()

            #print('settlement_price_str', settlement_price_str)

            self.settlement_price = self.settlement_find_closest_tick(settlement_price_str, tick_size, tick_display)

            #print('^^^settlement_price', self.settlement_price)
        except:
            print("extract_rowtype_8 error")
            logging.exception("extract_rowtype_8  error")

    def extract_settlement_filetype82(self, tick_size, tick_display):
        '''extracts future price from file type 81'''

        try:
            settlement_price_str = self.line[ \
                CME_SPAN_TYPE_8_2.settlement_price_start: \
                    CME_SPAN_TYPE_8_2.settlement_price_end].strip()

            #print('settlement_price_str', settlement_price_str)

            self.settlement_price = self.settlement_find_closest_tick(settlement_price_str, tick_size, tick_display)

            #print('^^^settlement_price', self.settlement_price)
        except:
            print("extract_rowtype_8 error")
            logging.exception("extract_rowtype_8  error")

    def settlement_find_closest_tick(self, settlement_price_str, tick_size, tick_display):
        '''
        extracts future price from file type 81
        :param settlement_price_str: string value of settlement
        :return:
        '''

        try:

            settlement_price_dec = float(settlement_price_str)

            #print("settlement_price_dec ", settlement_price_dec)

            if settlement_price_dec >= 9999999:
                return self.instrument['spanticksize']
            elif settlement_price_dec == 0:
                return 0
            else:
                return ConversionAndRounding.convertToTickMovesDouble(self, \
                    settlement_price_dec, \
                        tick_size, \
                        tick_display)

        except:
            print("extract_rowtype_8 error")
            logging.exception("extract_rowtype_8  error")


class DataSpanType_B_Extract:
    '''this class is used to extract and store the line identifiers etc from row type B'''

    def __init__(self, line, instrument):
        self.line = line
        self.instrument = instrument
        '''
        self.row_exchg
        self.underlying
        self.commodity_product
        self.product_type
        self.future_contract_month_str
        self.future_contract_day_or_week_code
        self.expiration_str
        self.future_contract_month_date
        self.future_contract_month
        self.future_contract_month_char
        self.future_contract_year
        self.future_contract_expiration
        self.future_cqg_symbol
        '''

    def extract_identifiers(self):

        try:
            self.row_exchg = self.line[ \
                             CME_SPAN_TYPE_B.row_exchange_start:CME_SPAN_TYPE_B.row_exchange_end].strip()

            self.underlying = self.line[ \
                              CME_SPAN_TYPE_B.underlying_start:CME_SPAN_TYPE_B.underlying_end].strip()
        except:
            print("extract_rowtype_B error")
            logging.exception("extract_rowtype_B  error")

    def extract_commodity_product_code_identifiers(self):
        '''extract commodity product code identifiers'''
        try:
            self.commodity_product = self.line[ \
                CME_SPAN_TYPE_B.commodity_product_code_start:CME_SPAN_TYPE_B.commodity_product_code_end].strip()

            self.product_type = self.line[ \
                CME_SPAN_TYPE_B.product_type_start:CME_SPAN_TYPE_B.product_type_end].strip()
        except:
            print("extract_rowtype_B error")
            logging.exception("extract_rowtype_B  error")

    def extract_future_identifiers(self):
        '''extracts future contract info'''
        try:
            self.future_contract_month_str = self.line[ \
                CME_SPAN_TYPE_B.future_contract_month_start: \
                    CME_SPAN_TYPE_B.future_contract_month_end].strip()

            self.future_contract_day_or_week_code = self.line[ \
                CME_SPAN_TYPE_B.future_contract_day_or_week_code_start: \
                    CME_SPAN_TYPE_B.future_contract_day_or_week_code_end].strip()

            #self.future_expiration_str = self.line[ \
            #    CME_SPAN_TYPE_B.expiration_start: \
            #        CME_SPAN_TYPE_B.expiration_end].strip()

            self.future_contract_month_date = datetime.strptime(self.future_contract_month_str, "%Y%m")

            self.future_contract_month = self.future_contract_month_date.month

            self.future_contract_month_char = SPAN_CONSTANTS.option_months[self.future_contract_month - 1]

            self.future_contract_year = self.future_contract_month_date.year

            #self.future_contract_expiration = datetime.strptime(self.future_expiration_str, "%Y%m%d")

            self.future_cqg_symbol = SPAN_FILE_CONTRACT_TYPE.future + "." + self.instrument['cqgsymbol'] \
                + self.future_contract_month_char + str(self.future_contract_year % 100)

            #self.future_span_symbol = SPAN_FILE_CONTRACT_TYPE.future + "." + self.instrument['spanfuturecode'] \
            #    + self.future_contract_month_char + str(self.future_contract_year % 100)
        except:
            print("extract_rowtype_B error")
            logging.exception("extract_rowtype_B  error")

    def extract_future_expiration(self):
        '''
        extracts future expiration
        this is separated out so that we can use this same class for options and futures extracted from the Type B file
        :return:
        '''
        try:
            self.future_expiration_str = self.line[ \
                CME_SPAN_TYPE_B.expiration_start: \
                    CME_SPAN_TYPE_B.expiration_end].strip()

            self.future_contract_expiration = datetime.strptime(self.future_expiration_str, "%Y%m%d")

        except:
            print("extract_rowtype_B error")
            logging.exception("extract_rowtype_B  error")


    def extract_option_identifiers_typeB(self):
        '''
        extracts option contract info
        :return:
        '''
        try:
            self.option_contract_month_str = self.line[ \
                CME_SPAN_TYPE_B.option_contract_month_start: \
                    CME_SPAN_TYPE_B.option_contract_month_end].strip()

            self.option_contract_day_or_week_code = self.line[ \
                CME_SPAN_TYPE_B.option_contract_day_or_week_code_start: \
                    CME_SPAN_TYPE_B.option_contract_day_or_week_code_end].strip()

            self.option_contract_month_date = datetime.strptime(self.option_contract_month_str, "%Y%m")

            self.option_contract_month = self.option_contract_month_date.month

            self.option_contract_month_char = SPAN_CONSTANTS.option_months[self.option_contract_month - 1]

            self.option_contract_year = self.option_contract_month_date.year

            #self.option_contract_expiration = datetime.strptime(self.expiration_str, "%Y%m%d")

            self.option_span_cqg_code = self.instrument['span_cqg_codes_dict'][self.commodity_product]


        except:
            print("extract_rowtype_B error")
            logging.exception("extract_rowtype_B  error")


    def extract_option_expiration_and_timetoexp(self):
        '''
        extracts option expiration
        :return:
        '''
        try:
            self.option_expiration_str = self.line[ \
                CME_SPAN_TYPE_B.expiration_start: \
                    CME_SPAN_TYPE_B.expiration_end].strip()

            self.option_time_to_exp_str = self.line[ \
                CME_SPAN_TYPE_B.time_to_expiration_start: \
                    CME_SPAN_TYPE_B.time_to_expiration_end].strip()

            self.option_contract_expiration = datetime.strptime(self.option_expiration_str, "%Y%m%d")

            self.option_time_to_exp = round(float(self.option_time_to_exp_str)/1000000,5)

            #print('self.time_to_exp', self.time_to_exp)

        except:
            print("extract_rowtype_B error")
            logging.exception("extract_rowtype_B  error")

