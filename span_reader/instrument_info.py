import json
from pprint import pprint
from span_reader.settings import *
from span_reader.datasource_mongo import DataSourceMongo
from pathlib import Path
from datetime import datetime

class InstrumentInfo(object):
    def __init__(self, optionenabled):
        self.fill_instrument_list(optionenabled)

    def get_instrument_list(self):

        if self.instrument_list:
            return self.instrument_list
        else:
            return []

    def get_exchange_dict(self):

        if self.exchange_dict:
            return self.exchange_dict
        else:
            return {}

    def fill_instrument_list(self, optionenabled):
        #print(MONGO_CONNSTR)

        data_source_mongo = DataSourceMongo(MONGO_CONNSTR, MONGO_EXO_DB)

        self.instrument_list = data_source_mongo.get_instrument_list(optionenabled)

        #print(self.instrument_list)

        self.exchange_dict = data_source_mongo.get_exchange_list()

        cfg_file = Path(INSTRUMENT_CONFIG_JSON_FILE)

        '''check if cfg_file is a file before attempting to import'''
        if cfg_file.is_file():

            #print(INSTRUMENT_CONFIG_JSON_FILE)

            file_object = open(INSTRUMENT_CONFIG_JSON_FILE, 'r')

            json_instrumentconfig_data = json.load(file_object)

            json_instrumentconfig_data_dict = {}

            for instrument_config in json_instrumentconfig_data["instruments"]:

                json_instrumentconfig_data_dict[instrument_config["idinstrument"]] = instrument_config

                #print(json_instrumentconfig_data_dict)

                # json_instrumentconfig_data_dict = {}

        else:

            json_instrumentconfig_data_dict = {}


        for instrument in self.instrument_list:

            instrument['exchange'] = self.exchange_dict[instrument['idexchange']]

            #print(instrument['exchange'])



            if json_instrumentconfig_data_dict:

                if instrument['idinstrument'] in json_instrumentconfig_data_dict:
                    #print('*********')
                    #print(json_instrumentconfig_data_dict[instrument['idinstrument']])
                    #print('*********!!!!!')

                    if 'optionstrikeincrement' in json_instrumentconfig_data_dict[instrument['idinstrument']]:

                        instrument['optionstrikeincrement'] = json_instrumentconfig_data_dict[instrument['idinstrument']]['optionstrikeincrement']

                        # print('optionstrikeincrement    ', instrument['optionstrikeincrement'])

                    if 'optionstrikedisplay' in json_instrumentconfig_data_dict[instrument['idinstrument']]:

                        instrument['optionstrikedisplay'] = json_instrumentconfig_data_dict[instrument['idinstrument']]['optionstrikedisplay']

                        # print('optionstrikedisplay    ', instrument['optionstrikedisplay'])

                    if 'spanstrikedisplay' in json_instrumentconfig_data_dict[instrument['idinstrument']]:

                        instrument['spanstrikedisplay'] = json_instrumentconfig_data_dict[instrument['idinstrument']]['spanstrikedisplay']

                        # print('spanstrikedisplay    ', instrument['spanstrikedisplay'])

                    if 'span_cqg_codes' in json_instrumentconfig_data_dict[instrument['idinstrument']]:

                        instrument['span_cqg_codes'] = json_instrumentconfig_data_dict[instrument['idinstrument']]['span_cqg_codes']

                        # print('span_cqg_codes    ', instrument['span_cqg_codes'])


            '''below checks if the span_cqg_codes have been filled from the config file
            if not it will put in the default values'''
            if not 'span_cqg_codes' in instrument:
                instrument['span_cqg_codes'] = []

                default_cqg_codes = {
                    'cqg': instrument['cqgsymbol'],
                    'span': instrument['spanoptioncode'],
                    'optcod': " ",

                }

                instrument['span_cqg_codes'].append(default_cqg_codes)


            span_cqg_codes_dict_span_key = {}

            for rows_in_span_cqg_codes in instrument['span_cqg_codes']:
                #print(x['cqg'], x['span'], x['optcod'])
                span_cqg_codes_dict_span_key[rows_in_span_cqg_codes['span']] = rows_in_span_cqg_codes
                #print(span_cqg_codes_dict_span_key[x['span']])

            #print(span_cqg_codes_dict_span_key)
            # print(instrument['span_cqg_codes'])
            '''this is the '''
            instrument['span_cqg_codes_dict'] = span_cqg_codes_dict_span_key


    def update_instrument_list(self, instrument, span_date):
        """
            Updates instrument list with the changes for specific instruments
            Parameters:
              instrument - the instrument to update
              span_date - the date of the import
        """

        try:

            if instrument['idinstrument'] == 23:

                if span_date < datetime(2016, 2, 22):
                    instrument['spantickdisplay'] = 1

                if span_date < datetime(2016, 9, 9):
                    instrument['spanoptiontickdisplay'] = 1

        except:

            print("update_instrument_list error")