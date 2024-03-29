import json
from pprint import pprint
from span_reader.settings import *
from span_reader.datasource_mongo import DataSourceMongo
from pathlib import Path
from datetime import datetime

class InstrumentInfo(object):
    def __init__(self, optionenabled = 0, idinstrument = 0):
        self.fill_instrument_list(optionenabled, idinstrument)

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

    def fill_instrument_list(self, optionenabled, idinstrument):
        #print(MONGO_CONNSTR)

        data_source_mongo = DataSourceMongo(MONGO_CONNSTR, MONGO_EXO_DB)

        if optionenabled != 0:
            self.instrument_list = data_source_mongo.get_instrument_list(optionenabled)
        elif idinstrument != 0:
            self.instrument_list = data_source_mongo.get_instrument_list_from_id(idinstrument)

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
            try:
                instrument['exchange'] = self.exchange_dict[instrument['idexchange']]
            except:
                instrument['exchange'] = 0
                print("Exchange is not found: {0}".format(instrument['idexchange']))

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

                    if 'span_tick_configs' in json_instrumentconfig_data_dict[instrument['idinstrument']]:

                        instrument['span_tick_configs'] = json_instrumentconfig_data_dict[instrument['idinstrument']]['span_tick_configs']

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
                span_cqg_codes_dict_span_key[rows_in_span_cqg_codes['span']] = rows_in_span_cqg_codes

            '''fills the span_cqg_codes dictionary into the instrument dictionary'''
            instrument['span_cqg_codes_dict'] = span_cqg_codes_dict_span_key






    def update_instrument_list(self, instrument, span_date):
        """
            Updates instrument list with the changes for specific instruments
            Parameters:
              instrument - the instrument to update
              span_date - the date of the import
        """

        try:

            '''
            if instrument['idinstrument'] == 23:

                if span_date < datetime(2016, 2, 22):
                    instrument['spantickdisplay'] = 1

                if span_date < datetime(2016, 9, 9):
                    instrument['spanoptiontickdisplay'] = 1
            '''

            if 'span_tick_configs' in instrument:
                if 'spantickdisplay' in instrument['span_tick_configs']:

                    last_use_date = None
                    for rows_in_spantickdisplay in instrument['span_tick_configs']['spantickdisplay']:

                        use_date = datetime.strptime(rows_in_spantickdisplay['use_date'], '%Y-%m-%d')

                        if span_date >= use_date \
                                and (last_use_date == None or use_date >= last_use_date):
                            instrument['spantickdisplay'] = rows_in_spantickdisplay['value']
                            last_use_date = use_date

                if 'spanoptiontickdisplay' in instrument['span_tick_configs']:

                    last_use_date = None
                    for rows_in_spanoptiontickdisplay in instrument['span_tick_configs']['spanoptiontickdisplay']:

                        use_date = datetime.strptime(rows_in_spanoptiontickdisplay['use_date'], '%Y-%m-%d')

                        if span_date >= use_date \
                                and (last_use_date == None or use_date >= last_use_date):
                            instrument['spanoptiontickdisplay'] = rows_in_spanoptiontickdisplay['value']
                            last_use_date = use_date

                if 'spanoptionticksize' in instrument['span_tick_configs']:

                    last_use_date = None
                    for rows_in_spanoptiontickdisplay in instrument['span_tick_configs']['spanoptionticksize']:

                        use_date = datetime.strptime(rows_in_spanoptiontickdisplay['use_date'], '%Y-%m-%d')

                        if span_date >= use_date \
                                and (last_use_date == None or use_date >= last_use_date):
                            instrument['spanoptionticksize'] = rows_in_spanoptiontickdisplay['value']
                            last_use_date = use_date




        except:

            print("update_instrument_list error")