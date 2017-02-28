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

        self.instrumentInfo = InstrumentInfo(idinstrument=self.idinstrument)


    def load_span_file(self, futures_filepath, options_filepath):
        """
            Reads and loads the span file into the mongodb
        """
        self.futures_filepath = futures_filepath
        self.options_filepath = options_filepath
        #self.short_file_name = ntpath.basename(self.futures_filepath)

        #print(self.short_file_name)

        #print(self.futures_filepath)


        if os.path.exists(self.futures_filepath) and os.path.exists(self.options_filepath):

            futures_object = pd.read_csv(self.futures_filepath, error_bad_lines=False)

            options_object = pd.read_csv(options_filepath, error_bad_lines=False)

            instrument = self.instrumentInfo.instrument_list[0]

            '''
            notes: on my local machine i created a collection counters that i seeded with the the current max values
            of idcontract and idoption

            1. get future contracts
            2. there are no expiration dates in these files, so we have a table tblcontractexpirations that we use to
             to get the expiration dates of the contracts, we will have to move this table to mongo
            3. fill the contracts collection with the future info (create the correct symbol for CQG, future data into the appropriate fields in mongo
            4. go through the options, find the underlying future (we can't always use the UnderlyingMarketID from the
             option file because they don't link to a future for serial month options)
            5. calculate the timetoexpinyears and the impliedvol

            updated notes are in the word doc


            '''





