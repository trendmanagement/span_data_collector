import warnings
import pymongo

class DataSourceMongo(object):
    def __init__(self, conn_str, dbname):
        """
            Gets the instrument list from the instrument collection
            Parameters:
              conn_str - the connection string to the db
              dbname - the name of the db in mongo
        """
        self.client = pymongo.MongoClient(conn_str)
        self.db = self.client[dbname]

        # Creating indexes for fast data fetching
        self.db.futures_contract_settlements.create_index([('idcontract', pymongo.ASCENDING),('datetime', pymongo.ASCENDING)])

    def get_instrument_list(self, option_enabled):
        """
            Gets the instrument list from the instrument collection
            Parameters:
              option_enabled - the option_enabled field in the db
        """
        try:
            instrument_list_cursor = self.db.instruments.find({'optionenabled':option_enabled})

            instrument_list = []

            for instrument in instrument_list_cursor:
                instrument_list.append(instrument)

            return instrument_list

        except:
            return None

    def get_instrument_list_from_id(self, idinstrument):
        """
            Gets the instrument list from the instrument collection
            Parameters:
              option_enabled - the option_enabled field in the db
        """
        try:
            instrument_list_cursor = self.db.instruments.find({'idinstrument':idinstrument})

            instrument_list = []

            for instrument in instrument_list_cursor:
                instrument_list.append(instrument)

            return instrument_list

        except:
            return None

    def get_exchange_list(self):
        """
            Gets the exchange list from mongodb and create a dictionary with idexchange key
            Parameters:
              option_enabled - the option_enabled field in the db
        """
        try:

            exchange_dict = {}

            cur = self.db.exchange.find()

            for row in cur:
                exchange_dict[row['idexchange']] = row
                print(exchange_dict[row['idexchange']])

            return exchange_dict

        except:
            return exchange_dict

    def get_expiration_date(self, idinstrument, year, monthint, contr_type):
        """
        Gets contract expiration for particular future or option contract
        :param idinstrument:
        :param year:
        :param monthint:
        :param contr_type:
        :return:
        """
        result = self.db.contractexpirations.find_one({'idinstrument': idinstrument,
                                          'optionyear': year,
                                          'optionmonthint': monthint,
                                          'contracttype': contr_type
                                          })
        if result is None:
            if year > 2012:
                warnings.warn('Expiration not found for: '
                              'Instrument {0} Year: {1} Month: {2} ContrType: {3}'.format(idinstrument,
                                                                                          year,
                                                                                          monthint,
                                                                                          contr_type))
            return None
        else:
            return result['expirationdate']