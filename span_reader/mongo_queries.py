from span_reader.settings import *
from pymongo import MongoClient, ReturnDocument
import pymongo
import pprint

COUNTER_FUTURES = 'idcontract'
COUNTER_OPTIONS = 'idoption'


class MongoQueries():
    def __init__(self, args=None):
        self.mongoclient = MongoClient(MONGO_CONNSTR_LOCAL)
        self.db = self.mongoclient[MONGO_EXO_DB_LOCAL]

        # Creating collection indexes for performance
        self.init_collection_indexes()

        self._set_max_counter_id('contracts', COUNTER_FUTURES)
        self._set_max_counter_id('options', COUNTER_OPTIONS)

    def init_collection_indexes(self):
        self.db['contracts'].create_index([('idinstrument', pymongo.ASCENDING),
                                           ('year', pymongo.ASCENDING),
                                           ('monthint', pymongo.ASCENDING)
                                           ])
        self.db['options'].create_index([('idinstrument', pymongo.ASCENDING),
                                         ('optionyear', pymongo.ASCENDING),
                                         ('optionmonthint', pymongo.ASCENDING),
                                         ('strikeprice', pymongo.ASCENDING),
                                         ('callorput', pymongo.ASCENDING),
                                         ])
        self.db['futures_contract_settlements'].create_index([('idcontract', pymongo.ASCENDING),
                                                              ('date', pymongo.ASCENDING)])

        self.db['options_data'].create_index([('idoption', pymongo.ASCENDING),
                                              ('datetime', pymongo.ASCENDING)])

    def _set_max_counter_id(self, collection, counter_name):
        """
        Read the collection's max counter id and populate counters collections with it
        :param collection: collection name
        :param counter_name: counter field name
        :return:
        """
        max_id_contract_agg = self.db[collection].aggregate(
            [{'$group': {'_id': 'null', 'max': {'$max': '$' + counter_name}}}])

        max_id_contract_list = list(max_id_contract_agg)
        if len(max_id_contract_list) == 0 or max_id_contract_list[0]['max'] is None:
            # Check if collection is empty initiate new counter from 1
            max_con = 1
        else:
            max_con = int(max_id_contract_list[0]['max'])
        self.db.counters.replace_one({'_id': counter_name}, {'seq': max_con}, upsert=True)


    def _get_new_counter_id(self, counter_name):
        """
        Increment counter value and return it
        :param counter_name:
        :return:
        """
        counter_doc = self.db.counters.find_one_and_update({'_id': counter_name}, {'$inc': {'seq': 1}},
                                                           return_document=ReturnDocument.AFTER)
        return counter_doc['seq']


    def _save_mongo_dict(self, collection, update_filter, info_dict, counter_name=None):
        """
        Universal save and update mongo method
        :param collection: collection name
        :param update_filter: list of keys of the 'info_dict' used for MongoDB find_and_update() filtration request
        :param info_dict: dictionary of MongoDB values as is
        :param counter_name: name of the unique contract ID counter
        :return: counter ID of the updated or upserted data
        """
        #
        # Populate filter request for Mongo
        #
        filter_request_dict = {}
        for k in update_filter:
            filter_request_dict[k] = info_dict[k]

        doc_update_message = self.db[collection].find_one_and_update(filter_request_dict,
                                                                     {'$set': info_dict},
                                                                     upsert=True,  # Insert non existing data!
                                                                     return_document=ReturnDocument.AFTER)
        if counter_name is None:
            return None
        else:
            if counter_name not in doc_update_message or doc_update_message[counter_name] is None:
                # Counter is not exists, we need to create new one and populate it to the contract data
                new_counter_id = self._get_new_counter_id(counter_name)
                self.db[collection].update({'_id': doc_update_message['_id']},
                                           {'$set': {counter_name: new_counter_id}})
                return new_counter_id
            else:
                return doc_update_message[counter_name]


    def save_future_info(self, info_dict):
        """
        Saves futures information to mongo
        :param info_dict: dictionary should contain at least
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
        :return:
        """
        return self._save_mongo_dict('contracts', ['monthint', 'year', 'idinstrument'], info_dict, COUNTER_FUTURES)


    def save_option_info(self, info_dict):
        """
        Saves option information to mongo
        :param info_dict: dictionary should contain at least
        {
            "expirationdate" : ISODate("2011-01-21T00:00:00.000Z"),
            "idinstrument" : 1,
            "strikeprice" : 91.0,
            #"idoption" : 3,  # Not required
            "callorput" : "C",
            "optionname" : "C.US.USAG119100",
            "optionmonthint" : 2,
            "cqgsymbol" : "C.US.USAG119100",
            "idcontract" : 738,
            "optionmonth" : "G",
            "optionyear" : 2011
            "optioncode" :
        }
        :return:
        """

        if ('optcod' in info_dict and info_dict['optcod'].strip() != ''):
            return self._save_mongo_dict('options',
                                         ['optionmonthint', 'optionyear', 'strikeprice', 'idinstrument', 'callorput','optcod'],
                                         info_dict, COUNTER_OPTIONS)
        else:
            return self._save_mongo_dict('options',
                                     ['optionmonthint', 'optionyear', 'strikeprice', 'idinstrument', 'callorput'],
                                     info_dict, COUNTER_OPTIONS)


    def save_futures_settlement(self, info_dict):
        """
        Saves futures contract settlements
        :param info_dict: dictionary should contain at least
             {'idcontract': 3678,
             'settlement': 134.0625,
             'openinterest': 0,
             'volume': 0,
             'date': datetime.datetime(2012, 12, 7, 0, 0)}
        :return:
        """
        return self._save_mongo_dict('futures_contract_settlements',
                                     ['idcontract', 'date'],
                                     info_dict)


    def save_options_data(self, info_dict):
        """
        Saves options quotes data
        :param info_dict:
        {
            "timetoexpinyears" : 0.42192,
            "idoption" : 11488838,
            "price" : 0.0,
            "datetime" : ISODate("2013-08-16T00:00:00.000Z"),
            "impliedvol" : 0.03442
        }
        :return:
        """
        return self._save_mongo_dict('options_data',
                                     ['idoption', 'datetime'],
                                     info_dict)


    def get_risk_free_rate(self, date):
        #oid_cur = self.db['option_input_data'].find({'idoptioninputsymbol':15,'optioninputdatetime':{'$lte':date}}) \
        #        .sort({'optioninputdatetime':-1}).limit(1)

        oid_cur = self.db['option_input_data'].find({'idoptioninputsymbol': 15,'optioninputdatetime':{'$lte':date}}).sort('optioninputdatetime',pymongo.DESCENDING).limit(1)

        ir = 0.01
        for oid_obj in oid_cur:
            ir = oid_obj['optioninputclose']/100
            break

        #print(ir)

        return ir

    #
    # OLD Code
    #
    #

    def fill_future_info(self, future_contract_info):
        doc_update_message = self.db.contracts.find_one_and_update(
            {'monthint': future_contract_info.future_contract_month, \
             'year': future_contract_info.future_contract_year, \
             'idinstrument': future_contract_info.instrument['idinstrument']}, \
            {'$set': {'cqgsymbol': future_contract_info.future_cqg_symbol, \
                      'contractname': future_contract_info.future_cqg_symbol, \
                      'expirationdate': future_contract_info.future_contract_expiration}}, \
            return_document=ReturnDocument.AFTER)

        print(doc_update_message)
        '''print(#doc_update_message['updatedExisting'],
              doc_update_message['idcontract'],
              'monthint', 13, \
                'year', row_dstBe.future_contract_year, \
                'idinstrument', row_dstBe.instrument['idinstrument'], \
                'cqgsymbol', row_dstBe.future_cqg_symbol, \
                          'contractname', row_dstBe.future_cqg_symbol, \
                          'expirationdate', row_dstBe.future_contract_expiration)
                          '''

        # if (doc_update_message['updatedExisting'] == True):
        if (doc_update_message != None):

            future_contract_info.idcontract = doc_update_message['idcontract']
            #future_contract_info.contract_objectid = doc_update_message['_id']

            print('%%%%%%%%%%%%', future_contract_info.idcontract)

        # if(doc_update_message['updatedExisting'] == False):
        else:
            print("999999999999999999", doc_update_message)

            idcontract_doc = \
                self.db.counters.find_one_and_update({'_id': 'idcontract'}, {'$inc': {'seq': 1}}, \
                                                     return_document=ReturnDocument.AFTER)

            print(idcontract_doc)

            _objectid = self.db.contracts.insert({'monthint': future_contract_info.future_contract_month, \
                                                  'month': future_contract_info.future_contract_month_char, \
                                                  'year': future_contract_info.future_contract_year, \
                                                  'idinstrument': future_contract_info.instrument['idinstrument'], \
                                                  'cqgsymbol': future_contract_info.future_cqg_symbol, \
                                                  'contractname': future_contract_info.future_cqg_symbol, \
                                                  'expirationdate': future_contract_info.future_contract_expiration, \
                                                  'idcontract': int(idcontract_doc['seq'])})

            print(_objectid)

            future_contract_info.idcontract = int(idcontract_doc['seq'])
            #future_contract_info.contract_objectid = _objectid

            print('2222%%%%%%%%%%%%', future_contract_info.idcontract)


    def fill_future_price(self, future_contract_data, future_contract_info):
        # doc = self.db.contracts.update({'monthint':6,'year':1983,'idinstrument':7777}, {'$set':{'cqgsymbol':'F.CLEM83'}}, upsert=True)

        print('&&&&&&&&&&',
              future_contract_info.idcontract,
              #future_contract_info.contract_objectid,
              future_contract_data.span_file_date_time,
              future_contract_data.settlement_price)

        doc_update_message = self.db.futures_contract_settlements.update({'idcontract': future_contract_info.idcontract, \
                                                                          'date': future_contract_data.span_file_date_time}, \
                                                                         {'$set': {
                                                                             'settlement': future_contract_data.settlement_price}}, \
                                                                         upsert=True)

        print('###############', doc_update_message)


    def fill_option_info_and_data(self, row_dst_8_OOF_e_option_data, row_dstBe_option_info):
        if (row_dst_8_OOF_e_option_data.option_span_cqg_code['optcod'].strip() == ''):

            doc_update_message = self.db.options.find_one_and_update(
                {'optionmonthint': row_dst_8_OOF_e_option_data.option_contract_month, \
                 'optionyear': row_dst_8_OOF_e_option_data.option_contract_year, \
                 'strikeprice': row_dst_8_OOF_e_option_data.option_strike_price,
                 'callorput': row_dst_8_OOF_e_option_data.option_type, \
                 'idinstrument': row_dst_8_OOF_e_option_data.instrument['idinstrument'], \
                 # 'optioncode': row_dst_8_OOF_e_option_data.option_span_cqg_code['optcod'] \
                 },
                {'$set': {'idcontract': row_dstBe_option_info.span_underlying_future_contract_props.idcontract, \
                          #'contract_objectid': row_dstBe_option_info.span_underlying_future_contract_props.contract_objectid, \
                          'optionname': row_dst_8_OOF_e_option_data.option_cqg_symbol, \
                          'cqgsymbol': row_dst_8_OOF_e_option_data.option_cqg_symbol, \
                          'expirationdate': row_dstBe_option_info.option_contract_expiration}}, \
                return_document=ReturnDocument.AFTER)

        else:

            doc_update_message = self.db.options.find_one_and_update(
                {'optionmonthint': row_dst_8_OOF_e_option_data.option_contract_month, \
                 'optionyear': row_dst_8_OOF_e_option_data.option_contract_year, \
                 'strikeprice': row_dst_8_OOF_e_option_data.option_strike_price,
                 'callorput': row_dst_8_OOF_e_option_data.option_type, \
                 'idinstrument': row_dst_8_OOF_e_option_data.instrument['idinstrument'], \
                 'optioncode': row_dst_8_OOF_e_option_data.option_span_cqg_code['optcod'] \
                 },
                {'$set': {'idcontract': row_dstBe_option_info.span_underlying_future_contract_props.idcontract, \
                          #'contract_objectid': row_dstBe_option_info.span_underlying_future_contract_props.contract_objectid, \
                          'optionname': row_dst_8_OOF_e_option_data.option_cqg_symbol, \
                          'cqgsymbol': row_dst_8_OOF_e_option_data.option_cqg_symbol, \
                          'expirationdate': row_dstBe_option_info.option_contract_expiration}}, \
                return_document=ReturnDocument.AFTER)

        print('^^option update^^', doc_update_message)

        # if (doc_update_message['updatedExisting'] == True):
        if (doc_update_message != None):

            row_dstBe_option_info.idoption = doc_update_message['idoption']
            #row_dstBe_option_info.option_objectid = doc_update_message['_id']

            print('%%%%%%%%%%%%')

        # if(doc_update_message['updatedExisting'] == False):
        else:
            print("999999999999999999", doc_update_message)

            idoption_doc = \
                self.db.counters.find_one_and_update({'_id': 'idoption'}, {'$inc': {'seq': 1}}, \
                                                     return_document=ReturnDocument.AFTER)

            print("idoption", idoption_doc)

            _objectid = self.db.options.insert({'optionname': row_dst_8_OOF_e_option_data.option_cqg_symbol, \
                                                'optionmonth': row_dst_8_OOF_e_option_data.option_contract_month_char, \
                                                'optionmonthint': row_dst_8_OOF_e_option_data.option_contract_month, \
                                                'optionyear': row_dst_8_OOF_e_option_data.option_contract_year, \
                                                'strikeprice': row_dst_8_OOF_e_option_data.option_strike_price, \
                                                'callorput': row_dst_8_OOF_e_option_data.option_type, \
                                                'idinstrument': row_dst_8_OOF_e_option_data.instrument['idinstrument'], \
                                                'expirationdate': row_dstBe_option_info.option_contract_expiration, \
                                                'idcontract': row_dstBe_option_info.span_underlying_future_contract_props.idcontract, \
                                                #'contract_objectid': row_dstBe_option_info.span_underlying_future_contract_props.contract_objectid, \
                                                'cqgsymbol': row_dst_8_OOF_e_option_data.option_cqg_symbol, \
                                                'optioncode': row_dst_8_OOF_e_option_data.option_span_cqg_code[
                                                    'optcod'], \
                                                'idoption': int(idoption_doc['seq'])})

            print(_objectid)

            row_dstBe_option_info.idoption = int(idoption_doc['seq'])
            #row_dstBe_option_info.option_objectid = _objectid

            print('2222%%%%%%%%%%%%', row_dstBe_option_info.idoption)

        doc_update_message = self.db.options_data.update({'idoption': row_dstBe_option_info.idoption, \
                                                          'datetime': row_dst_8_OOF_e_option_data.span_file_date_time}, \
                                                         {'$set': {
                                                             'price': row_dst_8_OOF_e_option_data.settlement_price, \
                                                             'impliedvol': row_dst_8_OOF_e_option_data.implied_vol, \
                                                             'timetoexpinyears': row_dstBe_option_info.option_time_to_exp}}, \
                                                         upsert=True)
