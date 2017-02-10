from span_reader.settings import *
from pymongo import MongoClient, ReturnDocument
import pprint

class MongoQueries():

    def __init__(self, args = None):

        self.mongoclient = MongoClient(MONGO_CONNSTR)
        self.db = self.mongoclient[MONGO_EXO_DB]

        max_id_contract_agg = self.db.contracts.aggregate([{'$group': {'_id': 'null', 'max': {'$max': '$idcontract'}}}])
        max_id_option_agg = self.db.options.aggregate([{'$group': {'_id': 'null', 'max': {'$max': '$idoption'}}}])

        max_id_contract_list = list(max_id_contract_agg)
        max_con = int(max_id_contract_list[0]['max'])
        print('+++++++++++++++',max_con)
        self.db.counters.update({'_id':'idcontract'},{'$set': {'seq':max_con}})

        max_id_option_list = list(max_id_option_agg)
        max_opt = int(max_id_option_list[0]['max'])
        print('+++++++++++++++', max_opt)
        self.db.counters.update({'_id': 'idoption'}, {'$set': {'seq': max_opt}})


    def fill_future_info(self, future_contract_info):



        doc_update_message = self.db.contracts.find_one_and_update({'monthint': future_contract_info.future_contract_month, \
                                                       'year': future_contract_info.future_contract_year, \
                                                       'idinstrument': future_contract_info.instrument['idinstrument']}, \
                                                                   {'$set': {'cqgsymbol': future_contract_info.future_cqg_symbol, \
                                                            'contractname':future_contract_info.future_cqg_symbol, \
                                                            'expirationdate':future_contract_info.future_contract_expiration}}, \
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

        #if (doc_update_message['updatedExisting'] == True):
        if (doc_update_message != None):

            future_contract_info.idcontract = doc_update_message['idcontract']
            future_contract_info.contract_objectid = doc_update_message['_id']

            print('%%%%%%%%%%%%', future_contract_info.idcontract, future_contract_info.contract_objectid)

        #if(doc_update_message['updatedExisting'] == False):
        else:
            print("999999999999999999", doc_update_message)

            idcontract_doc = \
                self.db.counters.find_one_and_update({'_id':'idcontract'},{'$inc':{'seq':1}}, \
                                                     return_document=ReturnDocument.AFTER)

            print(idcontract_doc)

            _objectid = self.db.contracts.insert({'monthint': future_contract_info.future_contract_month, \
                                                    'month': future_contract_info.future_contract_month_char, \
                                                    'year': future_contract_info.future_contract_year, \
                                                    'idinstrument': future_contract_info.instrument['idinstrument'], \
                                                    'cqgsymbol': future_contract_info.future_cqg_symbol, \
                                                    'contractname': future_contract_info.future_cqg_symbol, \
                                                    'expirationdate': future_contract_info.future_contract_expiration, \
                                                    'idcontract':int(idcontract_doc['seq'])})

            print(_objectid)

            future_contract_info.idcontract = int(idcontract_doc['seq'])
            future_contract_info.contract_objectid = _objectid

            print('2222%%%%%%%%%%%%', future_contract_info.idcontract, future_contract_info.contract_objectid)


    def fill_future_price(self, future_contract_data, future_contract_info):

        # doc = self.db.contracts.update({'monthint':6,'year':1983,'idinstrument':7777}, {'$set':{'cqgsymbol':'F.CLEM83'}}, upsert=True)

        print('&&&&&&&&&&',
              future_contract_info.idcontract,
              future_contract_info.contract_objectid,
              future_contract_data.span_file_date_time,
              future_contract_data.settlement_price)

        doc_update_message = self.db.futures_data.update({'idcontract': future_contract_info.idcontract, \
                                                       'contract_objectid':future_contract_info.contract_objectid, \
                                                       'date': future_contract_data.span_file_date_time}, \
                                                        {'$set': {'settlement': future_contract_data.settlement_price}}, \
                                                        upsert= True)

        print('###############', doc_update_message)


    def fill_option_info(self, option_contract_info):

        print('test')