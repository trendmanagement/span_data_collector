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

        doc_update_message = self.db.futures_contract_settlements.update({'idcontract': future_contract_info.idcontract, \
                                                       'date': future_contract_data.span_file_date_time}, \
                                                        {'$set': {'settlement': future_contract_data.settlement_price, \
                                                                  'contract_objectid': future_contract_info.contract_objectid}}, \
                                                        upsert= True)

        print('###############', doc_update_message)


    def fill_option_info_and_data(self, row_dst_8_OOF_e_option_data, row_dstBe_option_info):

        if(row_dst_8_OOF_e_option_data.option_span_cqg_code['optcod'].strip() == ''):

            doc_update_message = self.db.options.find_one_and_update(
                {'optionmonthint': row_dst_8_OOF_e_option_data.option_contract_month, \
                 'optionyear': row_dst_8_OOF_e_option_data.option_contract_year, \
                 'strikeprice': row_dst_8_OOF_e_option_data.option_strike_price,
                 'callorput': row_dst_8_OOF_e_option_data.option_type, \
                 'idinstrument': row_dst_8_OOF_e_option_data.instrument['idinstrument'], \
                 #'optioncode': row_dst_8_OOF_e_option_data.option_span_cqg_code['optcod'] \
                 },
                {'$set': {'idcontract': row_dstBe_option_info.span_underlying_future_contract_props.idcontract, \
                          'contract_objectid': row_dstBe_option_info.span_underlying_future_contract_props.contract_objectid, \
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
                          'contract_objectid': row_dstBe_option_info.span_underlying_future_contract_props.contract_objectid, \
                          'optionname': row_dst_8_OOF_e_option_data.option_cqg_symbol, \
                          'cqgsymbol': row_dst_8_OOF_e_option_data.option_cqg_symbol, \
                          'expirationdate': row_dstBe_option_info.option_contract_expiration}}, \
                return_document=ReturnDocument.AFTER)


        print('^^option update^^', doc_update_message)

        # if (doc_update_message['updatedExisting'] == True):
        if (doc_update_message != None):

            row_dstBe_option_info.idoption = doc_update_message['idoption']
            row_dstBe_option_info.option_objectid = doc_update_message['_id']

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
                                                  'contract_objectid': row_dstBe_option_info.span_underlying_future_contract_props.contract_objectid, \
                                                  'cqgsymbol': row_dst_8_OOF_e_option_data.option_cqg_symbol, \
                                                  'optioncode': row_dst_8_OOF_e_option_data.option_span_cqg_code['optcod'], \
                                                  'idoption': int(idoption_doc['seq'])})

            print(_objectid)

            row_dstBe_option_info.idoption = int(idoption_doc['seq'])
            row_dstBe_option_info.option_objectid = _objectid

            print('2222%%%%%%%%%%%%', row_dstBe_option_info.idoption, row_dstBe_option_info.option_objectid)


        doc_update_message = self.db.options_data.update({'idoption': row_dstBe_option_info.idoption, \
                                                          'datetime': row_dst_8_OOF_e_option_data.span_file_date_time}, \
                                                         {'$set': {
                                                             'price': row_dst_8_OOF_e_option_data.settlement_price, \
                                                             'impliedvol': row_dst_8_OOF_e_option_data.implied_vol, \
                                                             'timetoexpinyears': row_dstBe_option_info.option_time_to_exp, \
                                                             'option_objectid': row_dstBe_option_info.option_objectid}}, \
                                                         upsert=True)