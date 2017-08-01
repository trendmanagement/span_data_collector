
import sys, argparse, logging
import datetime
from decimal import Decimal
import pymongo
from pymongo import MongoClient
from tqdm import tqdm, tnrange, tqdm_notebook

#from scripts.settings import *

IDINSTRUMENT = None

def convert_dates(values):
    k,v = values
    if type(v) == datetime.date:
        return k, datetime.datetime.combine(
                v,
                datetime.datetime.min.time())
    if type(v) == Decimal:
        return k, float(v)
    if k == 'datetime' and type(v) == str:
        return k, datetime.datetime.strptime(v, '%Y-%m-%d %H:%M:%S')
    elif k == 'expirationdate' and type(v) == str:
        return k, datetime.datetime.strptime(v, '%Y-%m-%d')
    elif k == 'spanoptionstart' and type(v) == str:
        return k, datetime.datetime.strptime(v, '%Y-%m-%d')
    elif k == 'optionstart' and type(v) == str:
        return k, datetime.datetime.strptime(v, '%Y-%m-%d')
    elif k == 'datastart' and type(v) == str:
        return k, datetime.datetime.strptime(v, '%Y-%m-%d')
    elif k == 'customdayboundarytime' and type(v) == str:
        return k, datetime.datetime.strptime(v, '%Y-%m-%d %H:%M:%S')
    elif k == 'optioninputdatetime' and type(v) == str:
        return k, datetime.datetime.strptime(v, '%Y-%m-%d %H:%M:%S')
    if k == 'date' and type(v) == str:
        return k, datetime.datetime.strptime(v, '%Y-%m-%d')
    else:
        return k, v

#MONGO_CONNSTR = 'mongodb://exowriter:qmWSy4K3@10.0.1.2/tmldb?authMechanism=SCRAM-SHA-1'
MONGO_CONNSTR = 'mongodb://tmqr:tmqr@10.0.1.2/tmldb_v2?authMechanism=SCRAM-SHA-1'
MONGO_CONNSTR_LOCAL = 'mongodb://localhost:27017'
MONGO_EXO_DB = 'tmldb_v2'
MONGO_EXO_DB_LOCAL = 'tmldb'
MONGO_CONNSTR_LIVE = 'mongodb://exowriter:qmWSy4K3@10.0.1.2/tmldb?authMechanism=SCRAM-SHA-1'
MONGO_EXO_DB_LIVE = 'tmldb'


# Init mongo asset index
client = MongoClient(MONGO_CONNSTR)
mongo_db = client[MONGO_EXO_DB]

#mongo_collection.create_index([('idbardata', pymongo.ASCENDING)], unique=True)
#mongo_collection.create_index([('idcontract', pymongo.ASCENDING), ('datetime', pymongo.ASCENDING)], unique=True)

#print('Bar data')
############################################################################

options_col = mongo_db['options']
options_mongo = options_col.find({'idinstrument':IDINSTRUMENT})
contracts_dict = {}

max_steps = options_mongo.count()
pbar = tqdm(desc="Progress", total=max_steps)

option_data_col = mongo_db['options_data']

options_remove_col = mongo_db['options']

for option_mongo in options_mongo:
    #print(option_mongo['idoption'])
    done = option_data_col.remove({'idoption': option_mongo['idoption']})

    options_remove_col.remove({'idoption': option_mongo['idoption']})

    #print(done)
    #option_data_list = option_data_col.find({'idoption': option_mongo['idoption']})
    #for option_data in option_data_list:
    #    print(option_data['idoption'])
    pbar.update(1)

print('options done')


##########
contracts_col = mongo_db['contracts']
contracts_mongo = contracts_col.find({'idinstrument':IDINSTRUMENT})
contracts_dict = {}

max_steps = contracts_mongo.count()
pbar = tqdm(desc="Progress", total=max_steps)

contract_data_col = mongo_db['futures_contract_settlements']

for contracts in contracts_mongo:
    #print(option_mongo['idoption'])
    done = contract_data_col.remove({'idcontract': contracts['idcontract']})

    pbar.update(1)

print('contracts done')





