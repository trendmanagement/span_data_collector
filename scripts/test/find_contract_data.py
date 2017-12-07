import os
#import datetime as dt
from span_reader.settings import *
from tqdm import tqdm, tnrange, tqdm_notebook
from span_reader.settings import *
from pymongo import MongoClient, ReturnDocument
import pymongo
import gridfs


mongoclient = MongoClient(MONGO_CONNSTR)
db = mongoclient[MONGO_EXO_DB]

#512 -> 520

idinstrument = 513

contracts = db['contracts'].find({'idinstrument':idinstrument})

for contract in contracts:


    print(contract)

    bars = db['contracts_bars'].find({'idcontract': contract['idcontract']}).sort([('datetime',pymongo.ASCENDING)])



    for bar in bars:
        print(contract['idcontract'],bar['datetime'])