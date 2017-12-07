import os
import csv
from pymongo import MongoClient, ReturnDocument
from datetime import datetime
from tqdm import tqdm, tnrange, tqdm_notebook

#MONGO_CONNSTR = 'localhost'
MONGO_CONNSTR = 'mongodb://tmqr:tmqr@10.0.1.2/tmldb_v2?authMechanism=SCRAM-SHA-1'
MONGO_EXO_DB = 'tmldb_v2'

client = MongoClient(MONGO_CONNSTR)
db = client[MONGO_EXO_DB]

path = 'D:/Daves data for Steve/CQG downloads to Mongo/Data Ready for Mongo/CSVs/'

max_steps = len(os.listdir(path))
pbar = tqdm(desc="Progress", total=max_steps)

for filename in os.listdir(path):
    #with open('D:/Daves data for Steve/CQG downloads to Mongo/Data Ready for Mongo/CSVs/DBH17 5 min bars.csv','r') as my_file:
    with open(path + filename,
              'r') as my_file:
        reader = csv.reader(my_file)
        my_list = list(reader)

    line1 = True

    pbar.update(1)
    print(filename)

    for line in my_list:

        if line1:
            date = datetime.strptime(line[0][3:],"%Y-%m-%d %H:%M:%S")
        else:
            date = datetime.strptime(line[0], "%Y-%m-%d %H:%M:%S")

        line1 = False


        #print(date)
        info_dict = {
         "idcontract":line[5],
         "open": line[1],
         "high": line[2],
         "low": line[3],
         "close": line[4],
         "datetime": date,
         "volume": line[7]
        }

        #print(info_dict)

        filter_request_dict = {}

        filter_request_dict["idcontract"] = info_dict["idcontract"]
        filter_request_dict["datetime"] = info_dict["datetime"]

        #for k in info_dict:
        #    filter_request_dict[k] = info_dict[k]

        doc_update = db.contracts_bars.find_one_and_update(filter_request_dict,
                                 {'$set': info_dict},
                                 upsert=True,  # Insert non existing data!
                                 return_document=ReturnDocument.AFTER)

        #print(doc_update)