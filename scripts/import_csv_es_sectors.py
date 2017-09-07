import os
import pandas as pd
import numpy as np
from pymongo import MongoClient, ReturnDocument


MONGO_CONNSTR = 'mongodb://tmqr:tmqr@10.0.1.2/tmldb_v2?authMechanism=SCRAM-SHA-1'
MONGO_EXO_DB = 'tmldb_v2'

client = MongoClient(MONGO_CONNSTR)
db = client[MONGO_EXO_DB]

path = "C:/Users/Steve Pickering/Desktop/XAF/"

files = filter(os.path.isfile, os.listdir( path ) )

for filename in os.listdir( path ):
#if True:
#    filename = "XAFM13.csv"
    #data = np.loadtxt("C:/Users/Steve Pickering/Desktop/XAF/XAFH12.csv")
    # df = pd.read_csv("C:/Users/Steve Pickering/Desktop/XAF/XAFH12.csv", parse_dates=[['Date', 'Time']])

    print(filename)

    cqgsymbol = filename.split('.')

    contract = db.contracts.find_one({'cqgsymbol':'F.' + cqgsymbol[0]})

    assert contract != None

    df = pd.read_csv(path + filename, parse_dates=[['Date', 'Time']])

    #print(data.to_dict())

    df.dropna()
    #print(df)

    x = df.replace(r'\s+', np.nan, regex=True)

    x = x.dropna(axis=0, how='any')

    x = x.drop_duplicates(subset=['Date_Time'], keep=False)

    x = x.sort_values(by='Date_Time')

    #print(x.iloc[-1]['Date_Time'])

    print(contract['idcontract'], filename)

    db.contracts_bars.delete_many({'idcontract':contract['idcontract'], 'datetime':{'$lte':x.iloc[-1]['Date_Time']}})

    for index, row in x.iterrows():


        info_dict = {
            "idcontract": contract['idcontract'],
            "open": row[1],
            "high": row[2],
            "low": row[3],
            "close": row[4],
            "datetime": row['Date_Time'],
            "volume": row['Vol']
        }

        #print(info_dict)

        filter_request_dict = {}

        filter_request_dict["idcontract"] = info_dict["idcontract"]
        filter_request_dict["datetime"] = info_dict["datetime"]



        doc_update = db.contracts_bars.find_one_and_update(filter_request_dict,
                                                           {'$set': info_dict},
                                                           upsert=True,  # Insert non existing data!
                                                           return_document=ReturnDocument.AFTER)

        #print(doc_update)
    #
    #     if len(row['O'].strip()) != 0:
    #         info_dict = {
    #             "idcontract": 'x',
    #             "open": float(row['O']),
    #             "high": float(row['H']),
    #             "low": float(row['L']),
    #             "close": float(row['C']),
    #             "datetime": row['Date'] + row['Time'],
    #             "volume": int(row['Vol'])
    #         }
    #
    #         print(info_dict)