from span_reader.ice_span import IceSpanImport
import argparse
from pymongo import MongoClient
from span_reader.settings import *
from datetime import datetime

parser = argparse.ArgumentParser()

parser.add_argument("-i",
                    "--instrument",
                    help="instrument exchange symbol without 'US.'",
                    required=True,
                    type=str)

parser.add_argument("-f",
                    "--folder",
                    help="an instrument you want to backfill",
                    required=False,
                    type=str)

parser.add_argument("-y",
                    "--year",
                    help="year to start file to load",
                    required=False,
                    type=str)

parser.add_argument("-ey",
                    "--end_year",
                    help="year to end file to load",
                    required=False,
                    type=str)



args = parser.parse_args()

client = MongoClient(MONGO_CONNSTR)
db = client[MONGO_EXO_DB]
idinstrument = list(db['instruments'].find({'exchangesymbol':args.instrument},{'idinstrument':1}))



ice_instrument_list = {
    'SB':{'folder':'sugar/',
         'future_file':'EOD_Futures_582_',
            'option_file': 'EOD_Options_582_'},
    'CC':{'folder':'cocoa/',
         'future_file':'EOD_Futures_578_',
            'option_file': 'EOD_Options_578_'}
}

if idinstrument:

    instrument_info = ice_instrument_list[args.instrument]

    if args.folder == None:
        folder = '//10.0.1.4/backup/backups/ICE_DATA/' + instrument_info['folder']
    else:
        folder = args.folder

    if args.year == None:
        start_year = datetime.now().year
    else:
        start_year = int(args.year)

    if args.end_year == None:
        end_year = datetime.now().year
    else:
        end_year = int(args.end_year)



    csi = IceSpanImport(idinstrument[0])

    for year in range(start_year, end_year+1):
        futures_filepath = folder + instrument_info['future_file'] +  "{0}.csv".format(year)
        options_filepath = folder + instrument_info['option_file'] +  "{0}.csv".format(year)
        print(futures_filepath, options_filepath)
        csi.load_span_file(futures_filepath, options_filepath)
