#from span_reader.cme_span import CmeSpanImport
from span_reader.ice_span import IceSpanImport

'''
input_args = {}

input_args['optionenabled'] = 2
input_args['testing'] = True

csi = CmeSpanImport(input_args)

filepath = "C:\\Users\\Steve Pickering\\Desktop\\span_data_collector\\cme.20170106.c.pa2"

csi.load_span_file(filepath)

'''
#input_args['idinstrument'] = 36

csi = IceSpanImport(None)

#futures_filepath = "C:\\Users\\Steve Pickering\\Desktop\\span_data_collector\\Cocoa Data\\futures\\EOD_Futures_ProductFile_ProductID(578).csv"
#options_filepath = "C:\\Users\\Steve Pickering\\Desktop\\span_data_collector\\Cocoa Data\\options\\EOD_Options_578_2017.csv"

#futures_filepath = "C:/ICE_DATA/EOD_Futures_ProductFile_ProductID(578).csv"
#options_filepath = "C:/ICE_DATA/EOD_Options_578_2016.csv"

futures_filepath = "/home/ubertrader/Downloads/EOD_Futures_ProductFile_ProductID(578).csv"
options_filepath = "/home/ubertrader/Downloads/EOD_Options_578_2016.csv"

csi.load_span_file(futures_filepath, options_filepath)
