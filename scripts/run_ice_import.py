
from span_reader.ice_span import IceSpanImport

#input_args['idinstrument'] = 36

csi = IceSpanImport(None)

#futures_filepath = "C:\\Users\\Steve Pickering\\Desktop\\span_data_collector\\Cocoa Data\\futures\\EOD_Futures_ProductFile_ProductID(578).csv"
#options_filepath = "C:\\Users\\Steve Pickering\\Desktop\\span_data_collector\\Cocoa Data\\options\\EOD_Options_578_2017.csv"

#futures_filepath = "C:/Users/Steve Pickering/Desktop/span_data_collector/ICE_DATA/Sugar Data/EOD_Futures_582_2017.csv"
#options_filepath = "C:/Users/Steve Pickering/Desktop/span_data_collector/ICE_DATA/Sugar Data/EOD_Options_582_2017.csv"

futures_filepath = "C:/ICE_DATA/EOD_Futures_ProductFile_ProductID(578).csv"
options_filepath = "C:/ICE_DATA/EOD_Options_578_2016.csv"
#futures_filepath = "D:/EOD_Futures_582_2017.csv"
#options_filepath = "D:/EOD_Options_582_2017.csv"

#futures_filepath = "/home/ubertrader/Downloads/EOD_Futures_ProductFile_ProductID(578).csv"
#options_filepath = "/home/ubertrader/Downloads/EOD_Options_578_2013.csv"

csi.load_span_file(futures_filepath, options_filepath)
