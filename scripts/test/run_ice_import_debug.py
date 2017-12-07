
from span_reader.ice_span import IceSpanImport

csi = IceSpanImport(None)

#futures_filepath = "C:\\Users\\Steve Pickering\\Desktop\\span_data_collector\\Cocoa Data\\futures\\EOD_Futures_ProductFile_ProductID(578).csv"
#options_filepath = "C:\\Users\\Steve Pickering\\Desktop\\span_data_collector\\Cocoa Data\\options\\EOD_Options_578_2017.csv"

#futures_filepath = "C:/Users/Steve Pickering/Desktop/span_data_collector/ICE_DATA/Sugar Data/EOD_Futures_582_2017.csv"
#options_filepath = "C:/Users/Steve Pickering/Desktop/span_data_collector/ICE_DATA/Sugar Data/EOD_Options_582_2017.csv"

#futures_filepath = "C:/ICE_DATA/EOD_Futures_ProductFile_ProductID(578).csv"
#options_filepath = "C:/ICE_DATA/EOD_Options_578_2016.csv"
#futures_filepath = "D:/EOD_Futures_582_2017.csv"
#options_filepath = "D:/EOD_Options_582_2017.csv"

#futures_filepath = "/home/ubertrader/Downloads/EOD_Futures_ProductFile_ProductID(578).csv"
#options_filepath = "/home/ubertrader/Downloads/EOD_Options_578_2013.csv"

for year in range(2017,2017+1):
    # futures_filepath = "//10.0.1.4/backup/backups/ICE_DATA/sugar/EOD_Futures_582_{0}.csv".format(year)
    # options_filepath = "//10.0.1.4/backup/backups/ICE_DATA/sugar/EOD_Options_582_{0}.csv".format(year)
    futures_filepath = "//10.0.1.4/backup/backups/ICE_DATA/Cocoa/EOD_Futures_578_{0}.csv".format(year)
    options_filepath = "//10.0.1.4/backup/backups/ICE_DATA/Cocoa/EOD_Options_578_{0}.csv".format(year)
    print(futures_filepath, options_filepath)
    csi.load_span_file(futures_filepath, options_filepath)
