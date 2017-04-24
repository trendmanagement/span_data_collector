MONGO_CONNSTR = 'mongodb://tmqr:tmqr@10.0.1.2/tmldb_v2?authMechanism=SCRAM-SHA-1'
MONGO_EXO_DB = 'tmldb_v2'

"""
TODO: add settings_local.py to current folder and place local settings there
WARNING: DO NOT add settings_local.py to the GIT !

MONGO_CONNSTR = 'mongodb://localhost:27017'
MONGO_EXO_DB = 'tmldb'
"""

INSTRUMENT_CONFIG_JSON_FILE = 'C:/Span_Procedures_Cloud/span_data_collector-master/span_reader/instrumentconfig.json'
    #'C:/Users/Steve Pickering/Desktop/span_data_collector/span_reader/instrumentconfig.json'
    #'C:/Span_Procedures_Cloud/span_data_collector-master/span_reader/instrumentconfig.json'

DEFAULT_SPAN_FOLDER = 'C:/Span_Procedures_Cloud/SPAN_DATA/'
DEFAULT_SPAN_FOLDER_MULTIPLE_FILES = 'C:/CME_SPAN_FILES/'
    #'C:/Users/Steve Pickering/Desktop/span_data_collector/CME_SPAN'
    #'C:/CME_SPAN_FILES/'


# Allow any settings to be defined in settings_local.py which should be
# ignored in your version control system allowing for settings to be
# defined per machine.
try:
    from span_reader.settings_local import *
except ImportError as e:
    if "settings_local" not in str(e):
        raise e