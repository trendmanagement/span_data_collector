TESTING = False

MONGO_CONNSTR = 'mongodb://tmqr:tmqr@10.0.1.2/tmldb_v2?authMechanism=SCRAM-SHA-1'
MONGO_EXO_DB = 'tmldb_v2'
MONGO_SPAN_DATA = 'span_data'

INSTRUMENT_CONFIG_JSON_FILE = 'C:/Span_Procedures_Cloud/span_data_collector-master/span_reader/instrumentconfig.json'

DEFAULT_SPAN_FOLDER = 'C:/Span_Procedures_Cloud/SPAN_DATA/'

DEFAULT_SPAN_FOLDER_MULTIPLE_FILES = '//10.0.1.4/backup/backups/CME Span/'
#'C:/CME_SPAN_FILES/'

PUSH_SLACK_LOGGING = True

#Set to None to run all instruments, otherwise fill as an array [11,21]
RUN_SPECIFIC_INSTRUMENT_ID = None

#
# RabbitMQ credentials
RABBIT_HOST = '10.0.1.2'
RABBIT_USER = 'tmqr'
RABBIT_PASSW = 'tmqr'

# Allow any settings to be defined in settings_local.py which should be
# ignored in your version control system allowing for settings to be
# defined per machine.
try:
    from span_reader.settings_local import *
except ImportError as e:
    if "settings_local" not in str(e):
        raise e