import os
import datetime as dt
from span_reader.settings import *

from span_reader.cme_span import CmeSpanImport


input_args = {}

input_args['risk_free_rate'] = 0.01

input_args['optionenabled'] = 2

csi = CmeSpanImport(input_args)


if os.path.isdir(DEFAULT_SPAN_FOLDER_MULTIPLE_FILES):
    #span_file = "cme."
    #span_file += date.strftime('%Y%m%d')
    #span_file += ".c.pa2"



    #span_file = DEFAULT_SPAN_FOLDER + span_file

    #print(span_file)


    for file in os.listdir(DEFAULT_SPAN_FOLDER_MULTIPLE_FILES):
        if file.endswith(".pa2"):
            print(os.path.join(DEFAULT_SPAN_FOLDER_MULTIPLE_FILES, file))
            span_file = os.path.join(DEFAULT_SPAN_FOLDER_MULTIPLE_FILES, file)
            csi.load_span_file(span_file)
