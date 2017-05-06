import os
import datetime as dt
from span_reader.settings import *

from span_reader.cme_span import CmeSpanImport


input_args = {}

input_args['optionenabled'] = 2
input_args['testing'] = False

csi = CmeSpanImport(input_args)

#Monday is 0 and Sunday is 6
date = dt.datetime.now().date()
if date.weekday() == 0:
    date += dt.timedelta(days=-3)
elif date.weekday() == 6:
    date += dt.timedelta(days=-2)
else:
    date += dt.timedelta(days=-1)

print(date.strftime('%Y%m%d'))

if os.path.isdir(DEFAULT_SPAN_FOLDER):
    span_file = "cme."
    span_file += date.strftime('%Y%m%d')
    span_file += ".c.pa2"



    span_file = DEFAULT_SPAN_FOLDER + span_file

    print(span_file)

    csi.load_span_file(span_file)
