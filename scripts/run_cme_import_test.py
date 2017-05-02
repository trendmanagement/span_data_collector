from span_reader.cme_span import CmeSpanImport
from message_control.signalapp import SignalApp, APPCLASS_DATA
from message_control.messages import *
from span_reader.settings import *

print(RABBIT_HOST)
print(RABBIT_USER)
print(RABBIT_PASSW)

input_args = {}

input_args['optionenabled'] = 2
input_args['testing'] = True

#csi = CmeSpanImport(input_args)

filepath = "C:\\Users\\Steve Pickering\\Desktop\\span_data_collector\\cme.20170106.c.pa2"

#csi.load_span_file(filepath)

signalapp = SignalApp('test', APPCLASS_DATA, RABBIT_HOST, RABBIT_USER, RABBIT_PASSW)

signalapp.send(MsgStatus('RUN', 'Alpha rebalancer script', notify=True))