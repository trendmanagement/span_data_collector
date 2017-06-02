import os
#import datetime as dt
from span_reader.settings import *
from tqdm import tqdm, tnrange, tqdm_notebook

from span_reader.cme_span import CmeSpanImport


input_args = {}

input_args['optionenabled'] = 2

csi = CmeSpanImport(input_args)


if os.path.isdir(DEFAULT_SPAN_FOLDER_MULTIPLE_FILES):

    thefile = open("progress_data.txt", 'w')
    thefile.close()



    max_steps = len(os.listdir(DEFAULT_SPAN_FOLDER_MULTIPLE_FILES))
    pbar = tqdm(desc="Progress", total=max_steps)

    for file in os.listdir(DEFAULT_SPAN_FOLDER_MULTIPLE_FILES):
        if file.endswith(".pa2"):
            print(os.path.join(DEFAULT_SPAN_FOLDER_MULTIPLE_FILES, file))
            span_file = os.path.join(DEFAULT_SPAN_FOLDER_MULTIPLE_FILES, file)

            csi.load_span_file(span_file)

            thefile = open("progress_data.txt", 'a')
            thefile.write("%s\n" % file)
            thefile.close()

            pbar.update(1)
