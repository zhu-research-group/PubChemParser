"""
script to convert the native Json files offered through
the PubChem FTP service to ones that can be inserted into
the Mongo DB.  It splits a PubChem JSON file for a given
AID into two parts: the bioassay description and the
bioassay results.
"""

import gzip
import ijson, json
import os
import glob
import ntpath
import sys, decimal
import gzip
import shutil

bioassay_json_files = glob.glob('E:\\pubchem\\bioassay\\Concise\\JSON\\all\\0000001_0001000\\*.json.gz')[:10]



for f in bioassay_json_files:
    with gzip.open(f, 'rb') as f_in:
        with open(f.replace('.gz', ''), 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)



def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError


bioassay_json_files = glob.glob('E:\\pubchem\\bioassay\\Concise\\JSON\\all\\0000001_0001000\\*.json')[:10]

print(bioassay_json_files)

for json_file in bioassay_json_files:


    json_file_obj = open(json_file, 'r')
    assay_desc_objs = ijson.items(json_file_obj, 'PC_AssaySubmit.assay')
    assay_desc = list(assay_desc_objs)[0]


    aid = assay_desc['descr']['aid']['id']

    # for the assay desc store
    # as the identifier
    assay_desc['_id'] = {'aid': aid}




    json_file_obj = open(json_file, 'r')
    assay_data_objs = ijson.items(json_file_obj, 'PC_AssaySubmit.data')

    assay_data = list(assay_data_objs)[0]
    print(assay_data)
