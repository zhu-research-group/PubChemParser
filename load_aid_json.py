"""
script to convert the native Json files offered through
the PubChem FTP service to ones that can be inserted into
the Mongo DB.  It splits a PubChem JSON file for a given
AID into two parts: the bioassay description and the
bioassay results.
"""

import gzip
import json
import os
import glob
import ntpath
import sys

import pymongo
from sid_model import Substances
from config import Config
import glob, os
from connector import connect_to_db

client_db = connect_to_db()
db = getattr(client_db, Config.DB)
results_collection = getattr(db, Config.BIOASSAY_RESULTS_COLLECTION)
assay_collection = getattr(db, Config.BIOASSAY_COLLECTION)


bioassay_json_files = glob.glob(r'E:\pubchem\bioassay\Concise\JSON\all\*\*.json.gz')
total_files = len(bioassay_json_files)



for json_file in bioassay_json_files:
    try:
        json_data = ''

        for line in gzip.open(json_file, 'rt', encoding='utf-8'):
            json_data = json_data + line

        data = json.loads(json_data)

        assay_desc = data['PC_AssaySubmit']['assay']
        assay_data = data['PC_AssaySubmit']['data']

        aid = assay_desc['descr']['aid']['id']

#         # for the assay desc store
#         # as the identifier
#         assay_desc['_id'] = {'aid': aid}
#
#         for i, result in enumerate(assay_data):
#             # store aid/sid pairs as ids for results
#             # there are duplicate aid/sid combos in
#             # certain assays so the idx id should
#             # make every response unique
#             _id = {'_id': {'aid': assay_desc['_id']['aid'], 'sid': result['sid'], 'idx': i}}
#             result.update(_id)
#
#         for result in assay_data:
#             results_collection.update_one({'_id': result['_id']}, result)
#
#         assay_collection.update_one({'_id': assay_desc['_id']}, assay_desc)
#         print("added {} of bioassays}".format(counter/total_files))
#     except:
#         error = sys.exc_info()[0]
#         aid_file = ntpath.basename(json_file)
#         errors.append([error, aid_file])
#
# error_file = os.path.join(target_dir, 'parse.log')
# with open(error_file, 'w', encoding='utf-8') as f:
#     for row in errors:
#         error = row[0]
#         json_file = row[1]
#         f.write('{}: {}\n'.format(error, json_file))