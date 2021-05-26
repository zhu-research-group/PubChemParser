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

bioassay_json_files = glob.glob('/g5/home/danrusso/pc/json/bioassay/*/*.json.gz')
print(len(bioassay_json_files))
target_dir = '/lustre/scratch/danrusso/json/parsed_bioassay'

errors = []

if not os.path.exists(target_dir):
    os.mkdir(target_dir)

for json_file in bioassay_json_files:
    try:
        json_data = ''

        for line in gzip.open(json_file, 'rt', encoding='utf-8'):
            json_data = json_data + line

        data = json.loads(json_data)

        assay_desc = data['PC_AssaySubmit']['assay']
        assay_data = data['PC_AssaySubmit']['data']

        aid = assay_desc['descr']['aid']['id']

        # for the assay desc store
        # as the identifier
        assay_desc['_id'] = {'aid': aid}

        for result in assay_data:
            # store aid/sid pairs as ids for results
            _id = {'_id': {'aid': assay_desc['_id']['aid'], 'sid': result['sid']}}
            result.update(_id)

        # dump both to target directory
        desc_file = os.path.join(target_dir, '{}_desc.json'.format(aid))
        with open(desc_file, 'w', encoding='utf-8') as f:
            json.dump(assay_desc, f, ensure_ascii=False, indent=4)


        results_file = os.path.join(target_dir, '{}_results.json'.format(aid))
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(assay_data, f, ensure_ascii=False, indent=4)
    except:
        error = sys.exc_info()[0]
        aid_file = ntpath.basename(json_file)
        errors.append([error, aid_file])

error_file = os.path.join(target_dir, 'parse.log')
with open(error_file, 'w', encoding='utf-8') as f:
    for row in errors:
        error = row[0]
        json_file = row[1]
        f.write('{}: {}\n'.format(error, json_file))