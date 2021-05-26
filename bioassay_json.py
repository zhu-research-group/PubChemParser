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

bioassay_json_files = glob.glob('data/json/*.json.gz')
target_dir = 'data/parsed_json'

if not os.path.exists(target_dir):
    os.mkdir(target_dir)

for json_file in bioassay_json_files:

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