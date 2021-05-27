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

bioassay_json_files = glob.glob('/g5/home/danrusso/pc/json/bioassay/*/*.json.gz')
print(len(bioassay_json_files))
target_dir = '/lustre/scratch/danrusso/json/parsed_bioassay'

errors = []

if not os.path.exists(target_dir):
    os.mkdir(target_dir)

def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError

for json_file in bioassay_json_files:


        json_file_obj = open(json_file, 'r')
        assay_desc_objs = ijson.items(json_file_obj, 'PC_AssaySubmit.assay')
        assay_desc = list(assay_desc_objs)[0]


        aid = assay_desc['descr']['aid']['id']

        # for the assay desc store
        # as the identifier
        assay_desc['_id'] = {'aid': aid}

        # dump first normally
        desc_file = os.path.join(target_dir, '{}_desc.json'.format(aid))
        with open(desc_file, 'w', encoding='utf-8') as f:
            json.dump(assay_desc, f, ensure_ascii=False, indent=4)

        # need to close and reopen the file for so it can "restart"
        # its a thing of ijson and generatorsm i think
        json_file_obj.close()

        json_file_obj = open(json_file, 'r')
        assay_data_objs = ijson.items(json_file_obj, 'PC_AssaySubmit.data')

        assay_data = list(assay_data_objs)[0]

        results_file = os.path.join(target_dir, '{}_results.json'.format(aid))
        with open(results_file, 'w', encoding='utf-8') as f:
            f.write('[\n')

            for i, result in enumerate(assay_data):
                # store aid/sid pairs as ids for results
                # there are duplicate aid/sid combos in
                # certain assays so the idx id should
                # make every response unique
                _id = {'_id': {'aid': aid, 'sid': result['sid'], 'idx': i}}
                result.update(_id)
                f.write(json.dumps(result, default=decimal_default))
                f.write(',\n')
            f.write(']')
            f.close()



    #
    # except:
    #     error = sys.exc_info()[0]
    #     aid_file = ntpath.basename(json_file)
    #     errors.append([error, aid_file])
#
# error_file = os.path.join(target_dir, 'parse.log')
# with open(error_file, 'w', encoding='utf-8') as f:
#     for row in errors:
#         error = row[0]
#         json_file = row[1]
#         f.write('{}: {}\n'.format(error, json_file))