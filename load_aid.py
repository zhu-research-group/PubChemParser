import pymongo
from aid_model import Bioassay
from config import Config
import glob, os
from connector import connect_to_db

client_db = connect_to_db()
db = getattr(client_db, Config.DB)

bioassays_collection = getattr(db, Config.BIOASSAY_COLLECTION)
results_collection = getattr(db, Config.BIOASSAY_RESULTS_COLLECTION)

xmlfiles = glob.glob(os.path.join(Config.BIOASSAY_DIRECTORY, '*', '*.xml'))

total_files = len(xmlfiles)
total_loaded = 0

for xmlfile in xmlfiles:

    bioassay = Bioassay.load_bioassay(xmlfile)

    description_record = bioassay.assay.as_record()
    key = {'_id': description_record['aid']}

    bioassays_collection.update_one(key, {'$set': description_record}, upsert=True)


    for result in bioassay.assay_results.parse_results():
        aid = {'aid': description_record['aid']}
        record = aid.copy()
        record['_id'] = '{}-{}'.format(description_record['aid'], result['sid'])
        key = {'_id': record['_id']}
        record.update(result)

        results_collection.update_one(key, {'$set': record}, upsert=True)

    total_loaded += 1
    print('{:.2} files loaded'.format(total_loaded/total_files))

client_db.close()
