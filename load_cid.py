import pymongo
from cid_model import Compounds
from config import Config
import glob, os
from connector import connect_to_db

client_db = connect_to_db()
db = getattr(client_db, Config.DB)

compounds_collection = getattr(db, Config.COMPOUND_COLLECTION)

xmlfiles = glob.glob(os.path.join(Config.COMPOUND_DIRECTORY, 'Compound_*.xml'))

total_files = len(xmlfiles)
total_loaded = 0

for xmlfile in xmlfiles:

    compounds = Compounds.load_compounds(xmlfile)

    records = compounds.parse_compounds()

    for record in records:
        compounds_collection.update_many({'_id': record['_id']}, {'$set': record}, upsert=True)

    total_loaded += 1
    print('{:.2} files loaded'.format(total_loaded/total_files))

client_db.close()
