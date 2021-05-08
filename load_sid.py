import pymongo
from sid_model import Substances
from config import Config
import glob, os
from connector import connect_to_db

client_db = connect_to_db()
db = getattr(client_db, Config.DB)

substances_collection = getattr(db, Config.SUBSTANCE_COLLECTION)

xmlfiles = glob.glob(os.path.join(Config.SUBSTANCE_DIRECTORY, 'Substance_*.xml'))

total_files = len(xmlfiles)
total_loaded = 0

for xmlfile in xmlfiles:

    substances = Substances.load_substances(xmlfile)

    records = substances.parse_substances()

    for record in records:
        substances_collection.update_one({'_id': record['_id']}, {'$set': record}, upsert=True)

    total_loaded += 1
    print('{:.2} files loaded'.format(total_loaded/total_files))

client_db.close()
