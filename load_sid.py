import pymongo
from sid_model import Substances
from config import Config
import glob, os

update = True

client_db = pymongo.MongoClient(Config.HOST, Config.PORT)

db = getattr(client_db, Config.DB)

substances_collection = getattr(db, Config.SUBSTANCE_COLLECTION)

xmlfiles = glob.glob(os.path.join(Config.SUBSTANCE_DIRECTORY, 'Substance_*.xml'))

for xmlfile in xmlfiles:

    substances = Substances.load_substances(xmlfile)

    records = substances.parse_substances()

    for record in records:
        substances_collection.update_one({'_id': record['_id']}, {'$set': record}, upsert=True)



client_db.close()
