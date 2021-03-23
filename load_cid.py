import pymongo
from cid_model import Compounds
from config import Config
import glob, os

update = False

client_db = pymongo.MongoClient(Config.HOST, Config.PORT)

db = getattr(client_db, Config.DB)

compounds_collection = getattr(db, Config.COMPOUND_COLLECTION)

xmlfiles = glob.glob(os.path.join(Config.COMPOUND_DIRECTORY, 'Compound_*.xml'))

for xmlfile in xmlfiles:

    compounds = Compounds.load_compounds(xmlfile)

    records = compounds.parse_compounds()

    if not update:
        compounds_collection.insert_many(records)
    else:
        cids = [record['cid'] for record in records]
        compounds_collection.update_many({'cid': {'$in': cids}}, {'$set': records}, upsert=True)


client_db.close()
