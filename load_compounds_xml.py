import glob
import numpy as np
from sqlalchemy.orm import sessionmaker
from sqldb import Compound, engine
import os, tqdm
import gzip, shutil
from rdkit import Chem
from rdkit import RDLogger
from cid_model import Compounds

# ignore warnings: https://github.com/rdkit/rdkit/issues/2683
RDLogger.DisableLog('rdApp.*')


FILES = sorted(glob.glob(os.path.join(os.getenv('PUBCHEM_COMPOUND_FILES'), '*.xml.gz')))
print(f"There are {len(FILES)} folders")
print(FILES)






Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

session.query(Compound).delete()
session.commit()

failed_files = []

compounds_to_load = []

# these params apparently help
# make the inserts more efficient
# https://avi.im/blag/2021/fast-sqlite-inserts/
engine.execute('PRAGMA journal_mode = OFF;')
engine.execute('PRAGMA synchronous = 0;')
engine.execute('PRAGMA cache_size = 1000000;')  # give it a GB
engine.execute('PRAGMA locking_mode = EXCLUSIVE;')
engine.execute('PRAGMA temp_store = MEMORY;')

# this is how many records
# to insert at one time
batch_size = 100_000
current_batch_size = 0
results_to_add = []
cids = []


with open('errors.txt', 'w') as text_file:
    for xmlfile in tqdm.tqdm(FILES):
        try:
            compounds = Compounds(xmlfile)
            prop = compounds.parse_compounds()
        except:
            print("error: ", xmlfile)
            continue

        for cmp in prop:
            cid = cmp.get_cid()
            inchi = cmp.get_inchi()

            results_to_add = results_to_add + [{'cid': cid, 'inchi':inchi, 'smiles': np.nan}]
            cids.append(cid)

            if len(results_to_add) >= batch_size:
                engine.execute(Compound.__table__.insert(), results_to_add)
                results_to_add = []
                session.commit()
                print(np.unique(np.asarray(cids)).shape[0])


    engine.execute(Compound.__table__.insert(), results_to_add)
    results_to_add = []
    session.commit()
    print(np.unique(np.asarray(cids)).shape[0])

