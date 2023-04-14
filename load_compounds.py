import os, glob

import numpy as np
import pandas as pd
from sqlalchemy.orm import sessionmaker
from zipfile import ZipFile
import gzip
#from rdkit import Chem
from sqldb import Compound, engine
import ntpath, os, sys, tqdm
import xml.etree.ElementTree as ET
import gzip
from rdkit import Chem

FILES = sorted(glob.glob(os.path.join(os.getenv('PUBCHEM_COMPOUND_FILES'), '*.sdf.gz')))
print(f"There are {len(FILES)} folders")
print(FILES)






Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

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
batch_size = 1_000_000
current_batch_size = 0
results_to_add = []
cids = []

with open('errors.txt', 'w') as text_file:
    for f in tqdm.tqdm(FILES):


            with gzip.open(f, 'r') as inf:
                suppl = Chem.ForwardSDMolSupplier(inf)
                for mol in suppl:
                    if mol:
                        cid = int(mol.GetProp('PUBCHEM_COMPOUND_CID'))
                        inchi = Chem.MolToInchi(mol)
                        smiles = Chem.MolToSmiles(mol)
                    else:
                        error = 'bad mol'
                        text_file.write(f + '\t' + str(error) + '\n')
                        failed_files.append((f, error))


                    results_to_add = results_to_add + [{'cid': cid, 'inchi':inchi, 'smiles':smiles}]

                    if len(results_to_add) >= batch_size:
                        engine.execute(Compound.__table__.insert(), results_to_add)
                        results_to_add = []
                        session.commit()
                        print(np.unique(np.asarray(cids)).shape[0])


    engine.execute(Compound.__table__.insert(), results_to_add)
    results_to_add = []
    session.commit()
    print(np.unique(np.asarray(cids)).shape[0])

