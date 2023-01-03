import os, glob
import pandas as pd
from sqlalchemy.orm import sessionmaker
from zipfile import ZipFile
import gzip
#from rdkit import Chem
from sqldb import Activity, engine
import ntpath, os, sys, tqdm



FOLDERS = sorted(glob.glob(os.path.join(os.getenv('PUBCHEM_ASSAY_FILES'), '*')))
print(f"There are {len(FOLDERS)} folders")
CHUNK_FOLDERS = FOLDERS

ASSAY_FILES = []
for FOLDER in CHUNK_FOLDERS:
    ASSAY_FILES = ASSAY_FILES + glob.glob(os.path.join(FOLDER, '*.concise.csv.gz'))
text_file = open('errors.txt', 'w')
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

failed_files = []

activities_to_load = []

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
batch_size = 1_00_000
current_batch_size = 0
results_to_add = []

for f in tqdm.tqdm(ASSAY_FILES):
    try:
        aid = int(ntpath.basename(f).split('.')[0])
        # aid_exists = session.query(Activity.aid).filter_by(aid=aid).first()
        # if aid_exists:
        #     continue

        # the first n rows are a header
        # that describes the data
        # dose response example is 434931
        assay_results = pd.read_csv(f, on_bad_lines='skip', compression="gzip")

        # find index where header stops
        idx = assay_results[assay_results.PUBCHEM_RESULT_TAG == '1'].index[0]
        assay_results = assay_results.loc[idx:]

        results_to_add = []
        aid = int(ntpath.basename(f).split('.')[0])

        assay_results = assay_results[['PUBCHEM_CID',
                                       'PUBCHEM_SID',
                                       'PUBCHEM_ACTIVITY_OUTCOME',
                                       'PUBCHEM_ACTIVITY_SCORE',
                                       'PUBCHEM_RESULT_TAG']]
        assay_results['aid'] = aid
        mapper = {
            'PUBCHEM_CID': 'cid',
            'PUBCHEM_SID': 'sid',
            'PUBCHEM_ACTIVITY_OUTCOME': 'outcome',
            'PUBCHEM_ACTIVITY_SCORE': 'score',
            'PUBCHEM_RESULT_TAG': 'result_tag'
        }

        assay_results = assay_results.rename(columns=mapper)
        results_to_add = results_to_add + assay_results.to_dict('records')

        if len(results_to_add) >= batch_size:
            engine.execute(Activity.__table__.insert(), results_to_add)
            results_to_add = []


    except:
        error = sys.exc_info()[0]
        failed_files.append((f, error))
        continue
for failed_file, error in failed_files:
    text_file.write(failed_file + '\t' + str(error) + '\n')
text_file.close()