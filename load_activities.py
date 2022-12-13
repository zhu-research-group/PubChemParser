
import os, glob
import pandas as pd
from sqlalchemy.orm import sessionmaker
from zipfile import ZipFile
import gzip
from rdkit import Chem
from sqldb import Activity, engine
import ntpath, os, sys


import os, glob
import pandas as pd
from sqlalchemy.orm import sessionmaker
from zipfile import ZipFile
import gzip
from rdkit import Chem
from sqldb import Activity, engine
import ntpath, os, sys



FOLDERS = sorted(glob.glob(os.path.join(os.getenv('PUBCHEM_ASSAY_FILES'), '*')))
print(f"There are {len(FOLDERS)} folders")
CHUNK_FOLDERS = FOLDERS[0:100]

ASSAY_FILES = []
for FOLDER in CHUNK_FOLDERS:
    ASSAY_FILES = ASSAY_FILES + glob.glob(os.path.join(FOLDER, '*.concise.csv.gz'))
text_file = open('errors.txt', 'w')
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

failed_files = []

activities_to_load = []

for f in ASSAY_FILES:
    try:
        aid = int(ntpath.basename(f).split('.')[0])
        print("on {}".format(aid))
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

        print(aid)
        for i, data in assay_results.iterrows():

            cid = data['PUBCHEM_CID']
            sid = data['PUBCHEM_SID']

            outcome = data['PUBCHEM_ACTIVITY_OUTCOME']
            score = data['PUBCHEM_ACTIVITY_SCORE']
            result_tag = data.get('PUBCHEM_RESULT_TAG')

            #result_exists = session.query(Activity.aid).filter_by(cid=cid, sid=sid, aid=aid, result_tag=result_tag).first()
            # if result_exists:
            #     print("Skipping")
            #     continue

            #act = Activity(cid=cid, sid=sid, aid=aid, outcome=outcome, score=score, result_tag=result_tag)
            # using the SQLAlchemy ORM for bulk inserts is very slow
            # this explains: https://docs.sqlalchemy.org/en/13/faq/performance.html#i-m-inserting-400-000-rows-with-the-orm-and-it-s-really-slow
            act = dict(cid=cid, sid=sid, aid=aid, outcome=outcome, score=score, result_tag=result_tag)
            results_to_add.append(act)
    except:
        error = sys.exc_info()[0]
        failed_files.append((f, error))
        continue

    # session.add_all(results_to_add)
    # session.commit()
    engine.execute(Activity.__table__.insert(), results_to_add)

for failed_file, error in failed_files:
    text_file.write(failed_file + '\t' + str(error) + '\n')
text_file.close()