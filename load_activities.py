
import os, glob
import pandas as pd
from sqlalchemy.orm import sessionmaker
from zipfile import ZipFile
import gzip
from rdkit import Chem
from sqldb import Activity, engine
import ntpath, os


ASSAY_FILES = glob.glob(r"E:\pubchem\bioassay\Concise\CSV\Data\all\*\*.csv")

Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

failed_files = []

activities_to_load = []

for f in ASSAY_FILES:
    aid = int(ntpath.basename(f).split('.')[0])
    print("on {}".format(aid))
    aid_exists = session.query(Activity.aid).filter_by(aid=aid).first()
    if aid_exists:
        continue
    try:
        assay_results = pd.read_csv(f).iloc[2:]
        results_to_add = []
        aid = int(ntpath.basename(f).split('.')[0])

        print(aid)
        for i, data in assay_results.iterrows():

                cid = data['PUBCHEM_CID']
                sid = data['PUBCHEM_SID']

                outcome = data['PUBCHEM_ACTIVITY_OUTCOME']
                score = data['PUBCHEM_ACTIVITY_SCORE']
                result_tag = data['PUBCHEM_RESULT_TAG']

                act = Activity(cid=cid, sid=sid, aid=aid, outcome=outcome, score=score, result_tag=result_tag)

                results_to_add.append(act)
    except:
        failed_files.append(f)
        continue

    session.add_all(results_to_add)
    session.commit()

f = open('errors.txt', 'w')

for failed_file in failed_files:
    f.write(failed_file + '\n')
f.close()