
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
import ntpath, os, sys, csv, gzip, shutil



FOLDERS = sorted(glob.glob(os.path.join(os.getenv('PUBCHEM_ASSAY_FILES'), '*')))
print(f"There are {len(FOLDERS)} folders")
CHUNK_FOLDERS = FOLDERS[:1]

ASSAY_FILES = []
for FOLDER in CHUNK_FOLDERS:
    ASSAY_FILES = ASSAY_FILES + glob.glob(os.path.join(FOLDER, '*.concise.csv.gz'))
text_file = open('errors.txt', 'w')
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

failed_files = []

activities_to_load = []

import time

start = time.time()

def read_pubchem_file(pc_file: str):
    """ generator function to read a pubchem file and load data one by one """


    with gzip.open(pc_file, mode='rt') as inputfile:
        reader = csv.DictReader(inputfile, delimiter=',')
        # headers
        # headers = next(reader)
        #
        # tag_idx = headers.index("PUBCHEM_RESULT_TAG")
        # cid_idx = headers.index("PUBCHEM_CID")
        # sid_idx = headers.index("PUBCHEM_SID")
        # outcome_idx = headers.index("PUBCHEM_ACTIVITY_OUTCOME")
        # score_idx = headers.index("PUBCHEM_ACTIVITY_SCORE")
        #
        # # # find first datapoint row
        # # while reader[0] != '1':
        # #     next(reader)
        line = next(reader)
        while line['PUBCHEM_RESULT_TAG'] != '1':
            line = next(reader)

        yield line
        for line in reader:
            yield line


for f in ASSAY_FILES[:100]:
    aid = int(ntpath.basename(f).split('.')[0])

    print("on {}".format(aid))
    # aid_exists = session.query(Activity.aid).filter_by(aid=aid).first()
    # if aid_exists:
    #     continue
    # the first n rows are a header
    # that describes the data
    # dose response example is 434931
    results_to_add = []
    for data in read_pubchem_file(f):
        cid = data['PUBCHEM_CID']
        sid = data['PUBCHEM_SID']

        outcome = data['PUBCHEM_ACTIVITY_OUTCOME']
        score = data['PUBCHEM_ACTIVITY_SCORE']
        result_tag = data.get('PUBCHEM_RESULT_TAG')

        #act = Activity(cid=cid, sid=sid, aid=aid, outcome=outcome, score=score, result_tag=result_tag)
        # using the SQLAlchemy ORM for bulk inserts is very slow
        # this explains: https://docs.sqlalchemy.org/en/13/faq/performance.html#i-m-inserting-400-000-rows-with-the-orm-and-it-s-really-slow
        act = dict(cid=cid, sid=sid, aid=aid, outcome=outcome, score=score, result_tag=result_tag)
        results_to_add.append(act)

    # session.add_all(results_to_add)
    # session.commit()
    engine.execute(Activity.__table__.insert(), results_to_add)
#     except:
#         error = sys.exc_info()[0]
#         failed_files.append((f, error))
#         continue
#

#
# for failed_file, error in failed_files:
#     text_file.write(failed_file + '\t' + str(error) + '\n')
# text_file.close()
print("hello")
end = time.time()
print(end - start)