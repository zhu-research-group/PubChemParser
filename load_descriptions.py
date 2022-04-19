
import os, glob
import pandas as pd
from sqlalchemy.orm import sessionmaker
from zipfile import ZipFile
import gzip
from sqldb import Assay, engine
import ntpath

import ijson, json
import os
import glob
import ntpath
import sys, decimal
import gzip
import shutil


ASSAY_FILES = glob.glob(r"E:\pubchem\bioassay\Concise\JSON\all\*\*.json")

Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

assays_to_add = []


failed_files = []

for json_file in ASSAY_FILES:

    try:
        json_file_obj = open(json_file, 'r')
        assay_desc_objs = ijson.items(json_file_obj, 'PC_AssaySubmit.assay')
        assay_desc = list(assay_desc_objs)[0]


        aid = assay_desc['descr']['aid']['id']
        name = assay_desc['descr']['name']
        description = ' '.join(assay_desc['descr']['description'])
        source = assay_desc['descr']['aid_source']['db']['name']
        try:
            outcome_method = assay_desc['descr']['activity_outcome_method']
        except KeyError:
            outcome_method = None
        assay = Assay(aid=aid,
                      name=name,
                      description=description,
                      source=source,
                      outcome_method=outcome_method)

        assays_to_add.append(assay)
    except:
        failed_files.append(json_file)


session.add_all(assays_to_add)
session.commit()

f = open('description_errors.txt', 'w')

for failed_file in failed_files:
    f.write(failed_file + '\n')
f.close()
