import gzip
import ijson, json
import os
import glob
import ntpath
import sys, decimal
import gzip
import shutil
from zipfile import ZipFile

# BIO_ASSAY_DATA_DIR = "E:\\pubchem\\bioassay\\Concise\\CSV\\Data"
#
# if not os.path.join(BIO_ASSAY_DATA_DIR, 'all'):
#     os.mkdir(os.path.join(BIO_ASSAY_DATA_DIR, 'all'))
#
# bioassay_data_files = glob.glob(os.path.join(BIO_ASSAY_DATA_DIR, '*.zip'))
#
# for f in bioassay_data_files:
#     print("done")
#     with ZipFile(f, 'r') as zipObj:
#        # Extract all the contents of zip file in current directory
#        zipObj.extractall(os.path.join(BIO_ASSAY_DATA_DIR, 'all'))
#
# BIOASSAY_JSON_FILES = glob.glob(os.path.join(BIO_ASSAY_DATA_DIR, "all", "*", "*.csv.gz"))
#
# for f in BIOASSAY_JSON_FILES:
#     with gzip.open(f, 'rb') as f_in:
#         with open(f.replace('.gz', ''), 'wb') as f_out:
#             shutil.copyfileobj(f_in, f_out)


BIO_ASSAY_DATA_DIR = "E:\\pubchem\\bioassay\\Concise\\JSON"

if not os.path.join(BIO_ASSAY_DATA_DIR, 'all'):
    os.mkdir(os.path.join(BIO_ASSAY_DATA_DIR, 'all'))

# bioassay_data_files = glob.glob(os.path.join(BIO_ASSAY_DATA_DIR, "all", "*", '*.zip'))
#
# print(len(bioassay_data_files))
#
# for f in bioassay_data_files:
#     print("done")
#     with ZipFile(f, 'r') as zipObj:
#        # Extract all the contents of zip file in current directory
#        zipObj.extractall(os.path.join(BIO_ASSAY_DATA_DIR, 'all'))

BIOASSAY_JSON_FILES = glob.glob(os.path.join(BIO_ASSAY_DATA_DIR, "all", "*", "*.json.gz"))

print(len(BIOASSAY_JSON_FILES))

for f in BIOASSAY_JSON_FILES:
    with gzip.open(f, 'rb') as f_in:
        with open(f.replace('.gz', ''), 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)