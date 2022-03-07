import pandas as pd
import os, glob
import sqlalchemy
from zipfile import ZipFile

BIO_ASSAY_DATA_DIR = r"E:\pubchem\bioassay\Concise\JSON"
#BIO_DESC_DATA_DIR =  r"E:\pubchem\bioassay\CSV\Description"

# if not os.path.join(BIO_DESC_DATA_DIR, 'all'):
#     os.mkdir(os.path.join(BIO_DESC_DATA_DIR, 'all'))

# bioassay_desc_files = glob.glob(os.path.join(BIO_DESC_DATA_DIR, '*.zip'))
#
# for f in bioassay_desc_files:
#     with ZipFile(f, 'r') as zipObj:
#        # Extract all the contents of zip file in current directory
#        zipObj.extractall(os.path.join(BIO_DESC_DATA_DIR, 'all'))
#
if not os.path.join(BIO_ASSAY_DATA_DIR, 'all'):
    os.mkdir(os.path.join(BIO_ASSAY_DATA_DIR, 'all'))
print("done")
bioassay_data_files = glob.glob(os.path.join(BIO_ASSAY_DATA_DIR, '*.zip'))

for f in bioassay_data_files:
    print("done")
    with ZipFile(f, 'r') as zipObj:
       # Extract all the contents of zip file in current directory
       zipObj.extractall(os.path.join(BIO_ASSAY_DATA_DIR, 'all'))