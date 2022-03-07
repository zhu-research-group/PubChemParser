
import os, glob
from sqlalchemy.orm import sessionmaker
from zipfile import ZipFile
import gzip
from rdkit import Chem
from sqldb import Compound, engine


SDF_FILE_DIR = r"E:\pubchem\compounds\SDF"

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



props = ['PUBCHEM_COMPOUND_CID',
         'PUBCHEM_COMPOUND_CANONICALIZED',
         'PUBCHEM_CACTVS_COMPLEXITY',
         'PUBCHEM_CACTVS_HBOND_ACCEPTOR',
         'PUBCHEM_CACTVS_HBOND_DONOR',
         'PUBCHEM_CACTVS_ROTATABLE_BOND',
         'PUBCHEM_CACTVS_SUBSKEYS',
         'PUBCHEM_IUPAC_OPENEYE_NAME',
         'PUBCHEM_IUPAC_CAS_NAME',
         'PUBCHEM_IUPAC_NAME_MARKUP',
         'PUBCHEM_IUPAC_NAME',
         'PUBCHEM_IUPAC_SYSTEMATIC_NAME',
         'PUBCHEM_IUPAC_TRADITIONAL_NAME',
         'PUBCHEM_IUPAC_INCHI',
         'PUBCHEM_IUPAC_INCHIKEY',
         'PUBCHEM_XLOGP3_AA',
         'PUBCHEM_EXACT_MASS',
         'PUBCHEM_MOLECULAR_FORMULA',
         'PUBCHEM_MOLECULAR_WEIGHT',
         'PUBCHEM_OPENEYE_CAN_SMILES',
         'PUBCHEM_OPENEYE_ISO_SMILES',
         'PUBCHEM_CACTVS_TPSA',
         'PUBCHEM_MONOISOTOPIC_WEIGHT',
         'PUBCHEM_TOTAL_CHARGE',
         'PUBCHEM_HEAVY_ATOM_COUNT',
         'PUBCHEM_ATOM_DEF_STEREO_COUNT',
         'PUBCHEM_ATOM_UDEF_STEREO_COUNT',
         'PUBCHEM_BOND_DEF_STEREO_COUNT',
         'PUBCHEM_BOND_UDEF_STEREO_COUNT',
         'PUBCHEM_ISOTOPIC_ATOM_COUNT',
         'PUBCHEM_COMPONENT_COUNT',
         'PUBCHEM_CACTVS_TAUTO_COUNT',
         'PUBCHEM_COORDINATE_TYPE',
         'PUBCHEM_BONDANNOTATIONS']

props = ['PUBCHEM_COMPOUND_CID',
         'PUBCHEM_IUPAC_INCHI',
         'PUBCHEM_OPENEYE_CAN_SMILES']


sdf_files = glob.glob(os.path.join(SDF_FILE_DIR, '*.sdf.gz'))
for sdf_file in sdf_files:
    inf = gzip.open(sdf_file)

    gzsuppl = Chem.ForwardSDMolSupplier(inf)

    Session = sessionmaker()

    Session.configure(bind=engine)

    session = Session()

    mols_to_add = []

    for mol in gzsuppl:

        if mol:
            cid = mol.GetProp('PUBCHEM_COMPOUND_CID')
            inchi = mol.GetProp('PUBCHEM_IUPAC_INCHI')
            smiles = mol.GetProp('PUBCHEM_OPENEYE_CAN_SMILES')

            cmp = Compound(cid, inchi, smiles)
            mols_to_add.append(cmp)

    session.add_all(mols_to_add)
    session.commit()

