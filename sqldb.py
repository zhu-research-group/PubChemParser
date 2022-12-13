
from sqlalchemy import ForeignKey
from sqlalchemy import and_, or_
from sqlalchemy.orm import relationship

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import func

import pandas as pd


from sqlalchemy import Column, Integer, String, Float, Table
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

engine = create_engine(r"sqlite:///G:\Shared drives\ZhuLab\DATA\PUBCHEM\concise\CSV\Data\Data\all\pubchem_concise.db")


# class AssayData(Base):
#     """ sqlalchemly model for handling login information """
#     __tablename__ = "users"
#     id = Column('user_id', Integer, primary_key=True)
#     username = Column('username', String(20), unique=True, index=True)
#     pw_hash = Column('password', String(10))
#     email =Column('email', String(50), unique=True, index=True)
#
#     # datasets_id = db.Column('datasets_id', db.String, db.ForeignKey("datasets.id"))
#     datasets = relationship("Dataset", backref='users', uselist=True)
#
#     def __init__(self, username, password, email):
#         self.username = username
#         self.set_password(password)
#         self.email = email
#
# class Descriptions(Base):
#     __tablename__ = 'activities'
#     id = Column('id', db.Integer, primary_key=True)
#     chemical_id = Column(ForeignKey('chemicals.id'))
#     dataset_id = Column(ForeignKey('datasets.id'))
#     chemical = relationship("Chemical", back_populates="activities")
#     dataset = relationship("Dataset", back_populates="chemicals")
#
#     value = Column('value', db.Float)
#     units = Column('units', db.String)

class Compound(Base):
    """ main table to hold a users' datasets """
    __tablename__ = 'compounds'
    cid = Column('cid', Integer, primary_key=True)
    inchi = Column('inchi', String, index=True)
    smiles = Column('smiles', Integer, index=True)

    # chemicals = relationship("Chemical", back_populates="dataset")
    #chemicals = relationship("Activity", back_populates='dataset')
    #owner = relationship("User", back_populates="datasets")

    def __init__(self, cid, inchi, smiles):
        self.cid = cid
        self.inchi = inchi
        self.smiles = smiles


class Activity(Base):
    """ main table to hold a users' datasets """
    __tablename__ = 'activities'
    id = Column('id', Integer, primary_key=True)
    cid = Column('cid', Integer)
    sid = Column('sid', Integer)
    aid = Column('aid', Integer)
    outcome = Column('outcome', String)
    score = Column('score', Integer)
    result_tag = Column('tag', Integer)

    # def __init__(self, cid, sid, aid, outcome, score, result_tag):
    #     self.cid = cid
    #     self.sid = sid
    #     self.aid = aid
    #     self.outcome = outcome
    #     self.score = score
    #     self.result_tag = result_tag

class Assay(Base):
    __tablename__ = 'assays'
    aid = Column('aid', Integer, primary_key=True)

    description = Column('description', String)
    name = Column('name', String)
    source = Column('source', String)
    outcome_method = Column('outcome_method', Integer)

#Base.metadata.create_all(engine)
