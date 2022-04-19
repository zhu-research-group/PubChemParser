import pandas as pd
from sqlalchemy import ForeignKey
from sqlalchemy import and_, or_
from sqlalchemy.orm import relationship

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import func




from sqlalchemy import Column, Integer, String, Float, Table
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

engine = create_engine(r"sqlite:///E://BindingDB//bindingdb.db")



class Record(Base):
    """ main table to hold a users' datasets """
    __tablename__ = 'compounds'
    _id = Column('id', Integer, primary_key=True)




def populate_db(file):
    df = pd.read_csv(file, sep="\t")
    print(df)

populate_db("E://BindingDB//BindingDB_All.tsv")


Base.metadata.create_all(engine)
