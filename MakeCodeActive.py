import sqlalchemy as sql
from sqlalchemy.orm import mapper
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, BigInteger, Boolean, Date, Float

engine = sql.create_engine('postgresql+psycopg2://rbsp_owner:rbsp_owner@edgar:5432/rbsp', echo=False)

metadata = sql.MetaData()

Base = declarative_base()



class Executable_codes(Base):
    __tablename__ = 'executable_codes'
    ec_id = Column(BigInteger, primary_key=True)
    filename = Column(String)
    relative_path = Column(String)
    code_start_date = Column(Date)
    code_end_date = Column(Date)
    code_id = Column(String)
    p_id = Column(Integer)
    ds_id = Column(Integer)
    interface_version = Column(Integer)
    quality_version = Column(Integer)
    revision_version = Column(Integer)
    active_code = Column(Boolean)
    def __init__(self, 
                 filename, 
                 relative_path, 
                 code_start_date, 
                 code_end_date, 
                 code_id, 
                 p_id, 
                 ds_id, 
                 interface_version, 
                 quality_version, 
                 revision_version, 
                 active_code):
        self.filename = filename
        self.relative_path = relative_path
        self.code_start_date = code_start_date
        self.code_end_date = code_end_date
        self.code_id = code_id
        self.p_id = p_id
        self.ds_id = ds_id
        self.interface_version = interface_version 
        self.quality_version = quality_version
        self.revision_version = revision_version
        self.active_code = active_code


## this is going to change all codes to active

Session = sessionmaker(bind=engine)
session = Session()

## do a query to get create all the executable_codes objects
codes = session.query(Executable_codes).all()

for val in codes:
    val.active_code = True

session.commit()












