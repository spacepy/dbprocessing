import sqlalchemy as sql
#import sqlalchemy.orm as sql.orm

engine = sql.create_engine('postgresql+psycopg2://rbsp_owner:rbsp_owner@edgar:5432/rbsp', echo=False)
metadata = sql.MetaData()

metadata = sql.MetaData()
users_table = sql.Table('TESTING', metadata,
                    sql.Column('id', sql.Integer, primary_key=True),
                    sql.Column('name', sql.String),
                    sql.Column('fullname', sql.String),
                    sql.Column('password', sql.String)
                    )

metadata.create_all(engine) 

class User(object):
    def __init__(self, name, fullname, password):
        self.name = name
        self.fullname = fullname
        self.password = password
    def __repr__(self):
        return "<User('%s','%s', '%s')>" % (self.name, self.fullname, self.password)



from sqlalchemy.orm import mapper

sql.orm.mapper(User, users_table) 
ed_User = User('ed2', 'Ed2 Jones', 'edpass')


from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engine)
session = Session()
session.add(ed_User)

session.add_all([
        User('wendy', 'Wendy Williams', 'foobar'),
        User('mary', 'Mary Contrary', 'xxg527'),
        User('fred', 'Fred Flinstone', 'blah')])


session.commit()













