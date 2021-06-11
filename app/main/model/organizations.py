from .. import db

class Organizations(db.Model):
    """ Organizations Model for storing organizations related details """
    __tablename__ = 'organizations'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(100), nullable=False)
    csw = db.Column(db.String(255), nullable=False)

    def as_dict(self):
       return {c.name: getattr(self, c.name) for c in self.__table__.columns if c.name != 'id'}
    
