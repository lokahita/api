from .. import db

class Themes(db.Model):
    """ Themes Model for storing themes related details """
    __tablename__ = 'themes'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(100), nullable=False)

    def as_dict(self):
       return {c.name: getattr(self, c.name) for c in self.__table__.columns if c.name != 'id'}
    
