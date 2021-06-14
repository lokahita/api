from .. import db

class Keywords(db.Model):
    """ Keywords Model for storing keywords related details """
    __tablename__ = 'keywords'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(100), nullable=False)

    def as_dict(self):
       return {c.name: getattr(self, c.name) for c in self.__table__.columns if c.name != 'id'}
