from .. import db

class Continents(db.Model):
    """ Continents Model for storing continents related details """
    __tablename__ = 'continents'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(255), nullable=False)