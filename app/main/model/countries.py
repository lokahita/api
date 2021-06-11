from .. import db
from geoalchemy2 import Geometry

class Countries(db.Model):
    """ Countries Model for storing countries related details """
    __tablename__ = 'countries'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(255), nullable=False)
    continent_id = db.Column(db.Integer, db.ForeignKey('continents.id', ondelete='CASCADE'), nullable=False)
    continents = db.relationship('Continents', backref=db.backref('parent', lazy='dynamic'))
    geom = db.Column(Geometry('MULTIPOLYGON', srid='4326'))