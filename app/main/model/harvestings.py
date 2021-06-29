from .. import db
from geoalchemy2 import Geometry

class Harvestings(db.Model):
    """ Harvestings Model for storing harvestings related details """
    __tablename__ = 'harvestings'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    organization_id = db.Column(db.Integer, db.ForeignKey('organizations.id', ondelete='CASCADE'), nullable=False)
    organizations = db.relationship('Organizations', backref=db.backref('organizations', lazy='dynamic'))
    title = db.Column(db.String(255), nullable=False)
    abstract = db.Column(db.Text, nullable=True)
    publication_date = db.Column(db.DateTime, nullable=False)
    data_type = db.Column(db.String(50), nullable=False)
    keywords = db.Column(db.Text, nullable=True)
    categories = db.Column(db.Text, nullable=True)
    identifier = db.Column(db.String(255), nullable=False)
    distributions = db.Column(db.Text, nullable=True)
    bbox = db.Column(Geometry('POLYGON', srid='4326'))
